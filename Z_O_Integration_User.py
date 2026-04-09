#!/usr/bin/env python3
# Z_O_Integration_User.py
# Fill in the CONFIGURATION block below
"""
Zotero → Obsidian Pipeline
---------------------------
- Syncs Zotero annotations into Obsidian Source files
- Protects your manual edits with <!-- ZOTERO START/END --> markers
- Any [[concept]] or [[Folder/Note]] you write between or after Zotero blocks
  will be picked up and embedded in the target note on next run
- Works with any folder: Concepts, Drafts, or any other folder in your vault

SETUP: Fill in the 4 values in the CONFIGURATION block below.
         SNAPSHOT_FILE and TO_ORGANIZE_DIR are derived automatically — leave them blank.
"""

import sqlite3
import re
import json
import hashlib
import os
import sys
import argparse
import atexit
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

# ── Compiled regex constants (module-level for performance) ──────────────────
_RE_ZOTERO_SEGMENT   = re.compile(
    r'(<!-- zotero-start[^>]*-->.*?<!-- zotero-end -->)(.*?)(?=<!-- zotero-start|$)',
    re.DOTALL
)
_RE_INTER_ANN        = re.compile(
    r'<!-- zotero-start-([A-Z0-9]+) -->(.*?)<!-- zotero-end -->(.*?)(?=<!-- zotero-start|$)',
    re.DOTALL
)
_RE_LINKS            = re.compile(r'\[\[([^\]]+)\]\]')
_RE_YEAR             = re.compile(r'\d{4}')
_RE_PAGE_META        = re.compile(r'\*.*p\..*\*')
_RE_TASK_LINE        = re.compile(r'^- \[[ x]\]\s*$')
_RE_FIRST_MARKER     = re.compile(r'<!-- zotero-start[^>]*-->')
# Captures: group(1)=title, group(2)=authors (optional, in inner brackets)
_RE_PAPER_MARKER     = re.compile(r'^\[paper:\s*(.+?)(?:\s*\[([^\]]+)\])?\s*\]$', re.IGNORECASE)
_RE_BRACKET_HEADLINE = re.compile(r'^\[([^\]]+)\]')
# Matches: (p. 45)  (P. 45)  (pp. 45)  (p.45)  (page 45)  (Page 45)
_RE_PAGE_REF         = re.compile(r'\((?:pp?|[Pp]age)\.?\s*(\d+)\)')
_RE_PAGE_REF_STRIP   = re.compile(r'\((?:pp?|[Pp]age)\.?\s*\d+\)')
_RE_WIKI_LINK        = re.compile(r'\[\[[^\]]+\]\]')
_RE_SPACES           = re.compile(r'\s+')
_RE_PAGE_SPLIT       = re.compile(r'[-–]')
_RE_META_PAGE_LINE   = re.compile(r'^\*\(p\.')
_RE_UNSAFE_CHARS     = re.compile(r'[\/*?:"<>|]')
_RE_CONTROL_CHARS    = re.compile(r'[\x00-\x1f]')
_RE_CREATED_DATE     = re.compile(r'^created:\s*(\S+)', re.MULTILINE)

# ── File-size safety limits ───────────────────────────────────────────────────
MAX_FILE_SCAN_BYTES    = 500_000   # skip oversized files in annotation/link scan
MAX_SOURCE_NOTE_BYTES  = 200_000   # safety cap for source note writes
MAX_CONCEPT_FILE_BYTES = 500_000   # safety cap for concept file writes

def parse_paper_marker(comment: str):
    """Check if a sticky note starts with [paper: Title [Author]] on its first line.
    Returns (title, authors, body) if it's a marker, or (None, None, None) if not.
    - title:   the chapter/paper name
    - authors: comma-separated author names (or None)
    - body:    any text written AFTER the marker line (preserved as a note callout)
    Handles multiline comments where the marker is on line 1 and notes follow.
    """
    if not comment:
        return None, None, None
    lines = comment.strip().split('\n')
    first_line = lines[0].strip()
    m = _RE_PAPER_MARKER.match(first_line)
    if not m:
        return None, None, None
    title   = m.group(1).strip()
    authors = m.group(2).strip() if m.group(2) else None
    body    = '\n'.join(lines[1:]).strip()
    return title, authors, body


def _extract_year(pub_date: str) -> str:
    """Extract 4-digit year from a date string, or return empty string."""
    if not pub_date:
        return ''
    m = _RE_YEAR.search(pub_date)
    return m.group() if m else ''


def _clean_link_target(link: str) -> str:
    """Strip Obsidian wikilink suffixes before lookup or file creation.
    [[note|alias]] -> 'note',  [[note#heading]] -> 'note'
    """
    return link.split('|')[0].split('#')[0].strip()


def _norm_key(s: str) -> str:
    """Normalize a vault note name for case-insensitive, smart-quote-safe lookup."""
    return s.lower().replace('’', "'").replace('‘', "'")

# ─────────────────────────────────────────────
# CONFIGURATION — set your paths here
# ─────────────────────────────────────────────
DEFAULT_ZOTERO_DB    = str(Path.home() / "Zotero" / "zotero.sqlite")
_PHD_VAULT           = None  # set DEFAULT_VAULT_DIR below
_SOURCES_ROOT        = None  # derived from DEFAULT_VAULT_DIR automatically
DEFAULT_SOURCES_DIR  = ""  # e.g. "/Users/yourname/.../MyVault/Sources/Subjects"
DEFAULT_AUTHORS_DIR  = ""  # e.g. "/Users/yourname/.../MyVault/Sources/Authors"
DEFAULT_CONCEPTS_DIR = ""
# The root of your vault — used to resolve [[Folder/Note]] links
DEFAULT_VAULT_DIR    = ""  # e.g. "/Users/yourname/.../MyVault"
# ─────────────────────────────────────────────

SNAPSHOT_FILE   = ""
TO_ORGANIZE_DIR = ""
AUTHORS_DIR     = ""
PHD_COLLECTION  = ""

# ── Auto-derived paths ────────────────────────────────────────────────────────
if DEFAULT_VAULT_DIR:
    if not SNAPSHOT_FILE:
        SNAPSHOT_FILE = str(Path(DEFAULT_VAULT_DIR) / ".zotero_sync_state.json")
    if not TO_ORGANIZE_DIR:
        TO_ORGANIZE_DIR = str(Path(DEFAULT_VAULT_DIR) / "To_Organize")
    if not AUTHORS_DIR:
        AUTHORS_DIR = str(Path(DEFAULT_VAULT_DIR) / "Sources" / "Authors")
# ─────────────────────────────────────────────

ZOTERO_START    = "<!-- zotero-start -->"
CONCEPT_T_START = "<!-- zotero-auto-start -->"
CONCEPT_T_END   = "<!-- zotero-auto-end -->"
THOUGHTS_DIR    = "To_Organize/Open Thoughts/Thoughts and Directions: Sources.md"
ZOTERO_END      = "<!-- zotero-end -->"


# ── Parsing ───────────────────────────────────────────────────────────────────

def title_case(s: str) -> str:
    """Capitalize the first letter of each significant word.
    Preserves existing casing within words — McDowell stays McDowell, AI stays AI.
    Small words (a, an, the, and, etc.) are lowercased unless they start the headline.
    """
    if not s:
        return s
    small = {'a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor',
             'on', 'at', 'to', 'by', 'in', 'of', 'up', 'as', 'is'}
    words = s.split()
    result = []
    for i, word in enumerate(words):
        if i == 0 or word.lower() not in small:
            result.append(word[0].upper() + word[1:] if word else word)
        else:
            result.append(word.lower())
    return ' '.join(result)


def parse_comment(comment: str):
    """Extract headline and [[concepts]] from a Zotero annotation comment.
    Supports [[concept1, concept2]] comma-separated links.
    Auto-capitalizes headline to title case.
    """
    if not comment:
        return None, []
    concept_matches = _RE_LINKS.findall(comment)
    concepts = []
    for match in concept_matches:
        # Split on comma to support [[concept1, concept2]]
        for c in match.split(','):
            c = c.split('|')[0].strip()  # strip [[note|alias]] alias part
            if c:
                concepts.append(c)
    remaining = _RE_LINKS.sub('', comment).strip()
    remaining = _RE_SPACES.sub(' ', remaining).strip()
    headline = title_case(remaining) if remaining else None
    return headline, concepts


def parse_purple_comment(comment: str):
    """Extract optional [headline] and body text from a purple annotation comment.
    Format: [My Headline] rest of comment text
    Returns (headline, body) — both can be None/empty.
    """
    if not comment:
        return None, ""
    body = comment.strip()
    headline = None
    # Look for [headline] at the start
    bracket_match = _RE_BRACKET_HEADLINE.match(body)
    if bracket_match:
        headline = title_case(bracket_match.group(1).strip())
        body = body[bracket_match.end():].strip()
    return headline, body


def parse_grey_comment(comment: str):
    """Extract (p. X) usage reference and headline from a grey annotation comment.
    Returns (headline, doc_page) where doc_page is e.g. '45' from '(p. 45)' or '(P. 45)'.
    Headline: from [brackets] if present, otherwise the text (minus page ref and links).

    Handles [[wikilinks]] correctly — a comment like '[[chap. 1]] (p. 3)' correctly
    extracts headline='Chap. 1' and doc_page='3' without the bracket bug.
    """
    if not comment:
        return None, None
    body = comment.strip()
    doc_page = None
    # Extract (p. X) or (P. X) — page in the user's own document
    page_match = _RE_PAGE_REF.search(body)
    if page_match:
        doc_page = page_match.group(1)
    # Collect [[wikilinks]] before stripping them — used as fallback headline
    links = _RE_LINKS.findall(body)
    # Strip [[wikilinks]] so single-bracket [headline] matching works correctly
    body_no_links = _RE_WIKI_LINK.sub('', body).strip()
    body_clean = _RE_PAGE_REF_STRIP.sub('', body_no_links).strip().strip(',.')
    # Match [headline] — single brackets only (wikilinks already stripped)
    bracket_match = _RE_BRACKET_HEADLINE.match(body_no_links)
    if bracket_match:
        headline = title_case(bracket_match.group(1).strip())
    elif body_clean:
        headline = title_case(body_clean)
    elif links:
        # Comment consists only of wikilinks — use first link text as headline
        headline = title_case(links[0].split('|')[0].split('#')[0].strip())
    else:
        headline = None
    return headline, doc_page


def extract_manual_links(source_file: Path, content: str = None) -> list:
    """
    Scan content between zotero blocks for manually added [[links]].
    For each [[link]], captures the preceding annotation block and the user's text.
    Returns list of (link_target, preceding_annotation, user_text) tuples.
    Captures links anywhere in the file: before, between, and after annotation blocks.
    Accepts pre-read content to avoid redundant file reads.
    """
    if content is None:
        if not source_file.exists():
            return []
        content = source_file.read_text(encoding='utf-8', errors='replace')
    file_content = content
    # Safety: skip oversized files (mirrors extract_inter_annotation_notes guard)
    if len(file_content) > MAX_FILE_SCAN_BYTES:
        return []
    results = []

    # Capture links before the first Zotero marker (YAML header, title, etc.)
    first_marker = _RE_FIRST_MARKER.search(file_content)
    if first_marker:
        for line in file_content[:first_marker.start()].splitlines():
            stripped = line.strip()
            for link in _RE_LINKS.findall(stripped):
                link = _clean_link_target(link)
                if link:
                    user_text = _RE_LINKS.sub('', stripped).strip()
                    results.append((link, '', user_text))

    # Capture links between and after annotation blocks
    for match in _RE_ZOTERO_SEGMENT.finditer(file_content):
        annotation_block = match.group(1).strip()
        inter_text = match.group(2)

        for line in inter_text.split('\n'):
            stripped = line.strip()
            links = _RE_LINKS.findall(stripped)
            if links:
                user_text = _RE_LINKS.sub('', stripped).strip()
                for link in links:
                    link = _clean_link_target(link)
                    if link:
                        results.append((link, annotation_block, user_text))

    return results


def extract_manual_section(source_file: Path, content: str = None) -> tuple:
    """
    Extract everything OUTSIDE all ZOTERO START/END blocks.
    'before' = everything before the first zotero-start (header)
    'after'  = everything after the last zotero-end (user notes — ALWAYS preserved)
    Accepts pre-read content to avoid redundant file reads.
    """
    if content is None:
        if not source_file.exists():
            return "", ""
        content = source_file.read_text(encoding='utf-8', errors='replace')

    # Match both the generic marker and keyed markers (<!-- zotero-start-ANNKEY -->)
    first_start = _RE_FIRST_MARKER.search(content)
    if first_start and ZOTERO_END in content:
        start = first_start.start()
        end = content.rfind(ZOTERO_END) + len(ZOTERO_END)
        before = content[:start].rstrip('\n')
        after_raw = content[end:].strip('\n')
        # Filter out any leftover script-generated lines from old format
        # (old checkbox lines that ended up after the last zotero-end)
        after_lines = []
        for line in after_raw.split('\n'):
            s = line.strip()
            # Strip any script-generated task/checkbox lines
            if _RE_TASK_LINE.match(s):
                continue
            after_lines.append(line)
        after = '\n'.join(after_lines).strip('\n')
        return before, after
    else:
        # No markers found — treat as new file (no manual content to preserve)
        return "", ""


# ── Zotero DB ─────────────────────────────────────────────────────────────────

def get_zotero_data(db_path: str):
    if not Path(db_path).exists():
        print(f"[ERROR] Zotero database not found at: {db_path}")
        sys.exit(1)

    # Copy DB to /tmp first, then open the copy.
    # This avoids "database is locked" when Zotero is open and writing,
    # and is safer than immutable=1 (which skips locking and can return
    # corrupt results if the DB changes mid-read).
    tmp_db = "/tmp/zotero_readonly_copy.sqlite"
    shutil.copy2(db_path, tmp_db)
    # Ensure cleanup on any exit path — normal, exception, or sys.exit
    atexit.register(lambda p=tmp_db: os.unlink(p) if os.path.exists(p) else None)
    conn = sqlite3.connect(f"file:{tmp_db}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT i.itemID, i.key,
            idv_title.value AS title, idv_date.value AS pub_date,
            idv_doi.value AS doi, idv_url.value AS url,
            idv_abstract.value AS abstract, idv_journal.value AS journal,
            it.typeName AS item_type,
            idv_booktitle.value AS book_title,
            idv_pages.value AS pages
        FROM items i
        JOIN itemTypes it ON i.itemTypeID = it.itemTypeID
        LEFT JOIN itemData id_title ON i.itemID = id_title.itemID
            AND id_title.fieldID = (SELECT fieldID FROM fields WHERE fieldName='title')
        LEFT JOIN itemDataValues idv_title ON id_title.valueID = idv_title.valueID
        LEFT JOIN itemData id_date ON i.itemID = id_date.itemID
            AND id_date.fieldID = (SELECT fieldID FROM fields WHERE fieldName='date')
        LEFT JOIN itemDataValues idv_date ON id_date.valueID = idv_date.valueID
        LEFT JOIN itemData id_doi ON i.itemID = id_doi.itemID
            AND id_doi.fieldID = (SELECT fieldID FROM fields WHERE fieldName='DOI')
        LEFT JOIN itemDataValues idv_doi ON id_doi.valueID = idv_doi.valueID
        LEFT JOIN itemData id_url ON i.itemID = id_url.itemID
            AND id_url.fieldID = (SELECT fieldID FROM fields WHERE fieldName='url')
        LEFT JOIN itemDataValues idv_url ON id_url.valueID = idv_url.valueID
        LEFT JOIN itemData id_abs ON i.itemID = id_abs.itemID
            AND id_abs.fieldID = (SELECT fieldID FROM fields WHERE fieldName='abstractNote')
        LEFT JOIN itemDataValues idv_abstract ON id_abs.valueID = idv_abstract.valueID
        LEFT JOIN itemData id_journal ON i.itemID = id_journal.itemID
            AND id_journal.fieldID = (SELECT fieldID FROM fields WHERE fieldName='publicationTitle')
        LEFT JOIN itemDataValues idv_journal ON id_journal.valueID = idv_journal.valueID
        LEFT JOIN itemData id_booktitle ON i.itemID = id_booktitle.itemID
            AND id_booktitle.fieldID = (SELECT fieldID FROM fields WHERE fieldName='bookTitle')
        LEFT JOIN itemDataValues idv_booktitle ON id_booktitle.valueID = idv_booktitle.valueID
        LEFT JOIN itemData id_pages ON i.itemID = id_pages.itemID
            AND id_pages.fieldID = (SELECT fieldID FROM fields WHERE fieldName='pages')
        LEFT JOIN itemDataValues idv_pages ON id_pages.valueID = idv_pages.valueID
        WHERE it.typeName NOT IN ('attachment','note','annotation')
        AND i.itemID NOT IN (SELECT itemID FROM deletedItems)
    """)
    papers = {row['itemID']: dict(row) for row in cur.fetchall()}

    cur.execute("""
        SELECT ia.itemID, c.firstName, c.lastName, ct.creatorType
        FROM itemCreators ia
        JOIN creators c ON ia.creatorID = c.creatorID
        JOIN creatorTypes ct ON ia.creatorTypeID = ct.creatorTypeID
        WHERE ct.creatorType IN ('author', 'editor', 'bookAuthor', 'seriesEditor')
        ORDER BY ia.itemID, ia.orderIndex
    """)
    # creatorType routing:
    #   author / bookAuthor → authors list (chapter or primary book author)
    #   editor / seriesEditor → editors list (volume or series editor)
    _AUTHOR_TYPES = {'author', 'bookAuthor'}
    for row in cur.fetchall():
        iid = row['itemID']
        if iid in papers:
            name = f"{row['firstName']} {row['lastName']}".strip()
            if row['creatorType'] in _AUTHOR_TYPES:
                papers[iid].setdefault('authors', [])
                papers[iid]['authors'].append(name)
            else:
                papers[iid].setdefault('editors', [])
                papers[iid]['editors'].append(name)

    cur.execute("""
        SELECT ia.itemID AS ann_id, i.key AS ann_key,
            ia.parentItemID AS attachment_id, ia.type AS ann_type,
            ia.text AS highlighted_text, ia.comment AS comment,
            ia.color AS color, ia.pageLabel AS page_label,
            ia.sortIndex AS sort_index
        FROM itemAnnotations ia
        JOIN items i ON ia.itemID = i.itemID
        WHERE ia.itemID NOT IN (SELECT itemID FROM deletedItems)
        AND (
            ia.color IN ('#5fb236', '#7cc868', '#a28ae5', '#aaaaaa')
            OR ia.type = 2
        )
        ORDER BY ia.parentItemID, ia.sortIndex
    """)
    annotations_raw = cur.fetchall()

    # Also fetch ALL annotation keys (any color) so we can detect color changes
    cur.execute("""
        SELECT i.key AS ann_key, ia.color AS color,
               ia.parentItemID AS attachment_id
        FROM itemAnnotations ia
        JOIN items i ON ia.itemID = i.itemID
        WHERE ia.itemID NOT IN (SELECT itemID FROM deletedItems)
    """)
    all_ann_colors = {row['ann_key']: row['color'] for row in cur.fetchall()}

    # ── Fetch collection hierarchy under PHD_COLLECTION ─────────────────────
    cur.execute("""
        SELECT collectionID FROM collections
        WHERE collectionName = ?
        ORDER BY collectionID
        LIMIT 1
    """, (PHD_COLLECTION,))
    root_row = cur.fetchone()

    included_ids = set()
    collection_of_item = {}   # itemID -> subcollection name (direct parent)

    if root_row:
        root_id = root_row[0]

        # Fetch ALL collections once, then build hierarchy in Python
        cur.execute("SELECT collectionID, collectionName, parentCollectionID FROM collections")
        all_collections_raw = cur.fetchall()

        # Build parent->children map
        children_map = {}
        coll_names = {}
        for row in all_collections_raw:
            cid, cname, parent = row[0], row[1], row[2]
            coll_names[cid] = cname
            children_map.setdefault(parent, []).append(cid)

        # BFS to get all collection IDs under root
        def get_all_subcollections(root):
            result = []
            queue = [root]
            while queue:
                current = queue.pop()
                result.append((current, coll_names.get(current)))
                for child in children_map.get(current, []):
                    queue.append(child)
            return result

        all_colls = get_all_subcollections(root_id)

        # Build map: collectionID -> name
        coll_name_map = {cid: name for cid, name in all_colls if name}
        coll_name_map[root_id] = PHD_COLLECTION

        # Get items in these collections
        coll_ids = [cid for cid, _ in all_colls]
        placeholders = ','.join('?' * len(coll_ids))
        cur.execute(f"""
            SELECT ci.itemID, ci.collectionID
            FROM collectionItems ci
            WHERE ci.collectionID IN ({placeholders})
        """, coll_ids)
        for row in cur.fetchall():
            item_id = row[0]
            coll_id = row[1]
            included_ids.add(item_id)
            # Store ALL collection names per item (supports multiple collections)
            coll_name = coll_name_map.get(coll_id, '')
            if coll_name and coll_name != PHD_COLLECTION:
                collection_of_item.setdefault(item_id, [])
                if coll_name not in collection_of_item[item_id]:
                    collection_of_item[item_id].append(coll_name)

        print(f"   📁 Found {len(included_ids)} item(s) in '{PHD_COLLECTION}' and sub-folders.")
    else:
        print(f"   ⚠️  Collection '{PHD_COLLECTION}' not found — syncing all papers.")

    # Remove papers NOT in the PHD collection (if collection exists)
    if included_ids:
        for pid in list(papers.keys()):
            if pid not in included_ids:
                papers.pop(pid, None)

    # Store subcollection names on each paper — list for multiple collections
    for pid in papers:
        papers[pid]['subcollections'] = collection_of_item.get(pid, [])

    cur.execute("""
        SELECT i.itemID AS att_id, i.key AS att_key, ia.parentItemID AS paper_id
        FROM items i
        JOIN itemTypes it ON i.itemTypeID = it.itemTypeID AND it.typeName = 'attachment'
        JOIN itemAttachments ia ON i.itemID = ia.itemID
    """)
    att_rows = cur.fetchall()
    att_to_paper = {row['att_id']: row['paper_id'] for row in att_rows}
    att_to_key   = {row['att_id']: row['att_key'] for row in att_rows}

    for row in annotations_raw:
        att_id = row['attachment_id']
        paper_id = att_to_paper.get(att_id)
        if paper_id and paper_id in papers:
            papers[paper_id].setdefault('annotations', [])
            ann_dict = dict(row)
            ak = att_to_key.get(att_id, '')
            ann_dict['att_key'] = ak
            papers[paper_id]['annotations'].append(ann_dict)
            # Store first att_key on paper for header open-pdf link
            if ak and not papers[paper_id].get('att_key'):
                papers[paper_id]['att_key'] = ak

    conn.close()
    try:
        os.unlink(tmp_db)
    except OSError:
        pass

    total_anns = sum(len(p.get('annotations', [])) for p in papers.values())
    print(f"   Found {len(papers)} papers total, {total_anns} annotations total.")

    # Track papers that have ANY annotation (any color) — not truly unread
    papers_with_any_ann = set()
    for row in annotations_raw:
        att_id = row['attachment_id']
        paper_id = att_to_paper.get(att_id)
        if paper_id:
            papers_with_any_ann.add(paper_id)

    result = {}
    unread = {}  # papers with no annotations of any tracked color
    for pid, paper in papers.items():
        anns = paper.get('annotations', [])
        if anns:
            paper['annotations'] = anns
            paper['all_ann_colors'] = all_ann_colors
            result[pid] = paper
            commented = [a for a in anns if a.get('comment') or a.get('ann_type') == 2]
            print(f"   ✓ '{paper.get('title','Untitled')}' — {len(anns)} annotation(s), {len(commented)} with comments")
        elif pid not in papers_with_any_ann:
            # Only truly unread if no annotations of any color
            unread[pid] = paper

    # Papers with ONLY sticky-note annotations (no highlights) are "started"
    # — they have an overview note but no real annotations yet.
    # Keep them in result (source file written) but also list in unread.
    for pid, paper in result.items():
        anns = paper.get("annotations", [])
        if anns and all(a.get("ann_type") == 2 for a in anns):
            paper["_overview_only"] = True
            unread[pid] = paper

    if not result:
        print("\n   ⚠️  No annotations with comments found.")
        print("   Right-click a highlight in Zotero → Add Comment")

    return result, unread


# ── Building notes ────────────────────────────────────────────────────────────

def yaml_str(value: str) -> str:
    """Escape a string for safe YAML double-quoted value.
    Handles titles/authors containing double quotes or backslashes.
    """
    if not value:
        return value
    return value.replace('\\', '\\\\').replace('"', '\\"')


def safe_filename(title: str) -> str:
    if not title:
        return "Untitled"
    safe = re.sub(r'[\\/*?:"<>|\'\[\]]', '', title).strip('. ')
    safe = safe.replace('\u2019', '').replace('\u2018', '')
    return safe[:120]


def author_folder(paper: dict) -> str:
    """Return the Authors/ subfolder name for a paper.
    Format: "Last, First" — sorts alphabetically by surname.
    For edited books (no authors), uses the first editor the same way.
    """
    people = paper.get('authors') or paper.get('editors') or []
    if not people:
        return "Unknown Author"
    name = people[0]
    parts = name.strip().split()
    if len(parts) >= 2:
        return safe_filename(f"{parts[-1]}, {chr(32).join(parts[:-1])}")
    return safe_filename(name) if name else "Unknown Author"


def format_authors(authors: list) -> str:
    if not authors:
        return "Unknown Author"
    if len(authors) == 1:
        return authors[0]
    if len(authors) == 2:
        return " & ".join(authors)
    return f"{authors[0]} et al."


def format_creators(paper: dict) -> str:
    """Return a formatted author/editor string for display.
    Falls back to editors (with role label) when no authors are present,
    which is the normal case for edited volumes stored as book items.
    """
    authors = paper.get('authors', [])
    if authors:
        return format_authors(authors)
    editors = paper.get('editors', [])
    if editors:
        suffix = ' (ed.)' if len(editors) == 1 else ' (eds.)'
        return format_authors(editors) + suffix
    return 'Unknown Author'


def zotero_link(ann: dict, att_key: str) -> str:
    """Build a deep link that opens Zotero at this exact annotation."""
    ann_key = ann.get('ann_key', '')
    if att_key and ann_key:
        return f"[→](zotero://open-pdf/library/items/{att_key}?annotation={ann_key})"
    return ""


def build_annotation_block(ann: dict, headline: str, concepts: list,
                            purple_body: str = None,
                            grey: bool = False, doc_page: str = None,
                            is_first_note: bool = False) -> str:
    """Build a single annotation wrapped in its own ZOTERO START/END markers.
    Type 1 = green highlight, Type 2 = sticky note, purple = comment, grey = used.
    Returns None if there is nothing to render (e.g. scanned PDF with no text).
    """
    ann_key_str = ann.get('ann_key', '')
    marker = f"<!-- zotero-start-{ann_key_str} -->" if ann_key_str else ZOTERO_START
    lines = [marker]
    ann_type = ann.get('ann_type', 1)
    text = (ann.get('highlighted_text') or ann.get('comment') or '').strip()
    comment = (ann.get('comment') or '').strip()
    page = ann.get('page_label', '')
    concept_links = ' '.join(f'[[{c}]]' for c in concepts)
    zlink = zotero_link(ann, ann.get('att_key', ''))

    if grey:
        # Grey annotation = used citation — collapsible callout
        # Header format: ✅ Headline *(p. X)*
        if not text:
            return None
        header = f"✅ {headline}" if headline else "✅ Used"
        lines.append(f"> [!check]- {header}")
        lines.append(f"> {text}")
        # Zotero page + link — same as all annotations
        meta_parts = []
        if page:
            meta_parts.append(f"p. {page}")
        if zlink:
            meta_parts.append(zlink)
        if meta_parts:
            lines.append(f"> *{' · '.join(meta_parts)}*")
        # Doc page — where you cited this in your own writing
        if doc_page:
            lines.append(f"> ")
            lines.append(f"> *cited on p. {doc_page} of your doc*")
        lines.append(ZOTERO_END)
        return '\n'.join(lines)
    elif ann_type == 2:
        # Sticky note → My Note callout, with optional [My Headline]
        note_headline = None
        note_body = comment
        if comment:
            bracket_match = _RE_BRACKET_HEADLINE.match(comment.strip())
            if bracket_match:
                note_headline = title_case(bracket_match.group(1).strip())
                note_body = comment.strip()[bracket_match.end():].strip()
        if is_first_note and not note_headline:
            callout_title = "Overview"
        elif is_first_note and note_headline:
            callout_title = f"Overview: {note_headline}"
        elif note_headline:
            callout_title = f"Note: {note_headline}"
        else:
            callout_title = "Note"
        lines.append(f"> [!note] {callout_title}")
        lines.append("> ")
        if note_body:
            for note_line in note_body.split('\n'):
                lines.append(f"> {note_line}")
        lines.append("> ")
        now_str = datetime.now().strftime("%m/%Y")
        meta_parts = []
        if page:
            meta_parts.append(f"p. {page}")
        meta_parts.append(now_str)
        if zlink:
            meta_parts.append(zlink)
        if meta_parts:
            lines.append(f"> *({', '.join(meta_parts)})*")
    elif purple_body is not None:
        # Purple annotation → headline (optional) + citation + comment callout
        if not text and not headline and not purple_body:
            return None
        if headline:
            lines.append(f"### {headline}")
            lines.append("")
        if text:
            lines.append(f"> {text}")
            meta_parts = []
            if page:
                meta_parts.append(f"p. {page}")
            if concept_links:
                meta_parts.append(concept_links)
            if zlink:
                meta_parts.append(zlink)
            if meta_parts:
                lines.append(f"*{' · '.join(meta_parts)}*")
        if purple_body:
            lines.append("")
            lines.append("> [!reading]-")  # custom type — no icon, no label in Obsidian
            for body_line in purple_body.split('\n'):
                lines.append(f"> {body_line}")
    else:
        # Green highlight annotation
        if not text and not headline:
            return None
        if headline:
            lines.append(f"### {headline}")
            lines.append("")
        if text:
            lines.append(f"> {text}")
            meta_parts = []
            if page:
                meta_parts.append(f"p. {page}")
            if concept_links:
                meta_parts.append(concept_links)
            if zlink:
                meta_parts.append(zlink)
            if meta_parts:
                lines.append(f"*{' · '.join(meta_parts)}*")
    lines.append(ZOTERO_END)
    return '\n'.join(lines)


def extract_inter_annotation_notes(source_file: Path, content: str = None) -> dict:
    """
    Extract user-written text between annotation blocks.
    Only captures text that does NOT look like script-generated content.
    Returns dict: ann_key -> user text after that annotation.
    Accepts pre-read content to avoid redundant file reads.
    """
    if content is None:
        if not source_file.exists():
            return {}
        content = source_file.read_text(encoding='utf-8', errors='replace')
    text = content
    # Safety check — skip files over MAX_FILE_SCAN_BYTES
    if len(text) > MAX_FILE_SCAN_BYTES:
        print(f"  ⚠️  Skipping oversized file: {source_file.name}")
        return {}
    result = {}

    for match in _RE_INTER_ANN.finditer(text):
        ann_key = match.group(1)
        after_block = match.group(3).strip()
        if not after_block:
            continue
        user_lines = []
        for line in after_block.split('\n'):
            s = line.strip()
            if not s:
                continue
            if s.startswith('>'):
                continue
            if _RE_PAGE_META.match(s):
                continue
            if 'zotero://' in s:
                continue
            if s.startswith('<!--'):
                continue
            if _RE_META_PAGE_LINE.match(s):
                continue
            if _RE_TASK_LINE.match(s):
                continue
            user_lines.append(line)
        user_text = '\n'.join(user_lines).strip()
        if user_text:
            result[ann_key] = user_text

    return result


def build_zotero_block(paper: dict, inter_notes: dict = None) -> str:
    """Build all annotation blocks for this paper — each wrapped in its own markers."""
    all_annotations = paper.get('annotations_for_display', paper.get('annotations', []))
    inter_notes = inter_notes or {}
    parts = []
    first_sticky_seen = False

    for ann in all_annotations:
        ann_type = ann.get('ann_type', 1)
        comment = (ann.get('comment') or '').strip()
        text = (ann.get('highlighted_text') or '').strip()
        ann_key = ann.get('ann_key', '')

        color = (ann.get('color') or '').lower()
        is_purple = color == '#a28ae5'
        is_grey   = color == '#aaaaaa'

        if ann_type == 2:
            # Sticky note — only render if it actually has content
            if not comment:
                continue
            # Check for [paper: Title [Author]] marker (may have body text after it)
            _pm_title, _pm_authors, _pm_body = parse_paper_marker(comment)
            if _pm_title is not None:
                # It's a boundary marker — render only if it has body text after the marker line
                if not _pm_body:
                    continue  # pure boundary marker, no body — skip entirely
                # Has body text: render the body as a note callout, using the paper title as headline
                _marker_ann = dict(ann)
                _marker_ann['comment'] = f"[{_pm_title}] {_pm_body}"
                is_first = not first_sticky_seen
                first_sticky_seen = True
                block = build_annotation_block(_marker_ann, None, [], is_first_note=is_first)
            else:
                is_first = not first_sticky_seen
                first_sticky_seen = True
                block = build_annotation_block(ann, None, [], is_first_note=is_first)
        elif is_grey:
            # Grey annotation = used citation — render as collapsible "done" callout
            headline, doc_page = parse_grey_comment(comment)
            block = build_annotation_block(ann, headline, [], grey=True, doc_page=doc_page)
        elif is_purple:
            # Purple annotation — parse [headline] and comment body
            headline, purple_body = parse_purple_comment(comment)
            _, concepts = parse_comment(comment)
            block = build_annotation_block(ann, headline, concepts, purple_body=purple_body)
        elif comment:
            headline, concepts = parse_comment(comment)
            block = build_annotation_block(ann, headline, concepts)
        elif text:
            block = build_annotation_block(ann, None, [])
        else:
            # No text and no comment (e.g. scanned PDF highlight with no extractable text)
            continue

        if block is None:
            continue
        parts.append(block)
        # Re-insert any user note written after this annotation
        user_note = inter_notes.get(ann_key, '').strip()
        if user_note:
            parts.append(user_note)

    return '\n\n'.join(parts)


def build_source_note(paper: dict, before: str, after: str,
                      inter_notes: dict = None) -> str:
    """Build the full source note, preserving manual content outside the Zotero block."""
    item_type    = paper.get('item_type', '')
    book_title   = (paper.get('book_title') or '') if item_type == 'bookSection' else ''
    pages        = (paper.get('pages') or '')      if item_type == 'bookSection' else ''
    # Null-safe title: bookSections may have no paper title set in Zotero.
    title        = paper.get('title') or book_title or 'Untitled'
    authors_list = paper.get('authors', [])
    editors_list = paper.get('editors', [])
    pub_date     = paper.get('pub_date', '')
    journal      = paper.get('journal', '')
    doi          = paper.get('doi', '')
    url          = paper.get('url', '')
    abstract     = paper.get('abstract', '')
    zot_key      = paper.get('key', '')

    year = _extract_year(pub_date)

    # Preserve original creation date if the file already exists
    created_date = datetime.now().strftime("%Y-%m-%d")
    if before:
        m = _RE_CREATED_DATE.search(before)
        if m:
            created_date = m.group(1)

    # ── YAML frontmatter ──────────────────────────────────────────────────────
    header_lines = ["---"]
    # Use correct role key — never write "Unknown Author" for an editors-only item
    if authors_list:
        header_lines.append(f'authors: "{yaml_str(format_authors(authors_list))}"')
    elif editors_list:
        header_lines.append(f'editors: "{yaml_str(format_authors(editors_list))}"')
    if year:
        header_lines.append(f'year: {year}')
    if book_title:
        header_lines.append(f'from: "{yaml_str(book_title)}"')
        if pages:
            header_lines.append(f'pages: "{yaml_str(pages)}"')
    if journal:
        header_lines.append(f'journal: "{yaml_str(journal)}"')
    if doi:
        header_lines.append(f'doi: "{yaml_str(doi)}"')
    if zot_key:
        header_lines.append(f'zotero_key: "{yaml_str(zot_key)}"')
    header_lines.append(f'created: {created_date}')
    header_lines.append("tags: [source]")
    header_lines.append("zotero_sync_managed: true")
    header_lines.append("---")
    header_lines.append("")

    # ── Body ─────────────────────────────────────────────────────────────────
    # Show explicit # Title heading when filename has a sort prefix (p. 009 ...)
    # so Obsidian displays the clean title instead of the prefixed filename.
    # Heading shown whenever filename has a sort prefix — i.e. any bookSection chapter.
    _has_prefix = bool(book_title and pages and item_type == 'bookSection')
    if _has_prefix:
        header_lines.append(f"# {title}")
        header_lines.append("")
    att_key_paper = paper.get('att_key', '')
    if att_key_paper:
        zotero_item_link = f"[→](zotero://open-pdf/library/items/{att_key_paper})"
    elif zot_key:
        zotero_item_link = f"[→](zotero://select/library/items/{zot_key})"
    else:
        zotero_item_link = ""

    # Creator display line — label reflects actual role
    if authors_list:
        creator_label   = "Authors"
        creator_display = format_authors(authors_list)
    elif editors_list:
        creator_label   = "Editors"
        creator_display = format_authors(editors_list)
    else:
        creator_label   = "Authors"
        creator_display = "Unknown"

    # Attach the Zotero link to the creator line for non-bookSection items;
    # bookSection items get their link in the "In:" line below.
    if zotero_item_link and not book_title:
        header_lines.append(f"**{creator_label}:** {creator_display} {zotero_item_link}")
    else:
        header_lines.append(f"**{creator_label}:** {creator_display}")

    if year:
        header_lines.append(f"**Year:** {year}")
    # For bookSection: show collection context + page range with link to book
    if book_title:
        if att_key_paper and pages:
            _sp = pages.split('–')[0].split('-')[0].strip()
            if _sp.isdigit():
                book_link = f"[→](zotero://open-pdf/library/items/{att_key_paper}?page={_sp})"
            else:
                book_link = f"[→](zotero://open-pdf/library/items/{att_key_paper})"
        elif att_key_paper:
            book_link = f"[→](zotero://open-pdf/library/items/{att_key_paper})"
        elif zot_key:
            book_link = f"[→](zotero://select/library/items/{zot_key})"
        else:
            book_link = ""
        collection_ref = f"*{book_title}*"
        if pages:
            collection_ref += f", pp. {pages}"
        if book_link:
            collection_ref += f" {book_link}"
        header_lines.append(f"**In:** {collection_ref}")
    if journal:
        header_lines.append(f"**Journal:** {journal}")
    if doi:
        header_lines.append(f"**DOI:** [{doi}](https://doi.org/{doi})")
    elif url:
        header_lines.append(f"**URL:** [{url}]({url})")
    header_lines.append("")
    if abstract:
        header_lines.append("> [!abstract]- Abstract")
        # Truncate very long abstracts to keep file sizes manageable
        abstract_display = abstract if len(abstract) <= 800 else abstract[:800].rsplit(' ', 1)[0] + "…"
        for abs_line in abstract_display.split('\n'):
            header_lines.append(f"> {abs_line}")
        header_lines.append("")

    zotero_block = build_zotero_block(paper, inter_notes)

    parts = ['\n'.join(header_lines), zotero_block]
    if after:
        parts.append(after)
    return '\n\n'.join(p for p in parts if p.strip())


# ── Concept notes ─────────────────────────────────────────────────────────────

def build_concept_entry_from_zotero(paper_filename: str, annotations: list,
                                     paper: dict = None, current_concept: str = None) -> str:
    """Build a concept entry: one headline per source, bullets sorted by page order."""
    lines = []
    file_stem    = paper_filename.replace('.md', '')
    display_stem = (paper.get('title') if paper else None) or file_stem
    # Use a piped wikilink when the filename has a page prefix (e.g. "p. 105 — Title")
    # so Obsidian resolves to the actual file, not an empty root-level stub.
    if file_stem != display_stem:
        lines.append(f"### From [[{file_stem}|{display_stem}]]")
    else:
        lines.append(f"### From [[{display_stem}]]")

    if paper:
        authors = format_creators(paper)
        pub_date = paper.get('pub_date', '')
        year = _extract_year(pub_date)
        item_type  = paper.get('item_type', '')
        book_title = (paper.get('book_title') or '') if item_type == 'bookSection' else ''
        pages      = (paper.get('pages') or '')      if item_type == 'bookSection' else ''
        if book_title:
            # Show collection context subtly (paper is in an edited volume)
            collection_line = f"*in {book_title}"
            if pages:
                collection_line += f", pp. {pages}"
            collection_line += "*"
            lines.append(collection_line)
        if authors or year:
            attribution = ', '.join(p for p in [authors, year] if p)
            lines.append(f"*{attribution}*")

    lines.append("")
    sorted_anns = sorted(annotations, key=lambda x: x[0].get('sort_index') or '')
    first_sticky_in_concept = True
    for ann, headline, _ in sorted_anns:
        ann_type = ann.get('ann_type', 1)
        page = ann.get('page_label', '')
        zlink = zotero_link(ann, ann.get('att_key', ''))
        meta_parts = []
        if page:
            meta_parts.append(f"p. {page}")
        if zlink:
            meta_parts.append(zlink)
        meta_str = f" *({'  ·  '.join(meta_parts)})*" if meta_parts else ""

        # Collect other concepts this annotation links to (excluding current one)
        raw_comment = (ann.get('comment') or '')
        _, all_ann_concepts = parse_comment(raw_comment)
        other_concepts = [c for c in all_ann_concepts
                         if current_concept is None or c.lower() != current_concept.lower()]

        ann_color = (ann.get('color') or '').lower()
        if ann_color == '#aaaaaa':
            # Grey = used citation — show as [!check] callout in concept files too
            text = (ann.get('highlighted_text') or '').strip()
            if not text:
                continue
            _, doc_page = parse_grey_comment(ann.get('comment') or '')
            chk_header = f"✅ {headline}" if headline else "✅ Used"
            lines.append(f"> [!check]- {chk_header}")
            lines.append(f"> {text}")
            # Zotero page + link
            meta_parts_grey = []
            if page:
                meta_parts_grey.append(f"p. {page}")
            if zlink:
                meta_parts_grey.append(zlink)
            if meta_parts_grey:
                lines.append(f"> *{'  ·  '.join(meta_parts_grey)}*")
            # Doc page
            if doc_page:
                lines.append(f"> ")
                lines.append(f"> *cited on p. {doc_page} of your doc*")
        elif ann_color == '#a28ae5':
            # Purple = interpretive note — show with [!reading]- collapsible callout
            text = (ann.get('highlighted_text') or '').strip()
            raw_comment = (ann.get('comment') or '').strip()
            _, purple_body = parse_purple_comment(raw_comment)
            purple_body = _RE_WIKI_LINK.sub('', purple_body).strip()
            if headline:
                lines.append(f"#### {headline}{meta_str}")
                lines.append("")
            if text:
                lines.append(f"> {text}")
                if meta_parts:
                    lines.append(f"*{' · '.join(meta_parts)}*")
            if purple_body:
                lines.append("")
                lines.append("> [!reading]-")
                for body_line in purple_body.split('\n'):
                    lines.append(f"> {body_line}")
        elif ann_type == 2:
            raw_comment = (ann.get('comment') or '').strip()
            note_headline = None
            note_body = raw_comment
            bracket_match = _RE_BRACKET_HEADLINE.match(raw_comment)
            if bracket_match:
                note_headline = title_case(bracket_match.group(1).strip())
                note_body = raw_comment[bracket_match.end():].strip()
            note_body = _RE_WIKI_LINK.sub('', note_body).strip()
            if first_sticky_in_concept and not note_headline:
                callout_title = "Overview"
            elif first_sticky_in_concept and note_headline:
                callout_title = f"Overview: {note_headline}"
            elif note_headline:
                callout_title = f"Note: {note_headline}"
            else:
                callout_title = "Note"
            first_sticky_in_concept = False
            lines.append(f"> [!note] {callout_title}")
            lines.append("> ")
            if note_body:
                for note_line in note_body.split('\n'):
                    lines.append(f"> {note_line}")
            lines.append("> ")
            if meta_parts:
                lines.append(f"> *({'  ·  '.join(meta_parts)})*")
        else:
            text = (ann.get('highlighted_text') or '').strip()
            if headline:
                lines.append(f"#### {headline}{meta_str}")
                if text:
                    lines.append(f"> {text}")
            elif text:
                lines.append(f"{text}{meta_str}")
            else:
                continue
        # Add subtle "also in" footer — skip for grey (archived) annotations
        if other_concepts and ann_color != '#aaaaaa':
            also_links = ' '.join(f'[[{c}]]' for c in other_concepts)
            lines.append(f'<sub>also in: {also_links}</sub>')
        lines.append("")
    return '\n'.join(lines)


def build_concept_entry_from_manual(paper_filename: str, contexts: list,
                                    paper: dict = None) -> str:
    """Build a concept entry from manual [[links]]."""
    lines = []
    file_stem    = paper_filename.replace('.md', '')
    display_stem = (paper.get('title') if paper else None) or file_stem
    if file_stem != display_stem:
        lines.append(f"### From [[{file_stem}|{display_stem}]]")
    else:
        lines.append(f"### From [[{display_stem}]]")
    lines.append("")
    seen = set()
    for ann_block, user_text in contexts:
        key = hashlib.md5((ann_block + user_text).encode(), usedforsecurity=False).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        clean_ann = _RE_FIRST_MARKER.sub('', ann_block)
        clean_ann = clean_ann.replace('<!-- zotero-end -->', '').strip()
        for line in clean_ann.split('\n'):
            stripped = line.strip()
            if stripped:
                lines.append(f"> {stripped}")
        if user_text:
            lines.append(f"> *{user_text}*")
        lines.append("")
    return '\n'.join(lines)


def collect_concept_entry(all_entries: dict, concept_path: Path,
                           paper_filename: str, entry_text: str,
                           first_ann_date: str = "", book_title: str = ""):
    """Add an entry to be written to a concept/target note.
    Merges multiple entries from the same source into one block.
    book_title is non-empty only for Case B virtual-paper entries; it is used
    to insert a '## Edited Book: …' heading before that group in the note.
    """
    key = str(concept_path)
    if key not in all_entries:
        all_entries[key] = {
            'name': concept_path.stem,
            'path': concept_path,
            'entries': []
        }
    for i, (fname, etext, edate, ebt) in enumerate(all_entries[key]['entries']):
        if fname == paper_filename:
            lines = entry_text.split('\n')
            bullet_lines = []
            skip_header = True
            for line in lines:
                if skip_header and (line.startswith('### ') or line.startswith('*') or line == ''):
                    if bullet_lines:
                        bullet_lines.append(line)
                    continue
                skip_header = False
                bullet_lines.append(line)
            merged = etext.rstrip() + '\n' + '\n'.join(bullet_lines)
            all_entries[key]['entries'][i] = (
                fname, merged,
                min(edate, first_ann_date) if edate and first_ann_date else edate or first_ann_date,
                ebt or book_title,
            )
            return
    all_entries[key]['entries'].append((paper_filename, entry_text, first_ann_date, book_title))


def _filter_dead_entries(entries: list, active_filenames: set) -> list:
    """Remove entries for papers no longer in the active library.
    Operates on the list of (fname, text, sort, book_title) tuples — before
    joining — so that '## Edited Book:' headings are always tied to the entries
    they introduce and cannot become orphans.
    """
    return [
        e for e in entries
        if (e[0] in active_filenames or e[0].replace('.md', '') in active_filenames
            or e[0] + '.md' in active_filenames)
    ]


def _build_entry_blocks(sorted_entries: list) -> list:
    """Convert sorted (fname, (text, sort, book_title)) pairs into final block strings.

    Injects a '## Edited Book: …' heading before the first entry of each
    edited-volume group (entries with non-empty book_title).  The heading is
    prepended directly to the entry text so it travels with that entry — it
    cannot become an orphan if the entry is later removed.
    """
    blocks = []
    current_book = None
    for _fname, (entry_text, _sort, book_title) in sorted_entries:
        if book_title and book_title != current_book:
            blocks.append(f"## Edited Book: {book_title}\n\n{entry_text}")
            current_book = book_title
        else:
            blocks.append(entry_text)
    return blocks


def write_all_target_notes(all_entries: dict, active_filenames: set = None, vault_path: Path = None):
    """
    Write all concept/target notes using the same marker system as source files.
    Content OUTSIDE markers is always preserved.
    active_filenames: if provided, entries from removed papers are stripped.
    """
    for key, data in all_entries.items():
        target_path = data['path']
        name = data['name']

        # Deduplicate by fname — keep the entry with the earliest sort key.
        # Tuples are (fname, text, first_sort, book_title).
        seen_files = {}
        for fname, entry_text, first_sort, book_title in data['entries']:
            if fname not in seen_files or first_sort < seen_files[fname][1]:
                seen_files[fname] = (entry_text, first_sort, book_title)

        # Sort: regular entries (empty book_title) first, then edited-volume
        # groups sorted by book_title then by page-range sort key.
        sorted_entries = sorted(
            seen_files.items(),
            key=lambda x: (x[1][2] or '', x[1][1] or '')
        )

        # Filter dead entries at tuple level so headings stay tied to their entries.
        if active_filenames:
            sorted_entries = [
                (fname, vals) for fname, vals in sorted_entries
                if _filter_dead_entries([(fname,) + vals], active_filenames)
            ]

        entry_blocks = _build_entry_blocks(sorted_entries)
        new_zotero_block = (
            CONCEPT_T_START + "\n\n" +
            '\n---\n\n'.join(entry_blocks) +
            "\n\n" + CONCEPT_T_END
        )

        existing = None  # guard: ensures skip-write check below is safe
        if target_path.exists():
            existing = target_path.read_text(encoding='utf-8', errors='replace')
            if CONCEPT_T_START in existing and CONCEPT_T_END in existing:
                start = existing.find(CONCEPT_T_START)
                end = existing.find(CONCEPT_T_END) + len(CONCEPT_T_END)
                before = existing[:start].rstrip('\n')
                after = existing[end:].strip('\n')
                parts = [p for p in [before, new_zotero_block, after] if p.strip()]
                full_content = '\n\n'.join(parts)
            else:
                full_content = existing.rstrip('\n') + '\n\n' + new_zotero_block
        else:
            now = datetime.now().strftime("%Y-%m-%d")
            header = f"---\nconcept: \"{yaml_str(name)}\"\ntags: [concept]\ncreated: {now}\n---\n\n# {name}\n"
            full_content = header + "\n" + new_zotero_block

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if len(full_content.encode('utf-8')) > MAX_CONCEPT_FILE_BYTES:
            print(f"  ⚠️  Concept file too large, skipping: {target_path.name}")
            continue
        # Skip write if content hasn't changed — reuse already-read content (#3)
        if existing is not None and existing == full_content:
            continue
        try:
            atomic_write_text(target_path, full_content)
            rel = target_path.relative_to(vault_path) if vault_path else target_path.relative_to(target_path.parent.parent)
            print(f"  [→] {rel} ({len(data['entries'])} source(s))")
        except OSError as e:
            print(f"  ⚠️  Could not write {target_path.name}: {e}")


# ── Snapshot system ───────────────────────────────────────────────────────────

def ann_id(ann: dict) -> str:
    """Generate a stable unique ID for an annotation.
    Uses ann_key only — this is Zotero's own unique key and never changes,
    unlike text/comment which change when you edit annotations.
    """
    return ann.get('ann_key', '') or hashlib.md5(
        f"{ann.get('highlighted_text','')}-{ann.get('comment','')}".encode(),
        usedforsecurity=False
    ).hexdigest()


def load_snapshot(path: str) -> dict:
    """Load the last sync snapshot. Returns empty dict if none exists."""
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_snapshot(path: str, snapshot: dict):
    """Save the current sync state atomically (write to temp, then rename)."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, indent=2)
    os.replace(tmp_path, path)


def atomic_write_text(path: Path, content: str, encoding: str = 'utf-8'):
    """Write content atomically: write to temp file in same dir, then rename.
    Prevents partial files if the process is interrupted mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding=encoding) as f:
            f.write(content)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def mark_synced(paper_key: str, annotations: list, snapshot: dict):
    """Record these annotations as synced in the snapshot."""
    snapshot.setdefault('synced', {}).setdefault(paper_key, [])
    existing = set(snapshot['synced'][paper_key])
    for ann in annotations:
        existing.add(ann_id(ann))
    ids = list(existing)
    # Safety cap: a single paper should never accumulate more than 5000 IDs.
    # In practice this limit is never reached; it prevents runaway growth from bugs.
    if len(ids) > 5000:
        ids = ids[-5000:]
    snapshot['synced'][paper_key] = ids


def prune_snapshot(snapshot: dict, active_paper_keys: set):
    """Remove snapshot entries for papers no longer in the library.
    Prevents unbounded growth over years of use."""
    for section in ('synced', 'dismissed', 'concept_hash', 'concept_blocks'):
        if section in snapshot:
            stale = [k for k in snapshot[section] if k not in active_paper_keys]
            for k in stale:
                del snapshot[section][k]


def paper_state_hash(annotations: list, manual_links: list,
                     paper_meta: tuple = ()) -> str:
    """Hash the full state that drives concept/target note generation.
    Covers annotation state, manual-link state, and paper-level metadata
    (title, item_type, book_title, pages) so that any change affecting the
    rendered output — including reclassification or title edits — triggers a
    rebuild (#6).
    """
    ann_state = sorted(
        (
            a.get('ann_key', ''),
            a.get('color', ''),
            a.get('comment', ''),
            a.get('highlighted_text', ''),
            a.get('page_label', ''),      # shown as "p. X" in rendered entries
            str(a.get('ann_type', 1)),    # sticky note vs highlight renders differently
            str(a.get('sort_index', '')), # affects ordering in concept entries
            a.get('att_key', ''),         # used to build Zotero deep links
        )
        for a in annotations
    )
    link_state = sorted((lt, ab or '', ut) for lt, ab, ut in manual_links)
    combined = json.dumps(
        {'meta': list(paper_meta), 'anns': ann_state, 'links': link_state},
        sort_keys=True
    )
    return hashlib.md5(combined.encode(), usedforsecurity=False).hexdigest()


def get_revoked_ann_ids(paper_key: str, all_synced_anns: list,
                         all_ann_colors: dict, snapshot: dict) -> set:
    """
    Return ann_ids that were previously synced but whose color has changed
    away from green — meaning they should be removed from Obsidian.
    """
    GREEN = {'#5fb236', '#7cc868', '#a28ae5', '#aaaaaa'}  # green + purple + grey — all kept
    synced_ids = set(snapshot.get('synced', {}).get(paper_key, []))
    dismissed  = set(snapshot.get('dismissed', {}).get(paper_key, []))
    revoked = set()

    for ann in all_synced_anns:
        aid = ann_id(ann)
        if aid in synced_ids and aid not in dismissed:
            ak = ann.get('ann_key', '')
            color = all_ann_colors.get(ak, '')
            if color and color not in GREEN:
                revoked.add(aid)
                snapshot.setdefault('dismissed', {}).setdefault(paper_key, [])
                if aid not in snapshot['dismissed'][paper_key]:
                    snapshot['dismissed'][paper_key].append(aid)
    return revoked


# ── Vault link resolver ───────────────────────────────────────────────────────

def build_vault_index(vault_path: Path, sources_dir: str = None) -> dict:
    """
    Scan the vault (excluding Sources) to build a case-insensitive
    map of note names to file paths.
    """
    index = {}
    # Exclude the Sources/ parent (which contains Subjects/ and Authors/) from
    # the vault concept index — source notes are not concept targets.
    _sources_root_name = Path(sources_dir or DEFAULT_SOURCES_DIR).parent.name.lower()
    _excluded = {_sources_root_name}
    for item in vault_path.iterdir():
        if item.is_dir() and item.name.lower() not in _excluded and not item.name.startswith('.'):
            for md_file in sorted(item.rglob("*.md")):
                key = _norm_key(md_file.stem)
                if key in index:
                    try:
                        _p1 = index[key].relative_to(vault_path)
                        _p2 = md_file.relative_to(vault_path)
                    except ValueError:
                        _p1, _p2 = index[key], md_file
                    print(f"  ⚠️  Duplicate note '{md_file.stem}': {_p1} vs {_p2} — links resolve to first")
                else:
                    index[key] = md_file
        elif item.is_file() and item.suffix == '.md':
            key = _norm_key(item.stem)
            if key not in index:
                index[key] = item
    return index


def _edit_distance_1(s1: str, s2: str):
    """
    Returns (True, diffchars) if strings differ by exactly 1 edit:
    substitution, insertion, deletion, or transposition of adjacent chars.
    diffchars is a tuple of the character(s) involved in the change.
    Returns (False, None) if edit distance != 1.
    Using Damerau-Levenshtein so common typos like 'evidnece' are caught.
    """
    l1, l2 = len(s1), len(s2)
    if abs(l1 - l2) > 1:
        return False, None
    if l1 == l2:
        diffs = [i for i in range(l1) if s1[i] != s2[i]]
        if len(diffs) == 1:
            return True, (s1[diffs[0]], s2[diffs[0]])      # substitution
        if (len(diffs) == 2
                and s1[diffs[0]] == s2[diffs[1]]
                and s1[diffs[1]] == s2[diffs[0]]):
            return True, (s1[diffs[0]], s1[diffs[1]])       # transposition
        return False, None
    shorter, longer = (s1, s2) if l1 < l2 else (s2, s1)
    for i in range(len(longer)):
        if longer[:i] + longer[i+1:] == shorter:
            return True, (longer[i],)                       # insertion/deletion
    return False, None


def resolve_link(link_target: str, vault_index: dict,
                 vault_path: Path, to_organize_path: Path) -> Path:
    """
    Resolve a [[link]] to its actual file path:
    1. If it contains '/' — treat as explicit path (e.g. [[Writing Materials/Chap. 4]])
    2. If the note already exists somewhere in the vault — return that path
    3. Otherwise — return To_Organize/link_target.md
    """
    link_target = _clean_link_target(link_target)
    # Guard against directory traversal attempts
    if '..' in link_target:
        safe_name = _RE_UNSAFE_CHARS.sub('', link_target).strip()
        return to_organize_path / (safe_name + ".md")
    if '/' in link_target:
        resolved = vault_path / (link_target + ".md")
        # Ensure resolved path stays within vault
        try:
            resolved.relative_to(vault_path)
        except ValueError:
            safe_name = _RE_UNSAFE_CHARS.sub('', link_target.replace('/', '_')).strip()
            return to_organize_path / (safe_name + ".md")
        return resolved

    key = _norm_key(link_target)
    # vault_index keys are already normalized — direct lookup is sufficient
    if key in vault_index:
        return vault_index[key]

    # Fuzzy fallback: accept edit distance 1 unless the differing char is a digit.
    # This catches [[chap.1]] → [[chap. 1]] (space, not digit) and [[evidnece]] → [[evidence]]
    # but rejects [[chap. 1]] → [[chap. 2]] (the differing char '1'/'2' is a digit).
    # Only triggers if exactly one vault entry is an unambiguous match.
    matches = []
    for idx_key, idx_path in vault_index.items():
        is_close, diff = _edit_distance_1(key, idx_key)
        if is_close and diff and not any(c.isdigit() for c in diff):
            matches.append(idx_path)
    if len(matches) == 1:
        return matches[0]

    safe_name = _RE_CONTROL_CHARS.sub('', link_target)
    safe_name = _RE_UNSAFE_CHARS.sub('', safe_name)
    safe_name = _RE_SPACES.sub(' ', safe_name).strip().lstrip('.')
    safe_name = (safe_name[:120] if safe_name else "Untitled")
    return to_organize_path / (safe_name + ".md")


def cleanup_stale_to_organize(vault_path: Path, to_organize_path: Path, vault_index: dict):
    """Remove files from To_Organize that now have a proper home elsewhere in the vault.
    This happens when a link like [[husserl's truth]] was first routed to To_Organize,
    then later the correct-case file [[Husserl's truth]] was created in Concepts.
    """
    if not to_organize_path.exists():
        return
    removed = 0
    for md_file in to_organize_path.glob("*.md"):
        content = md_file.read_text(encoding='utf-8', errors='replace')
        # Never touch files with no auto-block markers — they are manual/user files
        if CONCEPT_T_START not in content:
            continue
        key = md_file.stem.lower()
        if key in vault_index and vault_index[key] != md_file:
            # A file with this name exists elsewhere — this To_Organize copy is stale
            # Only remove if it has no manual content outside the auto-block
            if CONCEPT_T_START in content and CONCEPT_T_END in content:
                end = content.find(CONCEPT_T_END) + len(CONCEPT_T_END)
                after = content[end:].strip()
                if not after:
                    md_file.unlink()
                    print(f"  🗑️  Removed stale To_Organize/{md_file.name} (now in {vault_index[key].parent.name}/)")
                    removed += 1
    if removed:
        print(f"  Cleaned {removed} stale To_Organize file(s).")


def cleanup_removed_papers(papers: dict, sources_path: Path,
                           authors_path: Path = None, dry_run: bool = False):
    """
    Remove Source files for papers that no longer exist in Zotero
    or have been moved out of the PHD Dissertation collection.
    """
    expected_files = set()
    for pid, paper in papers.items():
        item_type  = paper.get('item_type', '')
        book_title = (paper.get('book_title') or '') if item_type == 'bookSection' else ''
        title      = paper.get('title') or book_title or 'Untitled'
        subcolls   = paper.get('subcollections', [])
        # Match the same prefix logic used in the main sync loop
        if book_title and item_type == 'bookSection':
            pages_field = paper.get('pages') or ''
            first_page_str = _RE_PAGE_SPLIT.split(pages_field)[0].strip()
            try:
                sort_prefix = f"p. {int(first_page_str):03d} — "
                filename = safe_filename(sort_prefix + title) + ".md"
            except (ValueError, TypeError):
                filename = safe_filename(title) + ".md"
        else:
            filename = safe_filename(title) + ".md"
        if book_title:
            if subcolls:
                for sc in subcolls:
                    expected_files.add((sources_path / safe_filename(sc) / safe_filename(book_title) / filename).resolve())
            else:
                expected_files.add((sources_path / safe_filename(book_title) / filename).resolve())
        elif subcolls:
            for sc in subcolls:
                expected_files.add((sources_path / safe_filename(sc) / filename).resolve())
        else:
            expected_files.add((sources_path / filename).resolve())
        if authors_path:
            _af = author_folder(paper)
            if book_title:
                expected_files.add((authors_path / _af / safe_filename(book_title) / filename).resolve())
            else:
                expected_files.add((authors_path / _af / filename).resolve())

    _scan_roots = [sources_path]
    if authors_path and authors_path.exists():
        _scan_roots.append(authors_path)
    removed = 0
    for _root in _scan_roots:
        for md_file in _root.rglob("*.md"):
            if md_file.resolve() not in expected_files:
                if dry_run:
                    print(f"  [dry-run] Would remove: {md_file.name}")
                else:
                    file_text = md_file.read_text(encoding="utf-8", errors="replace")
                    if "zotero_sync_managed: true" not in file_text:
                        continue
                    after_content = ""
                    if ZOTERO_END in file_text:
                        end_pos = file_text.rfind(ZOTERO_END) + len(ZOTERO_END)
                        after_content = file_text[end_pos:].strip()
                    if after_content:
                        print(f"  ⚠️  Kept (has manual notes): {md_file.name}")
                    else:
                        md_file.unlink()
                        print(f"  🗑️  Removed: {md_file.name}")
                        removed += 1

    # Remove empty subdirectories in both trees
    for _root in _scan_roots:
        for subdir in sorted(_root.rglob("*"), reverse=True):
            if subdir.is_dir() and not any(subdir.iterdir()):
                subdir.rmdir()

    if removed:
        print(f"  Cleaned up {removed} stale source file(s).")


def write_to_read_file(unread: dict, to_organize_path: Path):
    """
    Write To_Read.md — papers with no real annotations, grouped by subcollection.
    Two sections:
      "Started" — has an overview sticky note but no highlights yet
      (ungrouped)  — truly unread, no annotations at all
    """
    # Split into overview-only vs truly unread
    started = {pid: p for pid, p in unread.items() if p.get("_overview_only")}
    truly_unread = {pid: p for pid, p in unread.items() if not p.get("_overview_only")}

    def _group_by_subcoll(papers):
        groups = {}
        for pid, paper in papers.items():
            subcolls = paper.get('subcollections', [])
            if subcolls:
                for sc in subcolls:
                    groups.setdefault(sc, []).append(paper)
            else:
                groups.setdefault('Unsorted', []).append(paper)
        return groups

    started_groups = _group_by_subcoll(started)
    unread_groups  = _group_by_subcoll(truly_unread)

    if not started_groups and not unread_groups:
        return

    def _paper_line(paper):
        title = paper.get('title', 'Untitled')
        authors = format_creators(paper)
        pub_date = paper.get('pub_date', '')
        year = _extract_year(pub_date)
        author_year = f" — {authors}" + (f", {year}" if year else "")
        zot_key = paper.get('key', '')
        zot_link = f" [↗](zotero://select/library/items/{zot_key})" if zot_key else ""
        return f"- {title}{author_year}{zot_link}"

    total_unread = len(truly_unread) + len(started)
    now_str = datetime.now().strftime('%Y-%m-%d')
    lines = ["# To Read", "",
             f"*{len(truly_unread)} unread · {len(started)} started — updated {now_str}*",
             ""]

    if started_groups:
        lines.append("## 📖 Started (overview note only)")
        lines.append("")
        for group_name in sorted(started_groups.keys()):
            if len(started_groups) > 1:
                lines.append(f"### {group_name}")
                lines.append("")
            for paper in started_groups[group_name]:
                lines.append(_paper_line(paper))
        lines.append("")

    if unread_groups:
        lines.append("## 📚 Not Yet Started")
        lines.append("")
        for group_name in sorted(unread_groups.keys()):
            lines.append(f"### {group_name}")
            lines.append("")
            for paper in unread_groups[group_name]:
                lines.append(_paper_line(paper))
            lines.append("")

    to_organize_path.mkdir(parents=True, exist_ok=True)
    out_path = to_organize_path / "To_Read.md"
    atomic_write_text(out_path, '\n'.join(lines))
    print(f"  📋 To_Read.md updated ({total_unread} unread papers)")


# ── Case B: multi-paper book partitioning ────────────────────────────────────

def _page_label_to_int(page_label: str):
    """Convert a plain-integer page label to int; return None for roman/alpha labels."""
    if not page_label:
        return None
    try:
        return int(page_label.strip())
    except ValueError:
        return None


def _page_range_str(annotations: list) -> str:
    """Return 'X–Y' page range string for a group of annotations."""
    nums = [n for a in annotations
            for n in [_page_label_to_int(a.get('page_label', ''))]
            if n is not None]
    if not nums:
        return ''
    lo, hi = min(nums), max(nums)
    return f"{lo}–{hi}" if lo != hi else str(lo)


def _is_case_b(paper: dict) -> bool:
    """Return True if this item should be partitioned into multiple source files.

    Triggers for:
    - item_type == 'book' with editors and no authors — edited volumes whose
      annotations span multiple chapters (monographs authored by a single person
      should NOT be partitioned; they are one coherent work).
    - item_type == 'bookSection' with no title set in Zotero — acts as a
      whole-book container rather than a single chapter entry.

    NOTE: This function cannot resolve the underlying Zotero model limitation.
    Annotations are always attached to a single parent attachment item. True
    per-chapter annotation scoping would require separate Zotero items (one
    bookSection per chapter) or a Zotero plugin — this heuristic is the best
    approximation available without modifying the Zotero database schema.
    """
    itype = paper.get('item_type', '')
    if itype == 'book':
        # Partition if: (a) editors present and no authors (classic edited volume)
        # OR (b) user has added explicit [paper: Title] marker stickies — they want
        # partitioning regardless of how the book is catalogued in Zotero.
        has_authors = bool(paper.get('authors'))
        has_editors = bool(paper.get('editors'))
        has_markers = any(
            parse_paper_marker((a.get('comment') or '').strip())[0] is not None
            for a in paper.get('annotations', [])
            if a.get('ann_type') == 2
        )
        return (has_editors and not has_authors) or has_markers
    if itype == 'bookSection' and not paper.get('title'):
        return True
    return False


def _gap_split(annotations: list, gap_threshold: int) -> list:
    """Split a sorted annotation list into sublists wherever page gap > gap_threshold.
    Returns a list of lists (each sublist is a group of annotations).
    """
    if not annotations:
        return []
    groups = []
    current = [annotations[0]]
    prev_page = _page_label_to_int(annotations[0].get('page_label', ''))
    for ann in annotations[1:]:
        page = _page_label_to_int(ann.get('page_label', ''))
        if prev_page is not None and page is not None and page - prev_page > gap_threshold:
            groups.append(current)
            current = []
        current.append(ann)
        if page is not None:
            prev_page = page
    groups.append(current)
    return [g for g in groups if g]


def partition_annotations(annotations: list, gap_threshold: int = 50) -> list:
    """Split a flat annotation list into per-paper groups.

    Strategy (priority order):
    1. [Paper: Title] sticky-note markers  → explicit named boundaries
    2. Page-number gap > gap_threshold     → heuristic boundaries
    3. Fallback                            → single group (no split)

    Returns a list of dicts: {'title': str|None, 'annotations': list}
    The 'title' is the marker text when markers were used, otherwise None.
    Marker annotations themselves are excluded from all groups.
    """
    if not annotations:
        return [{'title': None, 'annotations': [], 'authors': None}]

    sorted_anns = sorted(
        annotations,
        key=lambda a: (a.get('sort_index') or '', a.get('page_label') or '')
    )

    # ── Pass 1: explicit [Paper: Title] markers ───────────────────────────────
    groups = []
    current_title   = None
    current_authors = None
    current_group   = []
    found_markers   = False

    for ann in sorted_anns:
        comment = (ann.get('comment') or '').strip()
        _pm_title, _pm_authors, _pm_body = parse_paper_marker(comment)
        if _pm_title is not None:
            found_markers = True
            if current_group:
                groups.append({'title': current_title, 'annotations': current_group,
                               'authors': current_authors})
            current_title   = _pm_title
            current_authors = _pm_authors
            current_group   = []
            # Marker annotation itself excluded from all annotation groups
        else:
            current_group.append(ann)

    if current_group:
        groups.append({'title': current_title, 'annotations': current_group,
                       'authors': current_authors})

    if found_markers:
        # Markers take priority — but a single marker may cover multiple chapters.
        # Sub-split each marker group by page gaps so that:
        #   - The first sub-group gets the marker title
        #   - Subsequent sub-groups within the same marker get pp.X-Y names
        result_groups = []
        for g in groups:
            if not g['annotations']:
                continue
            sub = _gap_split(g['annotations'], gap_threshold)
            if len(sub) == 1:
                result_groups.append({'title': g['title'], 'annotations': sub[0],
                                      'authors': g['authors']})
            else:
                # First sub-group inherits marker title; rest are unnamed
                for j, sub_anns in enumerate(sub):
                    result_groups.append({
                        'title':   g['title'] if j == 0 else None,
                        'annotations': sub_anns,
                        'authors': g['authors'] if j == 0 else None,
                    })
        non_empty = [g for g in result_groups if g['annotations']]
        if non_empty:
            return non_empty

    # ── Pass 2: page-gap heuristic (no markers found) ────────────────────────
    non_empty = [g for g in _gap_split(sorted_anns, gap_threshold) if g]
    if len(non_empty) > 1:
        return [{'title': None, 'annotations': a, 'authors': None} for a in non_empty]

    # ── Fallback: keep everything together ────────────────────────────────────
    return [{'title': None, 'annotations': sorted_anns, 'authors': None}]


def _expand_case_b_papers(papers: dict) -> dict:
    """Expand Case B items into virtual per-paper entries keyed by '<key>:partN'.

    Non-Case-B papers pass through unchanged.
    Virtual papers inherit all parent fields and override:
      title, item_type, book_title, pages, annotations, _virtual_key
    The original key is preserved so Zotero deep links still work.
    """
    result = {}
    for pid, paper in papers.items():
        if not _is_case_b(paper):
            result[pid] = paper
            continue

        annotations = paper.get('annotations', [])
        if not annotations:
            result[pid] = paper
            continue

        groups = partition_annotations(annotations)

        if len(groups) == 1 and not groups[0].get('title'):
            # Single group with no marker title — treat as normal single-file paper
            result[pid] = paper
            continue
        # Single group WITH a marker title: still create a virtual paper so it
        # gets nested correctly under Sources/<Collection>/<BookTitle>/<ChapterTitle>.md

        parent_key   = paper.get('key', str(pid))
        # For 'book' items the book_title for virtual entries is the item's own title
        parent_title = paper.get('title') or paper.get('book_title') or 'Untitled'

        print(f"  📖 Partitioning '{parent_title}' → {len(groups)} part(s)")

        for i, group in enumerate(groups):
            group_anns   = group['annotations']
            marker_title = group.get('title')

            marker_authors = group.get('authors')  # from [paper: Title [Author]]
            if marker_title:
                virtual_title = marker_title
            else:
                page_range = _page_range_str(group_anns)
                virtual_title = f"pp. {page_range}" if page_range else f"Part {i + 1}"

            # Include first page in key for stability when annotation count shifts.
            first_page = (group_anns[0].get('page_label') or '').replace(' ', '')
            virtual_key = f"{parent_key}:p{first_page}:{i}" if first_page else f"{parent_key}:part{i}"
            pages_for_meta    = _page_range_str(group_anns)

            virtual_paper = dict(paper)  # shallow copy — inherits att_key, zot_key, etc.
            virtual_paper['title']           = virtual_title  # clean title — prefix added in _run
            virtual_paper['item_type']       = 'bookSection'
            virtual_paper['book_title']      = parent_title
            virtual_paper['pages']           = pages_for_meta
            virtual_paper['annotations']     = group_anns
            virtual_paper['_virtual_key']    = virtual_key
            # Author from [paper: Title [Author]] marker overrides inherited authors
            if marker_authors:
                # Parse comma-separated names into a list
                virtual_paper['authors'] = [n.strip() for n in marker_authors.split(',') if n.strip()]
                virtual_paper['editors'] = []  # clear editors — this entry has known authors

            result[f"{pid}:part{i}"] = virtual_paper

    return result


# ── Main run ──────────────────────────────────────────────────────────────────

LOCK_FILE = "/tmp/zotero_obsidian_sync.lock"


def run(zotero_db: str, sources_dir: str, concepts_dir: str,
        vault_dir: str, dry_run: bool = False):
    # ── Prevent multiple instances running simultaneously ─────────────────────
    lock_file = Path(LOCK_FILE)
    # Atomic lock: O_CREAT|O_EXCL creates the file only if it does not exist,
    # eliminating the exists()+write() race condition.
    try:
        fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        try:
            pid = int(lock_file.read_text(encoding='utf-8', errors='replace').strip())
            lock_age_s = datetime.now().timestamp() - lock_file.stat().st_mtime
            if lock_age_s > 7200:
                raise ProcessLookupError("Lock is stale (> 2 hours old)")
            os.kill(pid, 0)
            print("⚠️  Another sync is already running. Skipping.")
            return
        except (FileNotFoundError, ProcessLookupError, ValueError, OSError):
            lock_file.unlink(missing_ok=True)
            # Retry once
            try:
                fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
            except FileExistsError:
                print("⚠️  Another sync is already running. Skipping.")
                return
    try:
        _run(zotero_db, sources_dir, concepts_dir, vault_dir, dry_run)
    finally:
        lock_file.unlink(missing_ok=True)


def _run(zotero_db: str, sources_dir: str, concepts_dir: str,
         vault_dir: str, dry_run: bool = False):

    sources_path     = Path(sources_dir)
    authors_path     = Path(AUTHORS_DIR)
    vault_path       = Path(vault_dir)
    to_organize_path = Path(TO_ORGANIZE_DIR)

    if not vault_path.exists():
        print(f"[ERROR] Vault directory not found: {vault_dir}")
        print("  Is iCloud Drive mounted? Check Finder → iCloud Drive.")
        sys.exit(1)

    if not dry_run:
        sources_path.mkdir(parents=True, exist_ok=True)
        authors_path.mkdir(parents=True, exist_ok=True)
        to_organize_path.mkdir(parents=True, exist_ok=True)

    # Build vault index for case-insensitive link resolution
    vault_index = build_vault_index(vault_path, sources_dir)

    # ── Load sync snapshot ────────────────────────────────────────────────────
    snapshot = load_snapshot(SNAPSHOT_FILE)

    print(f"\n📚 Reading Zotero database: {zotero_db}")
    papers, unread_papers = get_zotero_data(zotero_db)
    print(f"   Found {len(papers)} paper(s) with annotated comments.\n")
    # Expand Case B items (books / untitled bookSections) into virtual per-paper entries
    papers = _expand_case_b_papers(papers)
    # Prune stale snapshot entries for papers no longer in library.
    # Virtual papers use _virtual_key; normal papers use key.
    prune_snapshot(snapshot, {
        p.get('_virtual_key') or p.get('key')
        for p in papers.values()
        if p.get('_virtual_key') or p.get('key')
    })

    if not papers:
        print("No papers found with comments in annotations.")
        return

    # ── Clean up source files for papers removed from Zotero/PHD collection ──
    if not dry_run:
        cleanup_removed_papers(papers, sources_path, authors_path, dry_run)
        write_to_read_file(unread_papers, to_organize_path)

    all_entries = {}  # target_path → entries to write

    for pid, paper in papers.items():
        item_type  = paper.get('item_type', '')
        book_title = (paper.get('book_title') or '') if item_type == 'bookSection' else ''
        # Use book_title as fallback when a bookSection has no paper title set in Zotero
        title      = paper.get('title') or book_title or 'Untitled'
        # For all bookSection chapters (Case A and Case B), prefix filename with
        # zero-padded first page so Obsidian sorts chapters in reading order.
        if book_title and item_type == 'bookSection':
            pages_field = paper.get('pages') or ''
            first_page_str = _RE_PAGE_SPLIT.split(pages_field)[0].strip()
            try:
                filename = safe_filename(f"p. {int(first_page_str):03d} — {title}") + ".md"
            except (ValueError, TypeError):
                filename = safe_filename(title) + ".md"
        else:
            filename = safe_filename(title) + ".md"
        subcolls   = paper.get('subcollections', [])
        # Virtual (Case B) papers use _virtual_key for snapshot tracking so that
        # each partition has its own independent state entry.
        paper_key  = paper.get('_virtual_key') or paper.get('key', str(pid))
        # Non-empty only for Case B virtual papers — drives ## Edited Book: heading
        virtual_book_title = book_title if '_virtual_key' in paper else ''

        # Build list of source directories (Subjects/ — Zotero collection hierarchy).
        if book_title:
            if subcolls:
                source_dirs = [sources_path / safe_filename(sc) / safe_filename(book_title)
                               for sc in subcolls]
            else:
                source_dirs = [sources_path / safe_filename(book_title)]
        elif subcolls:
            source_dirs = [sources_path / safe_filename(sc) for sc in subcolls]
        else:
            source_dirs = [sources_path]

        # Authors/ directory: Authors/<Last, First>/ or Authors/<Last, First>/<BookTitle>/
        _afolder = author_folder(paper)
        if book_title:
            author_dirs = [authors_path / _afolder / safe_filename(book_title)]
        else:
            author_dirs = [authors_path / _afolder]
        all_dirs = source_dirs + author_dirs

        if not dry_run:
            for sd in all_dirs:
                sd.mkdir(parents=True, exist_ok=True)

        label = f" [bookSection in {book_title}]" if book_title else (f" ({len(source_dirs)} collections)" if len(source_dirs) > 1 else "")
        is_virtual = '_virtual_key' in paper
        if is_virtual:
            print(f"  📄 {title}{label}")
        else:
            print(f"📄 {title}{label}")

        # ── Check for color-revoked annotations (changed away from green) ─────
        all_anns       = paper.get('annotations', [])
        all_ann_colors = paper.get('all_ann_colors', {})
        if not dry_run:
            revoked = get_revoked_ann_ids(paper_key, all_anns, all_ann_colors, snapshot)
            if revoked:
                print(f"  🔴 {len(revoked)} annotation(s) removed (color changed)")

        # ── Preserve manual content — read from ALL source dirs, use first non-empty ─
        source_content = None
        source_file_used = all_dirs[0] / filename
        for sd in all_dirs:
            candidate = sd / filename
            if candidate.exists():
                content = candidate.read_text(encoding='utf-8', errors='replace')
                if content.strip():
                    source_content = content
                    source_file_used = candidate
                    break
        before, after = extract_manual_section(source_file_used, source_content)
        inter_notes   = extract_inter_annotation_notes(source_file_used, source_content)
        # Merge manual links from all source dir copies — each may have unique manual edits.
        manual_links = extract_manual_links(source_file_used, source_content)
        for _sd in source_dirs:
            _candidate = _sd / filename
            if _candidate != source_file_used and _candidate.exists():
                _extra = extract_manual_links(_candidate)
                _seen = {(lt, ab, ut) for lt, ab, ut in manual_links}
                manual_links += [x for x in _extra if x not in _seen]

        # ── Build active annotation list ──────────────────────────────────────
        active_anns = all_anns  # alias — list is not modified
        paper['annotations_for_display'] = active_anns

        # ── Detect whether concept/target rebuild can be skipped (#6) ─────────
        # Hash covers both annotation state and manual-link state so that changes
        # to either are caught. Always written so the snapshot is self-healing.
        _current_state_hash = paper_state_hash(
            active_anns, manual_links,
            paper_meta=(
                title,
                item_type,
                book_title,
                paper.get('pages', '') if item_type == 'bookSection' else '',
                ','.join(paper.get('authors') or []),
                ','.join(paper.get('editors') or []),
            )
        )
        _last_state_hash = snapshot.get('concept_hash', {}).get(paper_key)
        skip_concept_rebuild = (not dry_run and _current_state_hash == _last_state_hash)
        snapshot.setdefault('concept_hash', {})[paper_key] = _current_state_hash

        # ── Write Source note to ALL collection folders ───────────────────────
        if not dry_run:
            note_content = build_source_note(paper, before, after, inter_notes)
            # Safety check — never write more than MAX_SOURCE_NOTE_BYTES to a source file
            if len(note_content.encode('utf-8')) > MAX_SOURCE_NOTE_BYTES:
                print(f"  ⚠️  Content too large, skipping write for safety: {filename}")
                continue
            for sd in all_dirs:
                sf = sd / filename
                try:
                    _rel_s = None
                    try: _rel_s = f"Subjects/{sf.relative_to(sources_path)}"
                    except ValueError:
                        try: _rel_s = f"Authors/{sf.relative_to(authors_path)}"
                        except ValueError: _rel_s = sf.name
                    # Skip write if content unchanged — avoids iCloud uploads every 2min.
                    # Reuse already-read content when possible (#2/#3).
                    if sf.exists():
                        existing_text = (
                            source_content
                            if (source_content is not None and sf == source_file_used)
                            else sf.read_text(encoding='utf-8', errors='replace')
                        )
                        if existing_text == note_content:
                            continue
                    atomic_write_text(sf, note_content)
                    print(f"  [written] {_rel_s}")
                except OSError as e:
                    print(f"  ⚠️  Could not write {sf.name}: {e}")
        # ── Report new vs already-synced (before mark_synced so count is accurate)
        synced_ids = set(snapshot.get('synced', {}).get(paper_key, []))
        commented_anns = [a for a in active_anns if a.get('comment') or a.get('ann_type') == 2]
        new_anns = [a for a in commented_anns if ann_id(a) not in synced_ids]
        if not dry_run:
            if new_anns:
                print(f"  ✨ {len(new_anns)} new annotation(s) synced")
            else:
                print(f"  ✓ {len(active_anns)} annotation(s) synced")
            # Mark ALL active annotations as synced
            mark_synced(paper_key, active_anns, snapshot)

        # ── Collect concept/target entries ────────────────────────────────────
        if skip_concept_rebuild:
            # Replay cached rendered blocks so unchanged papers still contribute
            # to concept notes — without this, their entries would disappear (#6).
            cached_blocks = snapshot.get('concept_blocks', {}).get(paper_key, {})
            if cached_blocks:
                # Validate all cached paths before using any: if a concept file was moved
                # in the vault the vault_index will point to its new location — using the
                # stale cached path would create a duplicate entry at the old location.
                cache_valid = all(
                    vault_index.get(_norm_key(Path(tp).stem)) in (None, Path(tp))
                    for tp in cached_blocks
                )
                if cache_valid:
                    print(f"  ↩ Concept entries unchanged — loading {len(cached_blocks)} block(s) from cache")
                    for target_path_str, cached_val in cached_blocks.items():
                        entry_text, first_sort = cached_val[0], cached_val[1]
                        cached_bt = cached_val[2] if len(cached_val) > 2 else ''
                        collect_concept_entry(all_entries, Path(target_path_str), filename, entry_text, first_sort, cached_bt)
                else:
                    skip_concept_rebuild = False   # cached path moved — rebuild
            else:
                skip_concept_rebuild = False   # no cache yet — fall through to rebuild

        if not skip_concept_rebuild:
            # Collect into a per-paper temp dict so we can cache final merged blocks.
            paper_entries = {}
            concept_map = {}
            for ann in active_anns:
                comment = ann.get('comment', '')
                if not comment:
                    continue
                ann_color = (ann.get('color') or '').lower()
                if ann_color == '#a28ae5':
                    # Purple: use parse_purple_comment to get clean headline
                    headline, _ = parse_purple_comment(comment)
                    _, concepts = parse_comment(comment)
                    # Always route purple annotations to shared Thoughts and Directions file
                    thoughts_file = vault_path / THOUGHTS_DIR
                    entry = build_concept_entry_from_zotero(filename, [(ann, headline, concepts)], paper)
                    collect_concept_entry(paper_entries, thoughts_file, filename, entry, ann.get('sort_index') or '', virtual_book_title)
                elif ann_color == '#aaaaaa':
                    # Grey: use parse_grey_comment for clean headline (strips page ref)
                    headline, _ = parse_grey_comment(comment)
                    _, concepts = parse_comment(comment)
                else:
                    headline, concepts = parse_comment(comment)
                for concept in concepts:
                    concept_map.setdefault(concept, [])
                    concept_map[concept].append((ann, headline, concepts))

            for concept_name, anns in concept_map.items():
                if dry_run:
                    print(f"  [dry-run] Would update concept: {concept_name}")
                    continue
                concept_file = resolve_link(concept_name.strip(), vault_index, vault_path, to_organize_path)
                entry = build_concept_entry_from_zotero(filename, anns, paper, current_concept=concept_name)
                first_sort = min((a[0].get('sort_index') or '' for a in anns), default='')
                collect_concept_entry(paper_entries, concept_file, filename, entry, first_sort, virtual_book_title)
                vault_index[_norm_key(concept_name)] = concept_file

            # ── Collect manual [[links]] → target entries ─────────────────────
            manual_by_target = {}
            for link_target, ann_block, user_text in manual_links:
                manual_by_target.setdefault(link_target, []).append((ann_block, user_text))

            for link_target, contexts in manual_by_target.items():
                if dry_run:
                    print(f"  [dry-run] Would send manual link to: {link_target}")
                    continue
                target_file = resolve_link(link_target, vault_index, vault_path, to_organize_path)
                entry = build_concept_entry_from_manual(filename, contexts, paper)
                collect_concept_entry(paper_entries, target_file, filename, entry, '', virtual_book_title)
                vault_index[_norm_key(link_target)] = target_file

            # Transfer per-paper entries into global all_entries and cache them.
            # collect_concept_entry() merges by paper_filename, so each target_path
            # has exactly one entry per paper in paper_entries.
            # Assert this invariant explicitly so a violation raises immediately
            # rather than silently dropping or overwriting data.
            paper_cache = {}
            for tkey, tdata in paper_entries.items():
                entries = tdata['entries']
                if len(entries) != 1:
                    raise RuntimeError(
                        f"Expected exactly 1 entry per paper in paper_entries for {tkey}, "
                        f"got {len(entries)}. Bug in collect_concept_entry()."
                    )
                fname, entry_text, first_sort, e_book_title = entries[0]
                collect_concept_entry(all_entries, tdata['path'], fname, entry_text, first_sort, e_book_title)
                paper_cache[tkey] = (entry_text, first_sort, e_book_title)
            if not dry_run:
                snapshot.setdefault('concept_blocks', {})[paper_key] = paper_cache

    # ── Write all target notes ────────────────────────────────────────────────
    if not dry_run and all_entries:
        print(f"\n📝 Writing concept/target notes...")
        # Must use the exact same title fallback as the write loop so that
        # concept-note cleanup cannot remove live entries due to filename mismatch.
        active_filenames = set()
        for _p in papers.values():
            _itype = _p.get('item_type', '')
            _bt    = _p.get('book_title', '') if _itype == 'bookSection' else ''
            _title = _p.get('title') or _bt or 'Untitled'
            # Apply same prefix logic as the write loop so _filter_dead_entries matches
            if _bt and _itype == 'bookSection':
                _pages_field = _p.get('pages') or ''
                _fps = _RE_PAGE_SPLIT.split(_pages_field)[0].strip()
                try:
                    _pfx = f"p. {int(_fps):03d} — "
                    active_filenames.add(safe_filename(_pfx + _title) + ".md")
                except (ValueError, TypeError):
                    active_filenames.add(safe_filename(_title) + ".md")
            else:
                active_filenames.add(safe_filename(_title) + ".md")
        write_all_target_notes(all_entries, active_filenames, vault_path)
        # Clean up To_Organize files that now have a proper home in Concepts etc.
        # Reuse already-built vault_index — no need to rebuild
        cleanup_stale_to_organize(vault_path, to_organize_path, vault_index)

    # ── Save updated snapshot ─────────────────────────────────────────────────
    if not dry_run:
        save_snapshot(SNAPSHOT_FILE, snapshot)
        print(f"  💾 Sync state saved.")

    print(f"\n✅ Done! Processed {len(papers)} paper(s).")


_LAUNCHD_LOGS = ("/tmp/zotero_sync.log", "/tmp/zotero_sync_err.log")


def main():
    # Rotate launchd log files so each run starts fresh but the previous run
    # is preserved as .1 for post-hoc debugging.  launchd opens them with
    # O_APPEND, so truncating from inside the script resets the write position.
    for _log in _LAUNCHD_LOGS:
        try:
            if os.path.exists(_log) and os.path.getsize(_log) > 0:
                shutil.copy2(_log, _log + ".1")  # copy content, keep original inode
            os.truncate(_log, 0)                 # truncate in-place; launchd's fd stays valid
        except OSError:
            pass

    parser = argparse.ArgumentParser(
        description="Zotero → Obsidian sync with manual link support."
    )
    parser.add_argument('--zotero-db',    default=DEFAULT_ZOTERO_DB)
    parser.add_argument('--sources-dir',  default=DEFAULT_SOURCES_DIR)
    parser.add_argument('--authors-dir',  default=DEFAULT_AUTHORS_DIR)
    parser.add_argument('--concepts-dir', default=DEFAULT_CONCEPTS_DIR)
    parser.add_argument('--vault-dir',    default=DEFAULT_VAULT_DIR)
    parser.add_argument('--dry-run',      action='store_true')
    args = parser.parse_args()

    if not args.sources_dir:
        print("ERROR: Please set DEFAULT_SOURCES_DIR at the top of this script.")
        sys.exit(1)
    if not PHD_COLLECTION:
        print("ERROR: Please set PHD_COLLECTION (your Zotero collection name) at the top of this script.")
        sys.exit(1)

    run(
        zotero_db=args.zotero_db,
        sources_dir=args.sources_dir,
        concepts_dir=args.concepts_dir,
        vault_dir=args.vault_dir,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
