#!/usr/bin/env python3
# Z_O_Integration_User.py
# Fill in the CONFIGURATION block below.
"""
Zotero → Obsidian Pipeline
---------------------------
- Syncs Zotero annotations into Obsidian Source files
- Protects your manual edits with <!-- ZOTERO START/END --> markers
- Any [[concept]] or [[Folder/Note]] you write ANYWHERE in a Source file
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
_RE_TASK_LINE        = re.compile(r'- \[.\]')
_RE_FIRST_MARKER     = re.compile(r'<!-- zotero-start[^>]*-->')
# ─────────────────────────────────────────────
# CONFIGURATION — set your paths here
# ─────────────────────────────────────────────
DEFAULT_ZOTERO_DB    = str(Path.home() / "Zotero" / "zotero.sqlite")
DEFAULT_SOURCES_DIR  = ""  # e.g. "/Users/yourname/Library/Mobile Documents/iCloud~md~obsidian/Documents/MyVault/Sources"
DEFAULT_CONCEPTS_DIR = ""  # kept for CLI compat — not required
# The root of your vault — used to resolve [[Folder/Note]] links
DEFAULT_VAULT_DIR    = ""  # e.g. "/Users/yourname/Library/Mobile Documents/iCloud~md~obsidian/Documents/MyVault"
# ─────────────────────────────────────────────

SNAPSHOT_FILE   = ""  # leave blank — derived automatically from DEFAULT_VAULT_DIR
TO_ORGANIZE_DIR = ""  # leave blank — derived automatically from DEFAULT_VAULT_DIR
PHD_COLLECTION  = ""  # exact name of your root Zotero collection, e.g. "My Research"

# ── Auto-derived paths ────────────────────────────────────────────────────────
if DEFAULT_VAULT_DIR:
    if not SNAPSHOT_FILE:
        SNAPSHOT_FILE = str(Path(DEFAULT_VAULT_DIR) / ".zotero_sync_state.json")
    if not TO_ORGANIZE_DIR:
        TO_ORGANIZE_DIR = str(Path(DEFAULT_VAULT_DIR) / "To_Organize")
# ─────────────────────────────────────────────

ZOTERO_START    = "<!-- zotero-start -->"
CONCEPT_T_START = "<!-- zotero-auto-start -->"
CONCEPT_T_END   = "<!-- zotero-auto-end -->"
THOUGHTS_DIR    = "To_Organize/Thoughts and Directions.md"
ZOTERO_END      = "<!-- zotero-end -->"


# ── Parsing ───────────────────────────────────────────────────────────────────

def title_case(s: str) -> str:
    """Capitalize only the first letter of the first word.
    Preserves all other casing — important for names, acronyms, German terms etc.
    (McDowell stays McDowell, AI stays AI, Husserl stays Husserl)
    """
    if not s:
        return s
    return s[0].upper() + s[1:]


def parse_comment(comment: str):
    """Extract headline and [[concepts]] from a Zotero annotation comment.
    Supports [[concept1, concept2]] comma-separated links.
    Auto-capitalizes headline to title case.
    """
    if not comment:
        return None, []
    concept_matches = re.findall(r'\[\[([^\]]+)\]\]', comment)
    concepts = []
    for match in concept_matches:
        # Split on comma to support [[concept1, concept2]]
        for c in match.split(','):
            c = c.strip()
            if c:
                concepts.append(c)
    remaining = re.sub(r'\[\[[^\]]+\]\]', '', comment).strip()
    remaining = re.sub(r'\s+', ' ', remaining).strip()
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
    bracket_match = re.match(r"^\[([^\]]+)\]", body)
    if bracket_match:
        headline = title_case(bracket_match.group(1).strip())
        body = body[bracket_match.end():].strip()
    return headline, body



def parse_grey_comment(comment: str):
    """Extract (p. X) usage reference and headline from a grey annotation comment.
    Returns (headline, doc_page) where doc_page is e.g. '45' from '(p. 45)' or '(P. 45)'.
    Headline: from [brackets] if present, otherwise the full comment text (minus page ref).
    """
    if not comment:
        return None, None
    body = comment.strip()
    headline = None
    doc_page = None
    # Extract (p. X) or (P. X) — page in the user's own document
    page_match = re.search(r'\((?:p|P)\.\s*(\d+)\)', body)
    if page_match:
        doc_page = page_match.group(1)
    # Extract [headline] if present
    bracket_match = re.match(r'^\[([^\]]+)\]', body)
    if bracket_match:
        headline = title_case(bracket_match.group(1).strip())
    else:
        # Use full comment (minus page ref) as headline — preserves original green headline
        headline_text = re.sub(r'\((?:p|P)\.\s*\d+\)', '', body).strip().strip(',.')
        if headline_text:
            headline = title_case(headline_text)
    return headline, doc_page


def extract_manual_links(source_file: Path, content: str = None) -> list:
    """
    Scan content between zotero blocks for manually added [[links]].
    For each [[link]], captures the preceding annotation block and the user's text.
    Returns list of (link_target, preceding_annotation, user_text) tuples.
    Accepts pre-read content to avoid redundant file reads.
    """
    if not source_file.exists():
        return []

    file_content = content if content is not None else source_file.read_text(encoding='utf-8', errors='replace')
    results = []

    # Split into segments: each annotation block + the text after it
    for match in _RE_ZOTERO_SEGMENT.finditer(file_content):
        annotation_block = match.group(1).strip()
        inter_text = match.group(2)

        for line in inter_text.split('\n'):
            stripped = line.strip()
            links = re.findall(r'\[\[([^\]]+)\]\]', stripped)
            if links:
                user_text = re.sub(r'\[\[[^\]]+\]\]', '', stripped).strip()
                for link in links:
                    results.append((link, annotation_block, user_text))

    return results


def extract_manual_section(source_file: Path, content: str = None) -> tuple:
    """
    Extract everything OUTSIDE all ZOTERO START/END blocks.
    'before' = everything before the first zotero-start (header)
    'after'  = everything after the last zotero-end (user notes — ALWAYS preserved)
    Accepts pre-read content to avoid redundant file reads.
    """
    if not source_file.exists():
        return "", ""

    content = content if content is not None else source_file.read_text(encoding='utf-8', errors='replace')

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
            if re.match(r'^- \[[ x]\]\s*$', s):  # only strip empty/checked auto-generated tasks
                continue
            after_lines.append(line)
        after = '\n'.join(after_lines).strip('\n')
        return before, after
    else:
        # No valid markers — return empty (file will be fully rebuilt)
        return "", ""


# ── Zotero DB ─────────────────────────────────────────────────────────────────

def get_zotero_data(db_path: str):
    if not Path(db_path).exists():
        print(f"[ERROR] Zotero database not found at: {db_path}")
        sys.exit(1)

    # Open in read-only mode via URI — safer than copying, no lock/corruption risk
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT i.itemID, i.key,
            idv_title.value AS title, idv_date.value AS pub_date,
            idv_doi.value AS doi, idv_url.value AS url,
            idv_abstract.value AS abstract, idv_journal.value AS journal,
            it.typeName AS item_type
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
        WHERE it.typeName NOT IN ('attachment','note','annotation')
        AND i.itemID NOT IN (SELECT itemID FROM deletedItems)
    """)
    papers = {row['itemID']: dict(row) for row in cur.fetchall()}

    cur.execute("""
        SELECT ia.itemID, c.firstName, c.lastName
        FROM itemCreators ia
        JOIN creators c ON ia.creatorID = c.creatorID
        JOIN creatorTypes ct ON ia.creatorTypeID = ct.creatorTypeID
        WHERE ct.creatorType = 'author'
        ORDER BY ia.itemID, ia.orderIndex
    """)
    for row in cur.fetchall():
        iid = row['itemID']
        if iid in papers:
            papers[iid].setdefault('authors', [])
            name = f"{row['firstName']} {row['lastName']}".strip()
            papers[iid]['authors'].append(name)

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


def format_authors(authors: list) -> str:
    if not authors:
        return "Unknown Author"
    if len(authors) == 1:
        return authors[0]
    if len(authors) == 2:
        return " & ".join(authors)
    return f"{authors[0]} et al."


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
        page_ref = f" *(p. {doc_page})*" if doc_page else ""
        header = f"✅ {headline}{page_ref}" if headline else f"✅ Used{page_ref}"
        lines.append(f"> [!check]- {header}")
        lines.append(f"> {text}")
        meta_parts = []
        if page:
            meta_parts.append(f"p. {page}")
        if zlink:
            meta_parts.append(zlink)
        if meta_parts:
            lines.append(f"> *{' · '.join(meta_parts)}*")
        lines.append(ZOTERO_END)
        return '\n'.join(lines)
    elif ann_type == 2:
        # Sticky note → My Note callout, with optional [My Headline]
        note_headline = None
        note_body = comment
        if comment:
            bracket_match = re.match(r'^\[([^\]]+)\]', comment.strip())
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
    if not source_file.exists():
        return {}

    # Safety check — skip files over 500KB
    if source_file.stat().st_size > 500_000:
        print(f"  ⚠️  Skipping oversized file: {source_file.name}")
        return {}

    text = content if content is not None else source_file.read_text(encoding='utf-8', errors='replace')
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
            if re.match(r'\*\(p\.', s):
                continue
            # Strip any script-generated task/checkbox lines
            if re.match(r'- \[.\]', s):
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
    title    = paper.get('title', 'Untitled')
    authors  = format_authors(paper.get('authors', []))
    pub_date = paper.get('pub_date', '')
    journal  = paper.get('journal', '')
    doi      = paper.get('doi', '')
    url      = paper.get('url', '')
    abstract = paper.get('abstract', '')
    zot_key  = paper.get('key', '')

    year = ''
    if pub_date:
        m = re.search(r'\d{4}', pub_date)
        if m:
            year = m.group()

    # Preserve original creation date if the file already exists
    created_date = datetime.now().strftime("%Y-%m-%d")
    if before:
        m = re.search(r'^created:\s*(\S+)', before, re.MULTILINE)
        if m:
            created_date = m.group(1)

    header_lines = ["---"]
    header_lines.append(f'authors: "{yaml_str(authors)}"')
    if year:
        header_lines.append(f'year: {year}')
    if journal:
        header_lines.append(f'journal: "{yaml_str(journal)}"')
    if doi:
        header_lines.append(f'doi: "{yaml_str(doi)}"')
    if zot_key:
        header_lines.append(f'zotero_key: "{yaml_str(zot_key)}"')
    header_lines.append(f'created: {created_date}')
    header_lines.append("tags: [source]")
    header_lines.append("---")
    header_lines.append("")
    att_key_paper = paper.get('att_key', '')
    if att_key_paper:
        zotero_item_link = f"[→](zotero://open-pdf/library/items/{att_key_paper})"
    elif zot_key:
        zotero_item_link = f"[→](zotero://select/library/items/{zot_key})"
    else:
        zotero_item_link = ""
    header_lines.append(f"# {title} {zotero_item_link}".strip())
    header_lines.append("")
    header_lines.append(f"**Authors:** {authors}")
    if year:
        header_lines.append(f"**Year:** {year}")
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
    lines.append(f"### From [[{paper_filename.replace('.md','')}]]")

    if paper:
        authors = format_authors(paper.get('authors', []))
        pub_date = paper.get('pub_date', '')
        year = ''
        if pub_date:
            m = re.search(r'\d{4}', pub_date)
            if m:
                year = m.group()
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
            page_ref = f" *(p. {doc_page})*" if doc_page else ""
            chk_header = f"✅ {headline}{page_ref}" if headline else f"✅ Used{page_ref}"
            lines.append(f"> [!check]- {chk_header}")
            lines.append(f"> {text}")
            if meta_parts:
                lines.append(f"> *({'  ·  '.join(meta_parts)})*")
        elif ann_color == '#a28ae5':
            # Purple = interpretive note — show with [!reading]- collapsible callout
            text = (ann.get('highlighted_text') or '').strip()
            raw_comment = (ann.get('comment') or '').strip()
            _, purple_body = parse_purple_comment(raw_comment)
            purple_body = re.sub(r'\[\[[^\]]+\]\]', '', purple_body).strip()
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
            bracket_match = re.match(r'^\[([^\]]+)\]', raw_comment)
            if bracket_match:
                note_headline = title_case(bracket_match.group(1).strip())
                note_body = raw_comment[bracket_match.end():].strip()
            note_body = re.sub(r'\[\[[^\]]+\]\]', '', note_body).strip()
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
        # Add subtle "also in" footer if annotation links to other concepts
        if other_concepts:
            also_links = ' '.join(f'[[{c}]]' for c in other_concepts)
            lines.append(f'<sub>also in: {also_links}</sub>')
        lines.append("")
    return '\n'.join(lines)


def build_concept_entry_from_manual(paper_filename: str, contexts: list) -> str:
    """Build a concept entry from manual [[links]]."""
    lines = []
    lines.append(f"### From [[{paper_filename.replace('.md','')}]]")
    lines.append("")
    seen = set()
    for ann_block, user_text in contexts:
        key = hashlib.md5((ann_block + user_text).encode()).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        clean_ann = re.sub(r'<!-- zotero-start[^>]*-->', '', ann_block)
        clean_ann = re.sub(r'<!-- zotero-end -->', '', clean_ann).strip()
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
                           first_ann_date: str = ""):
    """Add an entry to be written to a concept/target note.
    Merges multiple entries from the same source into one block."""
    key = str(concept_path)
    if key not in all_entries:
        all_entries[key] = {
            'name': concept_path.stem,
            'path': concept_path,
            'entries': []
        }
    for i, (fname, etext, edate) in enumerate(all_entries[key]['entries']):
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
                min(edate, first_ann_date) if edate and first_ann_date else edate or first_ann_date
            )
            return
    all_entries[key]['entries'].append((paper_filename, entry_text, first_ann_date))


def write_all_target_notes(all_entries: dict, active_filenames: set = None):
    """
    Write all concept/target notes using the same marker system as source files.
    Content OUTSIDE markers is always preserved.
    active_filenames: if provided, entries from removed papers are stripped.
    """
    for key, data in all_entries.items():
        target_path = data['path']
        name = data['name']

        seen_files = {}
        for fname, entry_text, first_sort in data['entries']:
            if fname not in seen_files or first_sort < seen_files[fname][1]:
                seen_files[fname] = (entry_text, first_sort)
        sorted_entries = sorted(seen_files.items(), key=lambda x: x[1][1] or '')
        T_START = CONCEPT_T_START
        T_END   = CONCEPT_T_END
        entry_blocks = [entry_text for _, (entry_text, _) in sorted_entries]
        new_zotero_block = (
            T_START + "\n\n" +
            '\n---\n\n'.join(entry_blocks) +
            "\n\n" + T_END
        )

        if target_path.exists():
            existing = target_path.read_text(encoding='utf-8', errors='replace')
            if T_START in existing and T_END in existing:
                start = existing.find(T_START)
                end = existing.find(T_END) + len(T_END)
                before = existing[:start].rstrip('\n')
                after = existing[end:].strip('\n')
                # Strip entries from papers that no longer exist in the library
                if active_filenames:
                    def _strip_dead_entries(block: str) -> str:
                        """Remove ### From [[Paper]] blocks for papers no longer active."""
                        sections = re.split(r'(?=### From \[\[)', block)
                        kept = []
                        for sec in sections:
                            m = re.match(r'### From \[\[([^\]]+)\]\]', sec.strip())
                            if m:
                                paper_stem = m.group(1).strip()
                                if (paper_stem + ".md") in active_filenames or paper_stem in active_filenames:
                                    kept.append(sec)
                            elif sec.strip():
                                kept.append(sec)
                        return ''.join(kept)
                    new_zotero_block = (
                        T_START + "\n\n" +
                        _strip_dead_entries('\n---\n\n'.join(entry_blocks)) +
                        "\n\n" + T_END
                    )
                parts = [p for p in [before, new_zotero_block, after] if p.strip()]
                full_content = '\n\n'.join(parts)
            else:
                full_content = existing.rstrip('\n') + '\n\n' + new_zotero_block
        else:
            now = datetime.now().strftime("%Y-%m-%d")
            header = f"---\nconcept: \"{name}\"\ntags: [concept]\ncreated: {now}\n---\n\n# {name}\n"
            full_content = header + "\n" + new_zotero_block

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if len(full_content.encode('utf-8')) > 500_000:
            print(f"  ⚠️  Concept file too large, skipping: {target_path.name}")
            continue
        # Skip write if content hasn't changed — avoids unnecessary iCloud uploads
        if target_path.exists():
            existing_hash = hashlib.md5(target_path.read_bytes()).hexdigest()
            new_hash = hashlib.md5(full_content.encode('utf-8')).hexdigest()
            if existing_hash == new_hash:
                continue
        try:
            target_path.write_text(full_content, encoding='utf-8')
            print(f"  [→] {target_path.relative_to(target_path.parent.parent)} ({len(data['entries'])} source(s))")
        except OSError as e:
            print(f"  ⚠️  Could not write {target_path.name}: {e}")


# ── Snapshot system ───────────────────────────────────────────────────────────

def ann_id(ann: dict) -> str:
    """Generate a stable unique ID for an annotation.
    Uses ann_key only — this is Zotero's own unique key and never changes,
    unlike text/comment which change when you edit annotations.
    """
    return ann.get('ann_key', '') or hashlib.md5(
        f"{ann.get('highlighted_text','')}-{ann.get('comment','')}".encode()
    ).hexdigest()


def load_snapshot(path: str) -> dict:
    """Load the last sync snapshot. Returns empty dict if none exists."""
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_snapshot(path: str, snapshot: dict):
    """Save the current sync state atomically (write to temp, then rename)."""
    tmp_path = path + ".tmp"
    with open(tmp_path, 'w') as f:
        json.dump(snapshot, f, indent=2)
    os.replace(tmp_path, path)


def mark_synced(paper_key: str, annotations: list, snapshot: dict):
    """Record these annotations as synced in the snapshot."""
    snapshot.setdefault('synced', {}).setdefault(paper_key, [])
    existing = set(snapshot['synced'][paper_key])
    for ann in annotations:
        existing.add(ann_id(ann))
    snapshot['synced'][paper_key] = list(existing)


def prune_snapshot(snapshot: dict, active_paper_keys: set):
    """Remove snapshot entries for papers no longer in the library.
    Prevents unbounded growth over years of use."""
    for section in ('synced', 'dismissed'):
        if section in snapshot:
            stale = [k for k in snapshot[section] if k not in active_paper_keys]
            for k in stale:
                del snapshot[section][k]


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
    def _norm(s):
        return s.lower().replace('\u2019', "'").replace('\u2018', "'")

    index = {}
    sources_name = Path(sources_dir or DEFAULT_SOURCES_DIR).name.lower()
    for item in vault_path.iterdir():
        if item.is_dir() and item.name.lower() != sources_name and not item.name.startswith('.'):
            for md_file in item.rglob("*.md"):
                key = _norm(md_file.stem)
                if key in index:
                    print(f"  ⚠️  Duplicate note name '{md_file.stem}': {index[key].parent.name}/ and {md_file.parent.name}/ — [[{md_file.stem}]] links will resolve to the first one found")
                else:
                    index[key] = md_file
        elif item.is_file() and item.suffix == '.md':
            key = _norm(item.stem)
            if key not in index:
                index[key] = item
    return index


def resolve_link(link_target: str, vault_index: dict,
                 vault_path: Path, to_organize_path: Path) -> Path:
    """
    Resolve a [[link]] to its actual file path:
    1. If it contains '/' — treat as explicit path (e.g. [[Writing Materials/Chap. 4]])
    2. If the note already exists somewhere in the vault — return that path
    3. Otherwise — return To_Organize/link_target.md
    """
    link_target = link_target.strip()
    # Guard against directory traversal attempts
    if '..' in link_target:
        safe_name = re.sub(r'[\/*?:"<>|]', '', link_target).strip()
        return to_organize_path / (safe_name + ".md")
    if '/' in link_target:
        resolved = vault_path / (link_target + ".md")
        # Ensure resolved path stays within vault
        try:
            resolved.relative_to(vault_path)
        except ValueError:
            safe_name = re.sub(r'[\/*?:"<>|]', '', link_target.replace('/', '_')).strip()
            return to_organize_path / (safe_name + ".md")
        return resolved

    # Normalize: lowercase + replace smart quotes with straight apostrophe
    def _normalize(s):
        return s.lower().replace('\u2019', "'").replace('\u2018', "'")

    key = _normalize(link_target)
    # Try exact normalized key first
    if key in vault_index:
        return vault_index[key]
    # Try all vault index keys normalized — catches any remaining casing/quote mismatches
    for idx_key, idx_path in vault_index.items():
        if _normalize(idx_key) == key:
            return idx_path

    safe_name = re.sub(r'[\\/*?:"<>|]', '', link_target).strip()
    return to_organize_path / (safe_name + ".md")


def cleanup_stale_to_organize(vault_path: Path, to_organize_path: Path, vault_index: dict):
    """Remove files from To_Organize that now have a proper home elsewhere in the vault.
    This happens when a link like [[husserl's truth]] was first routed to To_Organize,
    then later the correct-case file [[Husserl's truth]] was created in Concepts.
    """
    if not to_organize_path.exists():
        return
    protected = {'To_Read.md', 'Thoughts and Directions.md'}
    removed = 0
    for md_file in to_organize_path.glob("*.md"):
        if md_file.name in protected:
            continue
        key = md_file.stem.lower()
        if key in vault_index and vault_index[key] != md_file:
            # A file with this name exists elsewhere — this To_Organize copy is stale
            # Only remove if it has no manual content outside the auto-block
            content = md_file.read_text(encoding='utf-8', errors='replace')
            T_START = CONCEPT_T_START
            T_END = CONCEPT_T_END
            if T_START in content and T_END in content:
                end = content.find(T_END) + len(T_END)
                after = content[end:].strip()
                if not after:
                    md_file.unlink()
                    print(f"  🗑️  Removed stale To_Organize/{md_file.name} (now in {vault_index[key].parent.name}/)")
                    removed += 1
    if removed:
        print(f"  Cleaned {removed} stale To_Organize file(s).")


def cleanup_removed_papers(papers: dict, sources_path: Path, dry_run: bool = False):
    """
    Remove Source files for papers that no longer exist in Zotero
    or have been moved out of the PHD Dissertation collection.
    """
    expected_files = set()
    for pid, paper in papers.items():
        title = paper.get('title', 'Untitled')
        subcolls = paper.get('subcollections', [])
        filename = safe_filename(title) + ".md"
        if subcolls:
            for sc in subcolls:
                expected_files.add((sources_path / safe_filename(sc) / filename).resolve())
        else:
            expected_files.add((sources_path / filename).resolve())

    removed = 0
    for md_file in sources_path.rglob("*.md"):
        if md_file.resolve() not in expected_files:
            if dry_run:
                print(f"  [dry-run] Would remove: {md_file.name}")
            else:
                after_content = ""
                if md_file.exists():
                    file_text = md_file.read_text(encoding="utf-8")
                    if ZOTERO_END in file_text:
                        end_pos = file_text.rfind(ZOTERO_END) + len(ZOTERO_END)
                        after_content = file_text[end_pos:].strip()
                if after_content:
                    print(f"  ⚠️  Kept (has manual notes): {md_file.name}")
                else:
                    md_file.unlink()
                    print(f"  🗑️  Removed: {md_file.name}")
                    removed += 1

    # Remove empty subdirectories
    for subdir in sorted(sources_path.rglob("*"), reverse=True):
        if subdir.is_dir() and not any(subdir.iterdir()):
            subdir.rmdir()

    if removed:
        print(f"  Cleaned up {removed} stale source file(s).")



def write_to_read_file(unread: dict, to_organize_path: Path):
    """
    Write To_Read.md — a simple list of unannotated papers grouped by subcollection.
    Papers disappear from this list automatically once they gain green annotations.
    """
    # Group by subcollection (papers with no subcollection go under "Unsorted")
    groups = {}
    for pid, paper in unread.items():
        subcolls = paper.get('subcollections', [])
        if subcolls:
            for sc in subcolls:
                groups.setdefault(sc, []).append(paper)
        else:
            groups.setdefault('Unsorted', []).append(paper)

    if not groups:
        return

    lines = ["# To Read", ""]
    lines.append(f"*{sum(len(v) for v in groups.values())} papers — updated {datetime.now().strftime('%Y-%m-%d')}*")
    lines.append("")

    for group_name in sorted(groups.keys()):
        lines.append(f"## {group_name}")
        lines.append("")
        for paper in groups[group_name]:
            title = paper.get('title', 'Untitled')
            authors = format_authors(paper.get('authors', []))
            pub_date = paper.get('pub_date', '')
            year = ''
            if pub_date:
                m = re.search(r'\d{4}', pub_date)
                if m:
                    year = m.group()
            author_year = f" — {authors}" + (f", {year}" if year else "")
            zot_key = paper.get('key', '')
            zot_link = f" [↗](zotero://select/library/items/{zot_key})" if zot_key else ""
            lines.append(f"- {title}{author_year}{zot_link}")
        lines.append("")

    to_organize_path.mkdir(parents=True, exist_ok=True)
    out_path = to_organize_path / "To_Read.md"
    tmp_path = str(out_path) + ".tmp"
    Path(tmp_path).write_text('\n'.join(lines), encoding='utf-8')
    os.replace(tmp_path, out_path)
    print(f"  📋 To_Read.md updated ({sum(len(v) for v in groups.values())} unread papers)")


# ── Main run ──────────────────────────────────────────────────────────────────

LOCK_FILE = "/tmp/zotero_obsidian_sync.lock"


def run(zotero_db: str, sources_dir: str, concepts_dir: str,
        vault_dir: str, dry_run: bool = False):
    # ── Prevent multiple instances running simultaneously ─────────────────────
    lock_file = Path(LOCK_FILE)
    if lock_file.exists():
        try:
            pid = int(lock_file.read_text().strip())
            os.kill(pid, 0)  # Signal 0 = just check if process exists
            print("⚠️  Another sync is already running. Skipping.")
            return
        except FileNotFoundError:
            pass  # lock file disappeared between exists() and read — harmless
        except (ProcessLookupError, ValueError, OSError):
            lock_file.unlink(missing_ok=True)  # Stale lock — remove it
    lock_file.write_text(str(os.getpid()))
    try:
        _run(zotero_db, sources_dir, concepts_dir, vault_dir, dry_run)
    finally:
        lock_file.unlink(missing_ok=True)


def _run(zotero_db: str, sources_dir: str, concepts_dir: str,
         vault_dir: str, dry_run: bool = False):

    sources_path     = Path(sources_dir)
    vault_path       = Path(vault_dir)
    to_organize_path = Path(TO_ORGANIZE_DIR)

    if not dry_run:
        sources_path.mkdir(parents=True, exist_ok=True)
        to_organize_path.mkdir(parents=True, exist_ok=True)

    # Build vault index for case-insensitive link resolution
    vault_index = build_vault_index(vault_path, sources_dir)

    # ── Load sync snapshot ────────────────────────────────────────────────────
    snapshot = load_snapshot(SNAPSHOT_FILE)

    print(f"\n📚 Reading Zotero database: {zotero_db}")
    papers, unread_papers = get_zotero_data(zotero_db)
    print(f"   Found {len(papers)} paper(s) with annotated comments.\n")
    # Prune stale snapshot entries for papers no longer in library
    prune_snapshot(snapshot, {p.get("key", str(pid)) for pid, p in papers.items()})

    if not papers:
        print("No papers found with comments in annotations.")
        return

    # ── Clean up source files for papers removed from Zotero/PHD collection ──
    if not dry_run:
        cleanup_removed_papers(papers, sources_path, dry_run)
        write_to_read_file(unread_papers, to_organize_path)

    all_entries = {}  # target_path → entries to write

    for pid, paper in papers.items():
        title        = paper.get('title', 'Untitled')
        subcolls     = paper.get('subcollections', [])
        filename     = safe_filename(title) + ".md"
        paper_key    = paper.get('key', str(pid))

        # Build list of source directories — one per collection (or root if none)
        if subcolls:
            source_dirs = [sources_path / safe_filename(sc) for sc in subcolls]
        else:
            source_dirs = [sources_path]

        # Use first source dir as primary for reading manual content
        source_file = source_dirs[0] / filename
        if not dry_run:
            for sd in source_dirs:
                sd.mkdir(parents=True, exist_ok=True)

        print(f"📄 {title}" + (f" ({len(source_dirs)} collections)" if len(source_dirs) > 1 else ""))

        # ── Check for color-revoked annotations (changed away from green) ─────
        all_anns       = paper.get('annotations', [])
        all_ann_colors = paper.get('all_ann_colors', {})
        if not dry_run:
            revoked = get_revoked_ann_ids(paper_key, all_anns, all_ann_colors, snapshot)
            if revoked:
                print(f"  🔴 {len(revoked)} annotation(s) removed (color changed)")

        # ── Preserve manual content — read from ALL source dirs, use first non-empty ─
        source_content = None
        source_file_used = source_file
        for sd in source_dirs:
            candidate = sd / filename
            if candidate.exists():
                content = candidate.read_text(encoding='utf-8', errors='replace')
                if content.strip():
                    source_content = content
                    source_file_used = candidate
                    break
        before, after = extract_manual_section(source_file_used, source_content)
        inter_notes   = extract_inter_annotation_notes(source_file_used, source_content)
        manual_links  = extract_manual_links(source_file_used, source_content)

        # ── Build active annotation list ──────────────────────────────────────
        active_anns = list(all_anns)
        paper['annotations_for_display'] = active_anns
        paper['annotations_all'] = all_anns

        # ── Write Source note to ALL collection folders ───────────────────────
        if not dry_run:
            note_content = build_source_note(paper, before, after, inter_notes)
            # Safety check — never write more than 200KB to a source file
            if len(note_content.encode('utf-8')) > 200_000:
                print(f"  ⚠️  Content too large, skipping write for safety: {filename}")
                continue
            for sd in source_dirs:
                sf = sd / filename
                try:
                    sf.write_text(note_content, encoding='utf-8')
                    rel = sf.relative_to(sources_path)
                    print(f"  [written] Sources/{rel}")
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

        # ── Collect Zotero annotation → concept entries ───────────────────────
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
                collect_concept_entry(all_entries, thoughts_file, filename, entry, ann.get('sort_index') or '')
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
            collect_concept_entry(all_entries, concept_file, filename, entry, first_sort)
            vault_index[concept_name.lower()] = concept_file

        # ── Collect manual [[links]] → target entries ─────────────────────────
        manual_by_target = {}
        for link_target, ann_block, user_text in manual_links:
            manual_by_target.setdefault(link_target, []).append((ann_block, user_text))

        for link_target, contexts in manual_by_target.items():
            if dry_run:
                print(f"  [dry-run] Would send manual link to: {link_target}")
                continue
            target_file = resolve_link(link_target, vault_index, vault_path, to_organize_path)
            entry = build_concept_entry_from_manual(filename, contexts)
            collect_concept_entry(all_entries, target_file, filename, entry, '')
            vault_index[link_target.lower()] = target_file

    # ── Write all target notes ────────────────────────────────────────────────
    if not dry_run and all_entries:
        print(f"\n📝 Writing concept/target notes...")
        active_filenames = {safe_filename(p.get('title', 'Untitled')) + ".md" for p in papers.values()}
        write_all_target_notes(all_entries, active_filenames)
        # Clean up To_Organize files that now have a proper home in Concepts etc.
        cleanup_stale_to_organize(vault_path, to_organize_path, vault_index)

    # ── Save updated snapshot ─────────────────────────────────────────────────
    if not dry_run:
        save_snapshot(SNAPSHOT_FILE, snapshot)
        print(f"  💾 Sync state saved.")

    print(f"\n✅ Done! Processed {len(papers)} paper(s).")


def main():
    parser = argparse.ArgumentParser(
        description="Zotero → Obsidian sync with manual link support."
    )
    parser.add_argument('--zotero-db',    default=DEFAULT_ZOTERO_DB)
    parser.add_argument('--sources-dir',  default=DEFAULT_SOURCES_DIR)
    parser.add_argument('--concepts-dir', default=DEFAULT_CONCEPTS_DIR)
    parser.add_argument('--vault-dir',    default=DEFAULT_VAULT_DIR)
    parser.add_argument('--dry-run',      action='store_true')
    args = parser.parse_args()

    if not args.sources_dir:
        print("ERROR: Please set DEFAULT_SOURCES_DIR at the top of this script.")
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
