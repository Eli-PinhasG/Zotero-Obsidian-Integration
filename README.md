# Zotero → Obsidian Integration

A robust, researcher-focused sync tool that turns your Zotero annotations into clean, well-structured Obsidian notes — **while fully protecting every manual edit you make**.

Annotate freely in Zotero — everything appears in Obsidian in the correct reading order, respecting your Zotero collection structure, while giving you complete freedom to edit notes by hand.

Designed for long-term academic work (PhD dissertations, books, etc.) to maximize reading efficiency without losing your thoughts, links, or future-usable material.

## Features

- Full protection of manual edits using `<!-- zotero-start -->` / `<!-- zotero-end -->` markers
- Color-aware rendering with semantic meaning:
  - **Green**: Standard highlights + comments
  - **Purple**: Interpretive thoughts → automatically collected in `Thoughts and Directions.md`
  - **Grey**: “Already used/cited” → collapsible `> [!check]` callout with `(p. X)` parsing
  - **Sticky notes**: Clean `> [!note]` callouts (first one auto-titled “Overview”)
- Bidirectional [[link]] harvesting — tags anywhere in a source note
- Automatically mirrors your Zotero collection hierarchy under the `Sources/` folder
- Pure Markdown output (no HTML) — clean, theme-friendly, and future-proof
- Deep Zotero links on every annotation and paper title
- Smart incremental sync using a snapshot system

## Workflow

The script creates two main folders:
- **`Sources`** — all your literature notes
- **`To_Organize`** — temporary files and collected thoughts

You can create any additional folders you like (I use `Concepts` and one folder per dissertation chapter).

**How it works:**

- **Green highlight** → Citation you plan to use. Add a headline and `[[concept]]` / `[[Chapter X]]` tags in the comment (supports `[[concept x, chapter y]]`). Appears in Sources, Concepts, and Chapters.
- **Purple highlight** → Your own thoughts. Optionally start comment with `[Headline]`. Appears as `> [!reading]` callout **and** gets collected in `To_Organize/Thoughts and Directions.md`.
- **Sticky Note** → Thoughts not tied to a specific line. Can use `[Headline]` and will become `> [!note]`.
- **Grey highlight** → Mark as “done” once used in your draft. Turns into a clean `> [!check]` callout.
- Any other color is ignored by the script (if you want to use them in reading only).

### Additional Features
- Direct deep links back to Zotero from every callout
- New concepts/chapters auto-create in `To_Organize/` and stay where you move them
- `To_Organize\To_Read.md` automatically lists papers with no annotations yet
- All your manual notes and links between zotero blocks are safely preserved

## Automatic Cleanup & Safety Features

- **Full manual edit protection**: Content outside the markers is never overwritten
- **Smart cleanup**: Removes source files for papers removed from your PHD collection (unless they contain manual notes)
- **Stale file management**: Cleans up old files in `To_Organize/` when the real note exists elsewhere
- **Color-change awareness**: Automatically removes annotations whose color changed to untracked colors
- **Oversize protection**: Hard limits (200 KB per source file, 500 KB per concept file) to prevent performance problems
- **Safe by design**: Read-only Zotero access, atomic writes, lock file, and `--dry-run` flag

## Installation & Setup

1. **Recommended**: Create a dedicated Obsidian vault for this integration.

2. Open the script file **`Z_O_Integration.py`** and edit the configuration at the top:

```python
DEFAULT_SOURCES_DIR  = "/path/to/your/vault/Sources"           # ← Change this
DEFAULT_VAULT_DIR    = "/path/to/your/vault"                  # ← Change this
PHD_COLLECTION       = "PHD Dissertation"                     # ← Change to your collection name
DEFAULT_ZOTERO_DB    = str(Path.home() / "Zotero" / "zotero.sqlite")  # Usually works automatically 
```
3. Save the file.

4. Run the sync:

### First time — preview what will happen
```python
python3 Z_O_Integration_User.py --dry-run
```
### Normal sync
```python
python3 Z_O_Integration_User.py
```
Tip: Create a desktop shortcut, Raycast/Alfred command, or set up auto-sync every few minutes while reading so everything appears in Obsidian almost instantly after you finish annotating.
Here's a suggested additions section to append to the README:

---

## Recent Improvements (04.26)

### Annotation rendering
- **Grey annotations** now show two separate page references: the Zotero source page in the standard meta line, and your manually added `(p. X)` citation reference (where you used it in your own writing) at the bottom of the collapsible callout.


### Edited volumes and book support
Two workflows are supported for books annotated as a single Zotero item:

**Case A — Separate Zotero items per chapter** (recommended): Each chapter is its own `bookSection` entry in Zotero with its own title. The script automatically shows "In: *BookTitle*, pp. X–Y" context in the source note, labels editors correctly, and nests the file under `Sources/<Collection>/<BookTitle>/`.

**Case B — Single Zotero item for the whole book**: The script partitions your annotations into per-chapter virtual files automatically. Two ways to define chapter boundaries:
- **Explicit**: Add a sticky note with `[paper: Your Chapter Title Here]` at the first annotation of each chapter — this becomes both the file title and the chapter boundary marker
- **Automatic**: If no markers are present, the script detects chapter boundaries by page gaps and names files `pp. X–Y`
In both cases, chapter files are nested under `Sources/<Collection>/<BookTitle>/` and editors are displayed with the correct `(eds.)` label.



### Link resolution
- **Fuzzy matching**: A single-character typo in a `[[link]]` (wrong letter, missing space, transposed characters) automatically resolves to the correct file — as long as only one vault note is a close match. Digit differences are excluded so `[[chap. 1]]` never fuzzy-matches to `[[chap. 2]]`
- **Links anywhere in file**: `[[links]]` written before the first annotation block (e.g. in the YAML header or title area) are now captured and routed correctly, not silently ignored

#### +General Improvments in Performance, Safety and robustness
