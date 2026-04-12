# drive-mirror

[![Crates.io](https://img.shields.io/crates/v/drive-mirror.svg)](https://crates.io/crates/drive-mirror)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**drive-mirror** is a high-performance terminal-based (TUI) utility written in Rust, designed for efficient **drive mirroring**, comparison, and synchronization between two directories (e.g., local drive and backup/external drive).

## Features

- **Interactive TUI**: A terminal interface built with `ratatui` for intuitive **drive mirroring** and sync operations.
- **Command Palette**: Press `/` to open a fuzzy command palette — type `sync`, `delete`, `missing-left`, `history`, etc. to execute actions instantly.
- **Fast Comparison**: Efficiently compare directories based on **Size** (fast) or **BLAKE3 Hash** (accurate).
- **Drive Mirroring Strategies**:
  - `NewerMtime`: Prefer files with more recent modification times.
  - `PreferLeft`: Overwrite right with left (Standard Mirror).
  - `PreferRight`: Overwrite left with right.
  - `Skip`: Do nothing for mismatches.
- **Delete Confirmation**: Select files with `Space`, press `d`, confirm before anything is deleted.
- **Active Filter Badge**: Header shows the active filter (e.g., `[ Filter: Missing Left ]`) at all times.
- **Completed Actions Summary**: Done screen shows `[copy L→R]`, `[deleted R]` etc. per file.
- **Exclusion Support**: Use glob patterns to exclude specific files or directories.
- **SQLite History**: Tracks all sync activity in an `activity.db` file.
- **Dry Run**: Preview changes before committing to disk.
- **Retry Logic**: Configurable retries for reliable file synchronization.

## Installation

### Binary Downloads

Download pre-compiled binaries for Linux, macOS, and Windows from the [Releases](https://github.com/umesh-pradhan/drive-mirror/releases) page.

#### macOS Security Note
Because `drive-mirror` is not yet signed with a paid Apple Developer certificate, macOS Gatekeeper may block it. To allow it:

```bash
xattr -d com.apple.quarantine drive-mirror
```

Or right-click → Open → Open in Finder.

### From Source

Ensure you have [Rust and Cargo](https://rustup.rs/) installed.

```bash
git clone https://github.com/umesh-pradhan/drive-mirror.git
cd drive-mirror
cargo build --release -p drive-mirror
```

The binary will be at `target/release/drive-mirror`.

## Usage

```bash
./drive-mirror --left <PATH_LEFT> --right <PATH_RIGHT> [OPTIONS]
```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--left <PATH>` | Left directory | required |
| `--right <PATH>` | Right directory | required |
| `--db <PATH>` | SQLite database path | `activity.db` |
| `--compare <MODE>` | `size` or `hash` | `size` |
| `--exclude <PATTERNS>` | Comma-separated glob patterns | none |
| `--retries <N>` | Retries for file operations | `2` |
| `--dry-run` | Preview only, no changes | off |

### Examples

```bash
# Basic comparison by size
./drive-mirror --left /path/to/source --right /path/to/backup

# Hash comparison, excluding temp files
./drive-mirror --left ./src --right ./backup --compare hash --exclude "*.tmp,node_modules/*"
```

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `↑` / `↓` | Navigate list |
| `Space` | Toggle select item |
| `Enter` | Sync selected |
| `s` | Sync all (filtered) |
| `d` | Delete extras (with confirmation) |
| `/` | Open command palette |
| `l` / `r` | Override: copy L→R / R→L |
| `f` | Toggle force re-copy |
| `1`–`5` | Filter: all / missing-L / missing-R / mismatch / conflict |
| `n` | Toggle sort by name |
| `h` | History |
| `F5` | Refresh / rescan |
| `o` | Reveal last file in Finder/Explorer |
| `Esc Esc` | Quit |

### Command Palette (`/`)

Press `/` in the Review screen to open the palette. Type to fuzzy-filter:

```
> sy_
  sync       — Sync selected item
▶ sync-all   — Sync all filtered items
```

Available commands: `sync`, `sync-all`, `delete`, `missing-left`, `missing-right`, `mismatch`, `conflict`, `all`, `history`, `refresh`, `quit`.

## How it Works

1. **Scanning**: Walks both directories collecting file metadata (size, mtime, optionally BLAKE3 hash).
2. **Review**: Displays diffs — Missing Left, Missing Right, Mismatch, Conflict.
3. **Strategy**: Choose how to handle mismatches (newer, prefer-left, prefer-right, skip).
4. **Syncing**: Copies or deletes files with progress, verification, and retry.
5. **History**: View previous runs stored in SQLite.

## Project Structure

```
crates/
  core/   — Pure logic: models, db, scanner, planner, sync (no TUI dependency)
  tui/    — Ratatui UI: app loop, input handlers, renderer, command palette
  cli/    — Binary entry point
```

## Tech Stack

- **Rust** · **ratatui & crossterm** · **rusqlite** · **blake3** · **walkdir** · **clap**

## License

[MIT](LICENSE)
