# drive-mirror

[![Crates.io](https://img.shields.io/crates/v/drive-mirror.svg)](https://crates.io/crates/drive-mirror)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**drive-mirror** is a high-performance terminal-based (TUI) utility written in Rust, designed for efficient **drive mirroring**, comparison, and synchronization between two directories (e.g., local drive and backup/external drive).

If you are looking for a fast, reliable, and interactive way to **mirror your drive**, `drive-mirror` provides a robust TUI for scanning and reviewing changes before applying them.

## Features

- **Interactive TUI**: A terminal interface built with `ratatui` for intuitive **drive mirroring** and sync operations.
- **Fast Comparison**: Efficiently compare directories based on **Size** (fast) or **BLAKE3 Hash** (accurate).
- **Drive Mirroring Strategies**:
  - `NewerMtime`: Prefer files with more recent modification times.
  - `PreferLeft`: Overwrite right with left (Standard Mirror).
  - `PreferRight`: Overwrite left with right.
  - `Skip`: Do nothing for mismatches.
- **Exclusion Support**: Use glob patterns to exclude specific files or directories during the **mirror** process.
- **SQLite History**: Tracks all **drive mirroring** and sync activity in an `activity.db` file.
- **Dry Run**: Preview your **drive mirror** changes before committing to disk.
- **Retry Logic**: Configurable retries for reliable file synchronization.

## Installation

### Binary Downloads

You can download pre-compiled binaries for Linux, macOS, and Windows from the [Releases](https://github.com/umesh-pradhan/drive-mirror/releases) page.

#### macOS Security Note
Because `drive-mirror` is an open-source project and not yet signed with a paid Apple Developer certificate, macOS Gatekeeper may block it from running with a warning that "Apple could not verify it for malware."

To run it:
1.  Locate `drive-mirror` in **Finder**.
2.  **Right-click** (or Control-click) the application.
3.  Choose **Open** from the menu.
4.  Click **Open** again in the dialog box to confirm.

Once opened this way, it will run normally in the future.

**For Developers:** If you want to notarize your own builds, see the [macOS Notarization Guide](MACOS_NOTARIZATION.md).

### From Source

Ensure you have [Rust and Cargo](https://rustup.rs/) installed.

```bash
git clone https://github.com/umesh-pradhan/drive-mirror.git
cd drive-mirror
cargo build --release
```

The binary will be generated at `target/release/drive-mirror`.

You can also create a symbolic link in the root folder for easier access:
```bash
ln -s target/release/drive-mirror drive-mirror
```

### macOS Quick Start (Terminal)

If you downloaded the binary or built it yourself and see the "Apple could not verify..." warning, you can allow it using this command in your terminal:

```bash
xattr -d com.apple.quarantine drive-mirror
```

Once done, you can run the tool directly:
```bash
./drive-mirror --help
```

## Usage

To run the binary in your current directory, use the `./` prefix:

```bash
./drive-mirror --left <PATH_LEFT> --right <PATH_RIGHT> [OPTIONS]
```

### Options

- `--left <PATH>`: Path to the "left" directory.
- `--right <PATH>`: Path to the "right" directory.
- `--db <PATH>`: Path to the SQLite database for activity logging (default: `activity.db`).
- `--compare <MODE>`: Comparison mode: `size` or `hash` (default: `size`).
- `--exclude <PATTERNS>`: Comma-separated list of glob patterns to exclude.
- `--retries <N>`: Number of retries for file operations (default: 2).
- `--dry-run`: Enable dry run mode (no actual file changes).
- `-h, --help`: Print help information.
- `-V, --version`: Print version information.

### Examples

**Basic comparison by size:**
```bash
./drive-mirror --left /path/to/source --right /path/to/backup
```

**Accurate comparison using BLAKE3 hashes, excluding temporary files:**
```bash
./drive-mirror --left ./src --right ./backup --compare hash --exclude "*.tmp,node_modules/*"
```

## How it Works

1. **Scanning**: The tool walks through both directories, collecting file metadata (size, mtime, and optionally hashes).
2. **Review**: Displays a diff of the two directories (Missing Left, Missing Right, Mismatch).
3. **Strategy Selection**: Choose how to handle mismatches (e.g., sync newer, prefer left).
4. **Syncing**: Applies the selected strategy, copying or deleting files as necessary.
5. **History**: View previous synchronization actions stored in the database.

## Tech Stack

- **Rust**: Language.
- **ratatui & crossterm**: For the terminal UI.
- **rusqlite**: For activity logging.
- **blake3**: For fast and secure hashing.
- **walkdir**: For efficient directory traversal.
- **clap**: For command-line argument parsing.

## License

[MIT](LICENSE)
