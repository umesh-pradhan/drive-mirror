# Release Notes - v0.1.1 (Upcoming)

This release focus on improving documentation and project maintenance.

### 🚀 Improvements

- **macOS Experience:** Added detailed security notes and a quick-start guide for macOS users to handle Gatekeeper warnings.
- **Repository Maintenance:** Cleaned up the repository by removing unused files and ensuring IDE-specific files (like `.idea`) are properly ignored.

### 🛠️ Maintenance

- Updated `.gitignore` to prevent tracking of build artifacts and IDE configurations.
- Refined project structure by removing redundant files and directories.

---

# Release Notes - v0.1.0 (Initial Release)

High-performance TUI utility for directory synchronization.

### ✨ Features

- **TUI Interface:** Interactive terminal interface built with `ratatui` for easy scanning, review, and synchronization.
- **Comparison Modes:** Support for fast comparison by **Size** or accurate comparison by **BLAKE3 Hash**.
- **Sync Strategies:** Choose between `NewerMtime`, `PreferLeft`, `PreferRight`, and `Skip`.
- **Exclusion Support:** Define glob patterns to exclude specific files or directories.
- **Activity Logging:** Tracks synchronization history in a local SQLite database (`activity.db`).
- **Dry Run:** Safely preview changes before applying them to your files.
- **Retry Logic:** Automatic retries for failed file operations.

### 📦 Supported Platforms

- **Linux** (x86_64)
- **macOS** (Apple Silicon and Intel)
- **Windows** (x86_64)

### 🔗 Useful Links

- [GitHub Repository](https://github.com/umesh-pradhan/drive-mirror)
- [Installation Guide](https://github.com/umesh-pradhan/drive-mirror#installation)
- [Usage Examples](https://github.com/umesh-pradhan/drive-mirror#usage)
