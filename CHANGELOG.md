# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2026-04-12

### Added
- Command palette (`/` key) with fuzzy search for all actions (sync, delete, filter, history, etc.)
- Delete confirmation phase — `d` now prompts before executing any deletion
- Active filter badge in header showing current filter at all times
- Completed actions summary on Done screen with action type labels (`[copy L→R]`, `[deleted R]`, etc.)

### Fixed
- Delete override now correctly executes even for files in `copied_recently`
- Override priority: explicit overrides always win over `copied_recently` skip logic

### Changed
- Restructured into Cargo workspace with three crates: `core` (pure logic), `tui` (ratatui UI), `cli` (binary)
- Core logic has zero TUI dependency — fully unit testable in isolation

## [0.1.3] - 2026-04-04

### Fixed
- Replaced third-party `softprops/action-gh-release` with native GitHub CLI (`gh`) for creating releases. This avoids Node.js 20/24 runtime compatibility warnings and improves reliability.

## [0.1.2] - 2026-04-04

### Fixed
- Updated GitHub Actions to latest versions and enabled Node.js 24 for workflows to address deprecation warnings.

## [0.1.1] - 2026-04-04

### Added
- Detailed macOS security notes and quick-start guide in README.
- Better `.gitignore` management for IDE files.

### Fixed
- Cleaned up unused files and directories.

## [0.1.0] - 2026-04-03

### Added
- Initial release of `drive-mirror`.
- High-performance TUI for directory synchronization.
- Comparison by size and BLAKE3 hash.
- Support for `activity.db` SQLite history.
- Multi-platform support via GitHub Actions.
