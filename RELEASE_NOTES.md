## What's New in v0.1.4

### Command Palette
Press `/` in the Review screen to open a fuzzy command palette. Type to filter and `Enter` to execute:
- `sync`, `sync-all`, `delete`, `missing-left`, `missing-right`, `mismatch`, `conflict`, `all`, `history`, `refresh`, `quit`

### Delete Confirmation
Pressing `d` now shows a confirmation step before deleting anything. `y`/`Enter` to confirm, `n`/`b`/`Esc` to cancel.

### Active Filter Badge
The header now always shows the active filter, e.g. `[ Filter: Missing Left ]`.

### Improved Done Summary
The summary screen now shows action type per file: `[copy L→R]`, `[copy R→L]`, `[deleted L]`, `[deleted R]`.

### Bug Fixes
- Delete override now correctly executes for files previously in `copied_recently`
- Override priority fixed: explicit overrides always win over skip logic

### Internal
- Restructured into a Cargo workspace (`core` / `tui` / `cli`) for better testability and extensibility
