use crate::models::{ActionResult, ActionType, DiffEntry, DiffStatus, HistoryEntry, LastEntry};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

pub fn init_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            left_root TEXT NOT NULL,
            right_root TEXT NOT NULL,
            status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS diffs (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            path_rel TEXT NOT NULL,
            status TEXT NOT NULL,
            size_left INTEGER,
            size_right INTEGER,
            mtime_left INTEGER,
            mtime_right INTEGER,
            hash_left TEXT,
            hash_right TEXT
        );
        CREATE TABLE IF NOT EXISTS actions (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            path_rel TEXT NOT NULL,
            action_type TEXT NOT NULL,
            reason TEXT NOT NULL,
            outcome TEXT NOT NULL,
            error TEXT,
            src_path TEXT,
            dst_path TEXT,
            bytes INTEGER,
            duration_ms INTEGER,
            verified INTEGER
        );
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            message TEXT NOT NULL
        );
        ",
    )?;
    ensure_action_columns(&conn)?;
    Ok(conn)
}

fn ensure_action_columns(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(actions)")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = BTreeSet::new();
    for row in rows {
        columns.insert(row?);
    }
    add_column_if_missing(conn, &columns, "src_path", "TEXT")?;
    add_column_if_missing(conn, &columns, "dst_path", "TEXT")?;
    add_column_if_missing(conn, &columns, "bytes", "INTEGER")?;
    add_column_if_missing(conn, &columns, "duration_ms", "INTEGER")?;
    add_column_if_missing(conn, &columns, "verified", "INTEGER")?;
    Ok(())
}

fn add_column_if_missing(conn: &Connection, columns: &BTreeSet<String>, name: &str, col_type: &str) -> Result<()> {
    if columns.contains(name) {
        return Ok(());
    }
    conn.execute(&format!("ALTER TABLE actions ADD COLUMN {} {}", name, col_type), [])?;
    Ok(())
}

pub fn insert_run_start(conn: &Connection, left: &Path, right: &Path) -> Result<i64> {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO runs (started_at, left_root, right_root, status) VALUES (?1, ?2, ?3, ?4)",
        params![now, left.display().to_string(), right.display().to_string(), "running"],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn finalize_run(conn: &Connection, run_id: i64, status: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE runs SET completed_at = ?1, status = ?2 WHERE id = ?3",
        params![now, status, run_id],
    )?;
    Ok(())
}

pub fn insert_error(conn: &Connection, run_id: i64, message: &str) -> Result<()> {
    conn.execute("INSERT INTO errors (run_id, message) VALUES (?1, ?2)", params![run_id, message])?;
    Ok(())
}

pub fn insert_diffs(conn: &Connection, run_id: i64, diffs: &[DiffEntry]) -> Result<()> {
    for diff in diffs {
        let status = match diff.status {
            DiffStatus::Same => "same",
            DiffStatus::MissingLeft => "missing-left",
            DiffStatus::MissingRight => "missing-right",
            DiffStatus::Mismatch => "mismatch",
            DiffStatus::Conflict => "conflict",
        };
        let (size_left, mtime_left, hash_left) = diff.left.as_ref()
            .map(|m| (Some(m.size), Some(m.mtime), m.hash.clone()))
            .unwrap_or((None, None, None));
        let (size_right, mtime_right, hash_right) = diff.right.as_ref()
            .map(|m| (Some(m.size), Some(m.mtime), m.hash.clone()))
            .unwrap_or((None, None, None));
        conn.execute(
            "INSERT INTO diffs (run_id, path_rel, status, size_left, size_right, mtime_left, mtime_right, hash_left, hash_right) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![run_id, diff.path_rel.display().to_string(), status, size_left, size_right, mtime_left, mtime_right, hash_left, hash_right],
        )?;
    }
    Ok(())
}

pub fn insert_action_result(conn: &Connection, run_id: i64, result: &ActionResult) -> Result<()> {
    let action_type = match result.action.action_type {
        ActionType::CopyLeftToRight => "copy-left-to-right",
        ActionType::CopyRightToLeft => "copy-right-to-left",
        ActionType::DeleteLeft => "delete-left",
        ActionType::DeleteRight => "delete-right",
    };
    conn.execute(
        "INSERT INTO actions (run_id, path_rel, action_type, reason, outcome, error, src_path, dst_path, bytes, duration_ms, verified) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![run_id, result.action.path_rel.display().to_string(), action_type, result.action.reason, result.outcome, result.error, result.src.display().to_string(), result.dst.display().to_string(), result.bytes, result.duration_ms, if result.verified { 1 } else { 0 }],
    )?;
    Ok(())
}

pub fn load_last_run_diffs(conn: &Connection, left_root: &Path, right_root: &Path) -> Result<HashMap<PathBuf, LastEntry>> {
    let mut map = HashMap::new();
    let run_id: Option<i64> = conn.query_row(
        "SELECT id FROM runs WHERE left_root = ?1 AND right_root = ?2 ORDER BY id DESC LIMIT 1",
        params![left_root.display().to_string(), right_root.display().to_string()],
        |row| row.get(0),
    ).optional()?;
    let Some(run_id) = run_id else { return Ok(map); };
    let mut stmt = conn.prepare(
        "SELECT path_rel, size_left, size_right, mtime_left, mtime_right, hash_left, hash_right FROM diffs WHERE run_id = ?1",
    )?;
    let rows = stmt.query_map(params![run_id], |row| {
        Ok((row.get::<_, String>(0)?, LastEntry {
            size_left: row.get(1)?,
            size_right: row.get(2)?,
            mtime_left: row.get(3)?,
            mtime_right: row.get(4)?,
            hash_left: row.get(5)?,
            hash_right: row.get(6)?,
        }))
    })?;
    for row in rows {
        let (path, entry) = row?;
        map.insert(PathBuf::from(path), entry);
    }
    Ok(map)
}

pub fn load_history(conn: &Connection) -> Result<Vec<HistoryEntry>> {
    let mut stmt = conn.prepare(
        "SELECT id, started_at, completed_at, status, left_root, right_root FROM runs ORDER BY id DESC LIMIT 50",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(HistoryEntry {
            run_id: row.get(0)?,
            started_at: row.get(1)?,
            completed_at: row.get(2)?,
            status: row.get(3)?,
            left_root: row.get(4)?,
            right_root: row.get(5)?,
            actions: 0,
            errors: 0,
        })
    })?;
    let mut entries: Vec<HistoryEntry> = rows.collect::<rusqlite::Result<_>>()?;
    for entry in &mut entries {
        entry.actions = conn.query_row("SELECT COUNT(*) FROM actions WHERE run_id = ?1", params![entry.run_id], |row| row.get(0)).unwrap_or(0);
        entry.errors = conn.query_row("SELECT COUNT(*) FROM errors WHERE run_id = ?1", params![entry.run_id], |row| row.get(0)).unwrap_or(0);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_init_db_creates_tables() {
        let dir = tempdir().unwrap();
        let conn = init_db(&dir.path().join("test.db")).unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('runs','diffs','actions','errors')",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 4);
    }

    #[test]
    fn test_insert_and_load_run() {
        let dir = tempdir().unwrap();
        let conn = init_db(&dir.path().join("test.db")).unwrap();
        let left = Path::new("/left");
        let right = Path::new("/right");
        let run_id = insert_run_start(&conn, left, right).unwrap();
        assert!(run_id > 0);
        finalize_run(&conn, run_id, "done").unwrap();
        let history = load_history(&conn).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].status, "done");
    }
}
