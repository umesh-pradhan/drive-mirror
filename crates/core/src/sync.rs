use crate::models::{Action, ActionResult, ActionType, CompareMode, CopyOutcome, WorkerEvent};
use crate::scanner::{format_bytes, hash_file_progress};
use anyhow::{Context, Result};
use filetime::FileTime;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

pub fn sync_worker(
    left: PathBuf,
    right: PathBuf,
    actions: Vec<Action>,
    compare: CompareMode,
    retries: u32,
    dry_run: bool,
    cancel_after_current: Arc<AtomicBool>,
    tx: Sender<WorkerEvent>,
) -> Result<()> {
    let total = actions.len();
    let mut completed = 0usize;
    let mut bytes = 0u64;
    let mut results = Vec::new();
    for action in actions {
        let start = Instant::now();
        let (src, dst, result) = match action.action_type {
            ActionType::CopyLeftToRight => {
                let src = left.join(&action.path_rel);
                let dst = right.join(&action.path_rel);
                let r = copy_and_verify(&src, &dst, compare, retries, dry_run, &tx);
                (src, dst, r)
            }
            ActionType::CopyRightToLeft => {
                let src = right.join(&action.path_rel);
                let dst = left.join(&action.path_rel);
                let r = copy_and_verify(&src, &dst, compare, retries, dry_run, &tx);
                (src, dst, r)
            }
            ActionType::DeleteLeft => {
                let target = left.join(&action.path_rel);
                let r = delete_with_retry(&target, retries, dry_run, &tx);
                (target.clone(), target, r)
            }
            ActionType::DeleteRight => {
                let target = right.join(&action.path_rel);
                let r = delete_with_retry(&target, retries, dry_run, &tx);
                (target.clone(), target, r)
            }
        };
        let duration_ms = start.elapsed().as_millis() as i64;
        let (outcome, error, bytes_copied, verified) = match result {
            Ok(outcome) => {
                bytes += outcome.bytes;
                (if dry_run { "dry-run".to_string() } else { "ok".to_string() }, None, outcome.bytes, outcome.verified)
            }
            Err(err) => (err.to_string(), Some(err.to_string()), 0, false),
        };
        results.push(ActionResult { action: action.clone(), outcome, error, src, dst, bytes: bytes_copied, duration_ms, verified });
        completed += 1;
        let _ = tx.send(WorkerEvent::SyncProgress { completed, total, bytes });
        if cancel_after_current.load(Ordering::Relaxed) { break; }
    }
    tx.send(WorkerEvent::SyncDone { results })?;
    Ok(())
}

pub fn copy_and_verify(src: &Path, dst: &Path, compare: CompareMode, retries: u32, dry_run: bool, tx: &Sender<WorkerEvent>) -> Result<CopyOutcome> {
    let mut last_err = None;
    for attempt in 0..=retries {
        match copy_once(src, dst, compare, dry_run, tx) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                last_err = Some(err);
                if attempt < retries { thread::sleep(Duration::from_millis(200)); }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("copy failed")))
}

fn copy_once(src: &Path, dst: &Path, _compare: CompareMode, dry_run: bool, tx: &Sender<WorkerEvent>) -> Result<CopyOutcome> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let metadata = std::fs::symlink_metadata(src).with_context(|| format!("metadata {}", src.display()))?;
    let is_symlink = metadata.file_type().is_symlink();
    let size = if is_symlink { 0 } else { metadata.len() };
    if dry_run {
        tx.send(WorkerEvent::SyncFileProgress { src: src.to_path_buf(), dst: dst.to_path_buf(), copied: size, total: size }).ok();
        return Ok(CopyOutcome { bytes: size, verified: false });
    }
    if is_symlink {
        copy_symlink(src, dst)?;
        return Ok(CopyOutcome { bytes: 0, verified: true });
    }
    let dst_dir = dst.parent().unwrap_or(dst);
    let free = fs2::available_space(dst_dir).unwrap_or(0);
    if size > free {
        return Err(anyhow::anyhow!("not enough space at {}: need {}, have {}", dst_dir.display(), format_bytes(size), format_bytes(free)));
    }
    let src_hash = copy_with_progress(src, dst, size, tx)?;
    let dst_size = std::fs::metadata(dst).with_context(|| format!("metadata {}", dst.display()))?.len();
    if size != dst_size { return Err(anyhow::anyhow!("size mismatch after copy")); }
    tx.send(WorkerEvent::Verifying).ok();
    let dst_hash = hash_file_progress(dst, tx)?;
    if src_hash != dst_hash { return Err(anyhow::anyhow!("hash mismatch after copy")); }
    std::fs::set_permissions(dst, metadata.permissions()).with_context(|| format!("permissions {}", dst.display()))?;
    if let Ok(mtime) = metadata.modified() {
        filetime::set_file_mtime(dst, FileTime::from_system_time(mtime)).with_context(|| format!("mtime {}", dst.display()))?;
    }
    Ok(CopyOutcome { bytes: size, verified: true })
}

fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
    let target = std::fs::read_link(src).with_context(|| format!("read link {}", src.display()))?;
    #[cfg(unix)]
    { std::os::unix::fs::symlink(&target, dst).with_context(|| format!("symlink {}", dst.display()))?; return Ok(()); }
    #[cfg(windows)]
    {
        if target.is_dir() { std::os::windows::fs::symlink_dir(&target, dst).with_context(|| format!("symlink {}", dst.display()))?; }
        else { std::os::windows::fs::symlink_file(&target, dst).with_context(|| format!("symlink {}", dst.display()))?; }
        return Ok(());
    }
    #[allow(unreachable_code)]
    Ok(())
}

pub fn delete_with_retry(path: &Path, retries: u32, dry_run: bool, tx: &Sender<WorkerEvent>) -> Result<CopyOutcome> {
    let mut last_err = None;
    for attempt in 0..=retries {
        match delete_once(path, dry_run, tx) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                last_err = Some(err);
                if attempt < retries { thread::sleep(Duration::from_millis(200)); }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("delete failed")))
}

fn delete_once(path: &Path, dry_run: bool, tx: &Sender<WorkerEvent>) -> Result<CopyOutcome> {
    let metadata = std::fs::symlink_metadata(path).with_context(|| format!("metadata {}", path.display()))?;
    let size = metadata.len();
    tx.send(WorkerEvent::SyncFileProgress { src: path.to_path_buf(), dst: path.to_path_buf(), copied: size, total: size }).ok();
    if dry_run { return Ok(CopyOutcome { bytes: size, verified: false }); }
    if metadata.is_dir() { std::fs::remove_dir_all(path).with_context(|| format!("remove dir {}", path.display()))?; }
    else { std::fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?; }
    Ok(CopyOutcome { bytes: size, verified: true })
}

fn copy_with_progress(src: &Path, dst: &Path, total: u64, tx: &Sender<WorkerEvent>) -> Result<String> {
    let mut reader = std::io::BufReader::new(std::fs::File::open(src).with_context(|| format!("open {}", src.display()))?);
    let mut writer = std::io::BufWriter::new(std::fs::File::create(dst).with_context(|| format!("create {}", dst.display()))?);
    let mut buf = vec![0u8; 1024 * 1024];
    let mut copied = 0u64;
    let mut hasher = blake3::Hasher::new();
    tx.send(WorkerEvent::SyncFileProgress { src: src.to_path_buf(), dst: dst.to_path_buf(), copied, total }).ok();
    loop {
        let read = reader.read(&mut buf).with_context(|| format!("read {}", src.display()))?;
        if read == 0 { break; }
        writer.write_all(&buf[..read]).with_context(|| format!("write {}", dst.display()))?;
        hasher.update(&buf[..read]);
        copied += read as u64;
        tx.send(WorkerEvent::SyncFileProgress { src: src.to_path_buf(), dst: dst.to_path_buf(), copied, total }).ok();
    }
    writer.flush().with_context(|| format!("flush {}", dst.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CompareMode;
    use std::sync::mpsc;
    use tempfile::tempdir;

    #[test]
    fn test_sync_missing_right_copies_file() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, b"hello world").unwrap();
        let (tx, _rx) = mpsc::channel();
        let result = copy_and_verify(&src, &dst, CompareMode::Size, 0, false, &tx).unwrap();
        assert_eq!(result.bytes, 11);
        assert!(dst.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"hello world");
    }
}
