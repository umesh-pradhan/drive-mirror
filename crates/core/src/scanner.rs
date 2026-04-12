use crate::models::{CompareMode, DiffEntry, DiffStatus, FileMeta, LastEntry, Side, WorkerEvent};
use anyhow::{Context, Result};
use blake3::Hasher;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

pub fn scan_worker(
    left: PathBuf,
    right: PathBuf,
    compare: CompareMode,
    tx: Sender<WorkerEvent>,
    exclude: GlobSet,
    last: HashMap<PathBuf, LastEntry>,
) -> Result<()> {
    let mut errors = Vec::new();
    let left_map = scan_dir(&left, compare, Side::Left, &tx, &mut errors, &exclude, &last);
    let right_map = scan_dir(&right, compare, Side::Right, &tx, &mut errors, &exclude, &last);
    tx.send(WorkerEvent::ScanDone { left: left_map, right: right_map, errors })?;
    Ok(())
}

pub fn scan_dir(
    root: &Path,
    compare: CompareMode,
    side: Side,
    tx: &Sender<WorkerEvent>,
    errors: &mut Vec<String>,
    exclude: &GlobSet,
    last: &HashMap<PathBuf, LastEntry>,
) -> BTreeMap<PathBuf, FileMeta> {
    let mut map = BTreeMap::new();
    let mut count = 0usize;
    for entry in WalkDir::new(root).follow_links(false) {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if entry.file_type().is_file() || entry.file_type().is_symlink() {
                    let rel = match path.strip_prefix(root) {
                        Ok(rel) => rel.to_path_buf(),
                        Err(_) => continue,
                    };
                    if should_exclude(&rel, exclude) { continue; }
                    match build_file_meta(path, compare, side, last.get(&rel)) {
                        Ok(meta) => {
                            map.insert(rel, meta);
                            count += 1;
                            let _ = tx.send(WorkerEvent::ScanProgress { side, count });
                        }
                        Err(err) => errors.push(format!("{}: {}", path.display(), err)),
                    }
                }
            }
            Err(err) => errors.push(err.to_string()),
        }
    }
    map
}

pub fn build_file_meta(path: &Path, compare: CompareMode, side: Side, last: Option<&LastEntry>) -> Result<FileMeta> {
    let metadata = std::fs::symlink_metadata(path).with_context(|| format!("metadata for {}", path.display()))?;
    let is_symlink = metadata.file_type().is_symlink();
    let link_target = if is_symlink {
        std::fs::read_link(path).ok().map(|p| p.to_string_lossy().to_string())
    } else {
        None
    };
    let size = if is_symlink { link_target.as_ref().map(|t| t.len() as u64).unwrap_or(0) } else { metadata.len() };
    let mtime = metadata.modified().ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let hash = match compare {
        CompareMode::Hash => {
            if is_symlink {
                Some(link_target.clone().unwrap_or_default())
            } else {
                if let Some(last_entry) = last {
                    let (last_size, last_mtime, last_hash) = match side {
                        Side::Left => (last_entry.size_left, last_entry.mtime_left, last_entry.hash_left.clone()),
                        Side::Right => (last_entry.size_right, last_entry.mtime_right, last_entry.hash_right.clone()),
                    };
                    if last_size == Some(size) && last_mtime == Some(mtime) {
                        if let Some(hash) = last_hash {
                            return Ok(FileMeta { size, mtime, hash: Some(hash), is_symlink, link_target });
                        }
                    }
                }
                Some(hash_file(path)?)
            }
        }
        CompareMode::Size => None,
    };
    Ok(FileMeta { size, mtime, hash, is_symlink, link_target })
}

pub fn hash_file(path: &Path) -> Result<String> {
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut buf = vec![0u8; 1024 * 1024];
    let mut hasher = Hasher::new();
    loop {
        let read = reader.read(&mut buf).with_context(|| format!("read {}", path.display()))?;
        if read == 0 { break; }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn hash_file_progress(path: &Path, tx: &Sender<WorkerEvent>) -> Result<String> {
    let total = std::fs::metadata(path).with_context(|| format!("metadata {}", path.display()))?.len();
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut buf = vec![0u8; 1024 * 1024];
    let mut hasher = Hasher::new();
    let mut done = 0u64;
    tx.send(WorkerEvent::VerifyProgress { done, total }).ok();
    loop {
        let read = reader.read(&mut buf).with_context(|| format!("read {}", path.display()))?;
        if read == 0 { break; }
        hasher.update(&buf[..read]);
        done += read as u64;
        tx.send(WorkerEvent::VerifyProgress { done, total }).ok();
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn compute_diffs(
    left: &BTreeMap<PathBuf, FileMeta>,
    right: &BTreeMap<PathBuf, FileMeta>,
    compare: CompareMode,
    last: &HashMap<PathBuf, LastEntry>,
) -> Vec<DiffEntry> {
    let mut paths = BTreeSet::new();
    paths.extend(left.keys().cloned());
    paths.extend(right.keys().cloned());
    let mut diffs = Vec::new();
    for path in paths {
        let left_meta = left.get(&path).cloned();
        let right_meta = right.get(&path).cloned();
        let mut status = match (&left_meta, &right_meta) {
            (None, Some(_)) => DiffStatus::MissingLeft,
            (Some(_), None) => DiffStatus::MissingRight,
            (Some(l), Some(r)) => {
                if l.is_symlink != r.is_symlink {
                    DiffStatus::Mismatch
                } else if l.is_symlink && r.is_symlink {
                    if l.link_target != r.link_target { DiffStatus::Mismatch } else { DiffStatus::Same }
                } else if l.size != r.size {
                    DiffStatus::Mismatch
                } else if compare == CompareMode::Hash {
                    if l.hash != r.hash { DiffStatus::Mismatch } else { DiffStatus::Same }
                } else {
                    DiffStatus::Same
                }
            }
            (None, None) => DiffStatus::Same,
        };
        if status == DiffStatus::Mismatch {
            if let Some(last_entry) = last.get(&path) {
                let left_changed = last_entry.mtime_left.map(|t| left_meta.as_ref().map(|m| m.mtime > t).unwrap_or(false)).unwrap_or(false);
                let right_changed = last_entry.mtime_right.map(|t| right_meta.as_ref().map(|m| m.mtime > t).unwrap_or(false)).unwrap_or(false);
                if left_changed && right_changed { status = DiffStatus::Conflict; }
            }
        }
        if status != DiffStatus::Same {
            diffs.push(DiffEntry { path_rel: path, left: left_meta, right: right_meta, status });
        }
    }
    diffs
}

pub fn build_exclude_set(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

pub fn should_exclude(rel: &Path, exclude: &GlobSet) -> bool {
    exclude.is_match(rel)
}

pub fn space_info(path: &Path) -> (u64, u64) {
    let total = fs2::total_space(path).unwrap_or(0);
    let free = fs2::available_space(path).unwrap_or(0);
    (free, total)
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB { format!("{:.2} GB", b / GB) }
    else if b >= MB { format!("{:.2} MB", b / MB) }
    else if b >= KB { format!("{:.2} KB", b / KB) }
    else { format!("{} B", bytes) }
}

pub fn format_bytes_per_sec(bps: f64) -> String {
    if bps <= 0.0 { return "0 B".to_string(); }
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    if bps >= GB { format!("{:.2} GB", bps / GB) }
    else if bps >= MB { format!("{:.2} MB", bps / MB) }
    else if bps >= KB { format!("{:.2} KB", bps / KB) }
    else { format!("{:.0} B", bps) }
}

pub fn format_mtime(mtime: i64) -> String {
    match chrono::DateTime::<chrono::Utc>::from_timestamp(mtime, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => mtime.to_string(),
    }
}

pub fn progress_bar(current: u64, total: u64, width: usize) -> String {
    if total == 0 || width == 0 { return "[----------]".to_string(); }
    let ratio = (current as f64 / total as f64).min(1.0);
    let filled = (ratio * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "#".repeat(filled), "-".repeat(empty))
}

pub fn eta_seconds(done: u64, total: u64, speed_bps: f64) -> u64 {
    if speed_bps <= 0.0 || total == 0 || done >= total { return 0; }
    ((total - done) as f64 / speed_bps).ceil() as u64
}

pub fn format_eta(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 { format!("{}h {:02}m {:02}s", h, m, s) }
    else if m > 0 { format!("{}m {:02}s", m, s) }
    else { format!("{}s", s) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn diff_detects_missing_and_mismatch() {
        let mut left = BTreeMap::new();
        let mut right = BTreeMap::new();
        left.insert(PathBuf::from("a.txt"), FileMeta { size: 10, mtime: 1, hash: None, is_symlink: false, link_target: None });
        right.insert(PathBuf::from("b.txt"), FileMeta { size: 10, mtime: 1, hash: None, is_symlink: false, link_target: None });
        left.insert(PathBuf::from("c.txt"), FileMeta { size: 10, mtime: 1, hash: None, is_symlink: false, link_target: None });
        right.insert(PathBuf::from("c.txt"), FileMeta { size: 12, mtime: 1, hash: None, is_symlink: false, link_target: None });
        let diffs = compute_diffs(&left, &right, CompareMode::Size, &HashMap::new());
        assert!(diffs.iter().any(|d| d.status == DiffStatus::MissingLeft));
        assert!(diffs.iter().any(|d| d.status == DiffStatus::MissingRight));
        assert!(diffs.iter().any(|d| d.status == DiffStatus::Mismatch));
    }
}
