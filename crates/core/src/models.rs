use clap::ValueEnum;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool},
    Arc,
};
use std::time::Instant;

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum CompareMode {
    Size,
    Hash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Scanning,
    Review,
    ChoosingStrategy,
    ConfirmSync,
    ConfirmDelete,
    History,
    Syncing,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug)]
pub enum MismatchStrategy {
    NewerMtime,
    PreferLeft,
    PreferRight,
    Skip,
}

#[derive(Clone, Debug)]
pub struct FileMeta {
    pub size: u64,
    pub mtime: i64,
    pub hash: Option<String>,
    pub is_symlink: bool,
    pub link_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffStatus {
    Same,
    MissingLeft,
    MissingRight,
    Mismatch,
    Conflict,
}

#[derive(Clone, Debug)]
pub struct DiffEntry {
    pub path_rel: PathBuf,
    pub left: Option<FileMeta>,
    pub right: Option<FileMeta>,
    pub status: DiffStatus,
}

#[derive(Clone, Debug)]
pub struct LastEntry {
    pub size_left: Option<u64>,
    pub size_right: Option<u64>,
    pub mtime_left: Option<i64>,
    pub mtime_right: Option<i64>,
    pub hash_left: Option<String>,
    pub hash_right: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionType {
    CopyLeftToRight,
    CopyRightToLeft,
    DeleteLeft,
    DeleteRight,
}

#[derive(Clone, Debug)]
pub struct Action {
    pub path_rel: PathBuf,
    pub action_type: ActionType,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct ActionResult {
    pub action: Action,
    pub outcome: String,
    pub error: Option<String>,
    pub src: PathBuf,
    pub dst: PathBuf,
    pub bytes: u64,
    pub duration_ms: i64,
    pub verified: bool,
}

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub run_id: i64,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub status: String,
    pub left_root: String,
    pub right_root: String,
    pub actions: i64,
    pub errors: i64,
}

pub struct CopyOutcome {
    pub bytes: u64,
    pub verified: bool,
}

#[derive(Debug)]
pub enum WorkerEvent {
    ScanProgress { side: Side, count: usize },
    ScanDone { left: BTreeMap<PathBuf, FileMeta>, right: BTreeMap<PathBuf, FileMeta>, errors: Vec<String> },
    SyncProgress { completed: usize, total: usize, bytes: u64 },
    SyncFileProgress { src: PathBuf, dst: PathBuf, copied: u64, total: u64 },
    Verifying,
    VerifyProgress { done: u64, total: u64 },
    SyncDone { results: Vec<ActionResult> },
    Error(String),
}

#[derive(Clone, Copy, Debug)]
pub enum SyncScope {
    All,
    Selected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Filter {
    All,
    MissingLeft,
    MissingRight,
    Mismatch,
    Conflict,
}

pub struct AppState {
    pub phase: Phase,
    pub scanned_left: usize,
    pub scanned_right: usize,
    pub diffs: Vec<DiffEntry>,
    pub selected: usize,
    pub selected_items: BTreeSet<usize>,
    pub action_overrides: HashMap<PathBuf, ActionType>,
    pub history: Vec<HistoryEntry>,
    pub history_selected: usize,
    pub copied_recently: BTreeSet<PathBuf>,
    pub force_recopy: BTreeSet<PathBuf>,
    pub filter: Filter,
    pub filtered_indices: Vec<usize>,
    pub status_line: String,
    pub mismatch_strategy: Option<MismatchStrategy>,
    pub pending_actions: Vec<Action>,
    pub sync_scope: SyncScope,
    pub sort_by_name: bool,
    pub sync_completed: usize,
    pub sync_total: usize,
    pub sync_bytes: u64,
    pub sync_start: Option<Instant>,
    pub sync_speed_bps: f64,
    pub current_src: Option<PathBuf>,
    pub current_dst: Option<PathBuf>,
    pub current_copied: u64,
    pub current_total: u64,
    pub current_start: Option<Instant>,
    pub current_speed_bps: f64,
    pub last_copied_dst: Option<PathBuf>,
    pub last_esc: Option<Instant>,
    pub last_results: Vec<ActionResult>,
    pub verifying: bool,
    pub verify_done: u64,
    pub verify_total: u64,
    pub verify_start: Option<Instant>,
    pub verify_speed_bps: f64,
    pub dirty: bool,
    pub last_draw: Instant,
    pub cancel_after_current: Arc<AtomicBool>,
    // palette
    pub palette_open: bool,
    pub palette_query: String,
    pub palette_selected: usize,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            phase: Phase::Scanning,
            scanned_left: 0,
            scanned_right: 0,
            diffs: Vec::new(),
            selected: 0,
            selected_items: BTreeSet::new(),
            action_overrides: HashMap::new(),
            history: Vec::new(),
            history_selected: 0,
            copied_recently: BTreeSet::new(),
            force_recopy: BTreeSet::new(),
            filter: Filter::All,
            filtered_indices: Vec::new(),
            status_line: String::new(),
            mismatch_strategy: None,
            pending_actions: Vec::new(),
            sync_scope: SyncScope::All,
            sort_by_name: true,
            sync_completed: 0,
            sync_total: 0,
            sync_bytes: 0,
            sync_start: None,
            sync_speed_bps: 0.0,
            current_src: None,
            current_dst: None,
            current_copied: 0,
            current_total: 0,
            current_start: None,
            current_speed_bps: 0.0,
            last_copied_dst: None,
            last_esc: None,
            last_results: Vec::new(),
            verifying: false,
            verify_done: 0,
            verify_total: 0,
            verify_start: None,
            verify_speed_bps: 0.0,
            dirty: true,
            last_draw: Instant::now(),
            cancel_after_current: Arc::new(AtomicBool::new(false)),
            palette_open: false,
            palette_query: String::new(),
            palette_selected: 0,
        }
    }

    pub fn close_palette(&mut self) {
        self.palette_open = false;
        self.palette_query.clear();
        self.palette_selected = 0;
    }
}

pub fn recompute_filtered_indices(state: &mut AppState) {
    state.filtered_indices.clear();
    for (i, d) in state.diffs.iter().enumerate() {
        let pass = match state.filter {
            Filter::All => true,
            Filter::MissingLeft => d.status == DiffStatus::MissingLeft,
            Filter::MissingRight => d.status == DiffStatus::MissingRight,
            Filter::Mismatch => d.status == DiffStatus::Mismatch,
            Filter::Conflict => d.status == DiffStatus::Conflict,
        };
        if pass { state.filtered_indices.push(i); }
    }
    state.selected = 0;
}
