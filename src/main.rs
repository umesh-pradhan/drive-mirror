use anyhow::{Context, Result};
use blake3::Hasher;
use clap::{Parser, ValueEnum};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use filetime::FileTime;
use globset::{Glob, GlobSet, GlobSetBuilder};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::Color;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    #[arg(long)]
    left: PathBuf,
    #[arg(long)]
    right: PathBuf,
    #[arg(long, default_value = "activity.db")]
    db: PathBuf,
    #[arg(long, value_enum, default_value = "size")]
    compare: CompareMode,
    #[arg(long, value_delimiter = ',')]
    exclude: Vec<String>,
    #[arg(long, default_value_t = 2)]
    retries: u32,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
enum CompareMode {
    Size,
    Hash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Scanning,
    Review,
    ChoosingStrategy,
    ConfirmSync,
    History,
    Syncing,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug)]
enum MismatchStrategy {
    NewerMtime,
    PreferLeft,
    PreferRight,
    Skip,
}

#[derive(Clone, Debug)]
struct FileMeta {
    size: u64,
    mtime: i64,
    hash: Option<String>,
    is_symlink: bool,
    link_target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DiffStatus {
    Same,
    MissingLeft,
    MissingRight,
    Mismatch,
    Conflict,
}

#[derive(Clone, Debug)]
struct DiffEntry {
    path_rel: PathBuf,
    left: Option<FileMeta>,
    right: Option<FileMeta>,
    status: DiffStatus,
}

#[derive(Clone, Debug)]
struct LastEntry {
    size_left: Option<u64>,
    size_right: Option<u64>,
    mtime_left: Option<i64>,
    mtime_right: Option<i64>,
    hash_left: Option<String>,
    hash_right: Option<String>,
}

#[derive(Clone, Copy, Debug)]
enum ActionType {
    CopyLeftToRight,
    CopyRightToLeft,
    DeleteLeft,
    DeleteRight,
}

#[derive(Clone, Debug)]
struct Action {
    path_rel: PathBuf,
    action_type: ActionType,
    reason: String,
}

#[derive(Clone, Debug)]
struct ActionResult {
    action: Action,
    outcome: String,
    error: Option<String>,
    src: PathBuf,
    dst: PathBuf,
    bytes: u64,
    duration_ms: i64,
    verified: bool,
}

#[derive(Clone, Debug)]
struct HistoryEntry {
    run_id: i64,
    started_at: String,
    completed_at: Option<String>,
    status: String,
    left_root: String,
    right_root: String,
    actions: i64,
    errors: i64,
}

struct CopyOutcome {
    bytes: u64,
    verified: bool,
}

#[derive(Debug)]
enum WorkerEvent {
    ScanProgress {
        side: Side,
        count: usize,
    },
    ScanDone {
        left: BTreeMap<PathBuf, FileMeta>,
        right: BTreeMap<PathBuf, FileMeta>,
        errors: Vec<String>,
    },
    SyncProgress {
        completed: usize,
        total: usize,
        bytes: u64,
    },
    SyncFileProgress {
        src: PathBuf,
        dst: PathBuf,
        copied: u64,
        total: u64,
    },
    Verifying,
    VerifyProgress {
        done: u64,
        total: u64,
    },
    SyncDone {
        results: Vec<ActionResult>,
    },
    Error(String),
}

struct AppState {
    phase: Phase,
    scanned_left: usize,
    scanned_right: usize,
    diffs: Vec<DiffEntry>,
    selected: ListState,
    selected_items: BTreeSet<usize>,
    action_overrides: HashMap<PathBuf, ActionType>,
    history: Vec<HistoryEntry>,
    history_selected: ListState,
    copied_recently: BTreeSet<PathBuf>,
    force_recopy: BTreeSet<PathBuf>,
    filter: Filter,
    filtered_indices: Vec<usize>,
    status_line: String,
    mismatch_strategy: Option<MismatchStrategy>,
    pending_actions: Vec<Action>,
    sync_scope: SyncScope,
    sort_by_name: bool,
    sync_completed: usize,
    sync_total: usize,
    sync_bytes: u64,
    sync_start: Option<Instant>,
    sync_speed_bps: f64,
    current_src: Option<PathBuf>,
    current_dst: Option<PathBuf>,
    current_copied: u64,
    current_total: u64,
    current_start: Option<Instant>,
    current_speed_bps: f64,
    last_copied_dst: Option<PathBuf>,
    last_esc: Option<Instant>,
    last_results: Vec<ActionResult>,
    verifying: bool,
    verify_done: u64,
    verify_total: u64,
    verify_start: Option<Instant>,
    verify_speed_bps: f64,
    dirty: bool,
    last_draw: Instant,
    cancel_after_current: Arc<AtomicBool>,
}

impl AppState {
    fn new() -> Self {
        let mut selected = ListState::default();
        selected.select(Some(0));
        Self {
            phase: Phase::Scanning,
            scanned_left: 0,
            scanned_right: 0,
            diffs: Vec::new(),
            selected,
            selected_items: BTreeSet::new(),
            action_overrides: HashMap::new(),
            history: Vec::new(),
            history_selected: ListState::default(),
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
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SyncScope {
    All,
    Selected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Filter {
    All,
    MissingLeft,
    MissingRight,
    Mismatch,
    Conflict,
}

fn recompute_filtered_indices(state: &mut AppState) {
    state.filtered_indices.clear();
    for (i, d) in state.diffs.iter().enumerate() {
        let pass = match state.filter {
            Filter::All => true,
            Filter::MissingLeft => d.status == DiffStatus::MissingLeft,
            Filter::MissingRight => d.status == DiffStatus::MissingRight,
            Filter::Mismatch => d.status == DiffStatus::Mismatch,
            Filter::Conflict => d.status == DiffStatus::Conflict,
        };
        if pass {
            state.filtered_indices.push(i);
        }
    }
    if state.filtered_indices.is_empty() {
        state.selected.select(None);
    } else {
        state.selected.select(Some(0));
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let conn = init_db(&args.db).context("init db")?;
    let run_id = insert_run_start(&conn, &args.left, &args.right)?;

    let (tx, rx) = mpsc::channel();
    let exclude_set = build_exclude_set(&args.exclude)?;
    let last_diffs = load_last_run_diffs(&conn, &args.left, &args.right)?;
    let compare_mode = args.compare;
    let left_root = args.left.clone();
    let right_root = args.right.clone();
    let scan_tx = tx.clone();
    let scan_exclude = exclude_set.clone();
    let scan_last = last_diffs.clone();
    thread::spawn(move || {
        if let Err(err) = scan_worker(
            left_root,
            right_root,
            compare_mode,
            scan_tx.clone(),
            scan_exclude,
            scan_last,
        ) {
            let _ = scan_tx.send(WorkerEvent::Error(err.to_string()));
        }
    });

    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen).ok();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    let mut state = AppState::new();

    let res = run_app(
        &mut terminal,
        &rx,
        tx,
        &mut state,
        &conn,
        run_id,
        &args,
        &last_diffs,
    );

    disable_raw_mode().ok();
    terminal.backend_mut().execute(LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    res
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    rx: &Receiver<WorkerEvent>,
    tx: Sender<WorkerEvent>,
    state: &mut AppState,
    conn: &Connection,
    run_id: i64,
    args: &Args,
    last_diffs: &HashMap<PathBuf, LastEntry>,
) -> Result<()> {
    loop {
        while let Ok(event) = rx.try_recv() {
            match event {
                WorkerEvent::ScanProgress { side, count } => match side {
                    Side::Left => state.scanned_left = count,
                    Side::Right => state.scanned_right = count,
                },
                WorkerEvent::ScanDone {
                    left,
                    right,
                    errors,
                } => {
                    if !errors.is_empty() {
                        for error in errors {
                            insert_error(conn, run_id, &error)?;
                        }
                    }
                    state.diffs = compute_diffs(&left, &right, args.compare, last_diffs);
                    if state.sort_by_name {
                        state.diffs.sort_by(|a, b| a.path_rel.cmp(&b.path_rel));
                    }
                    state.selected.select(Some(0));
                    state.selected_items.clear();
                    state.action_overrides.clear();
                    recompute_filtered_indices(state);
                    insert_diffs(conn, run_id, &state.diffs)?;
                    state.phase = Phase::Review;
                    state.status_line = "Scan complete. Review differences.".to_string();
                }
                WorkerEvent::SyncProgress {
                    completed,
                    total,
                    bytes,
                } => {
                    state.verifying = false;
                    state.sync_completed = completed;
                    state.sync_total = total;
                    state.sync_bytes = bytes;
                    if state.sync_start.is_none() {
                        state.sync_start = Some(Instant::now());
                    }
                    if let Some(start) = state.sync_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 {
                            state.sync_speed_bps = bytes as f64 / elapsed;
                        }
                    }
                }
                WorkerEvent::Verifying => {
                    state.verifying = true;
                    state.verify_done = 0;
                    state.verify_total = 0;
                    state.verify_start = Some(Instant::now());
                    state.verify_speed_bps = 0.0;
                }
                WorkerEvent::SyncFileProgress {
                    src,
                    dst,
                    copied,
                    total,
                } => {
                    state.verifying = false;
                    if state.current_src.as_ref() != Some(&src) {
                        state.current_start = Some(Instant::now());
                    }
                    state.current_src = Some(src);
                    state.current_dst = Some(dst.clone());
                    state.current_copied = copied;
                    state.current_total = total;
                    if total > 0 && copied >= total {
                        state.last_copied_dst = Some(state.current_dst.clone().unwrap_or(dst));
                        if let Some(ref rel) =
                            state.last_results.last().map(|r| r.action.path_rel.clone())
                        {
                            state.copied_recently.insert(rel.clone());
                        }
                        if let Some(ref p) = state.last_copied_dst {
                            state.status_line = format!("Copied: {}", p.display());
                        }
                    }
                    if let Some(start) = state.current_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 {
                            state.current_speed_bps = copied as f64 / elapsed;
                        }
                    }
                }
                WorkerEvent::VerifyProgress { done, total } => {
                    state.verify_done = done;
                    state.verify_total = total;
                    if state.verify_start.is_none() {
                        state.verify_start = Some(Instant::now());
                    }
                    if let Some(start) = state.verify_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 {
                            state.verify_speed_bps = done as f64 / elapsed;
                        }
                    }
                }
                WorkerEvent::SyncDone { results } => {
                    for result in &results {
                        insert_action_result(conn, run_id, result)?;
                    }
                    // Track copied files; keep them in list (excluded by default next sync)
                    for r in &results {
                        if r.error.is_none() && (r.outcome == "ok" || r.outcome == "dry-run") {
                            state.copied_recently.insert(r.action.path_rel.clone());
                        }
                    }
                    state.last_results = results;
                    state.phase = Phase::Done;
                    let base = if args.dry_run {
                        "Dry-run complete"
                    } else {
                        "Sync complete"
                    };
                    let copied_count = state.copied_recently.len();
                    state.status_line = match state.last_copied_dst.as_ref() {
                        Some(path) => format!(
                            "{}. Last copied: {}. {} item(s) excluded from next sync.",
                            base,
                            path.display(),
                            copied_count
                        ),
                        None => format!(
                            "{}. {} item(s) excluded from next sync.",
                            base, copied_count
                        ),
                    };
                    finalize_run(conn, run_id, "done")?;
                }
                WorkerEvent::Error(err) => {
                    state.status_line = err;
                    state.phase = Phase::Done;
                    finalize_run(conn, run_id, "error")?;
                }
            }
            state.dirty = true;
        }
        // Throttle drawing to reduce CPU; only draw when dirty or after 100ms
        let should_draw = state.dirty || state.last_draw.elapsed() >= Duration::from_millis(100);
        if should_draw {
            terminal.draw(|frame| {
            let size = frame.size();
            frame.render_widget(Clear, size);
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(60),
                    Constraint::Percentage(20),
                ])
                .split(size);

            let title = if args.dry_run {
                format!("DriveMirror - {:?} (dry-run)", state.phase)
            } else {
                format!("DriveMirror - {:?}", state.phase)
            };
            let (lr_bytes, rl_bytes, miss_l, miss_r, mism) = compute_sync_overview(state);
            let hdr = format!(
                "{} | L->R: {}  R->L: {} | missing-L: {}  missing-R: {}  mismatch: {}",
                title,
                format_bytes(lr_bytes),
                format_bytes(rl_bytes),
                miss_l,
                miss_r,
                mism
            );
            let _l_label = args.left.display().to_string();
            let _r_label = args.right.display().to_string();
            let (l_free, l_total) = space_info(&args.left);
            let (r_free, r_total) = space_info(&args.right);
            // Add an extra blank line between the summary and the free-space line for readability
            let hdr2 = format!(
                "{}\n\nL: {} free/{}    R: {} free/{}",
                hdr,
                format_bytes(l_free),
                format_bytes(l_total),
                format_bytes(r_free),
                format_bytes(r_total)
            );
            let header = Paragraph::new(hdr2)
                .block(Block::default().borders(Borders::ALL))
                .wrap(Wrap { trim: true });
            frame.render_widget(header, chunks[0]);

            match state.phase {
                Phase::Scanning => {
                    let body = Paragraph::new(format!(
                        "Scanning... left: {} files, right: {} files",
                        state.scanned_left, state.scanned_right
                    ))
                    .block(Block::default().borders(Borders::ALL).title("Progress"))
                    .wrap(Wrap { trim: true });
                    frame.render_widget(body, chunks[1]);
                }
                Phase::Review | Phase::ChoosingStrategy | Phase::ConfirmSync => {
                    let body_chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                        .split(chunks[1]);

                    let width = body_chunks[0].width.saturating_sub(4) as usize;
                    let height = body_chunks[0].height.saturating_sub(2) as usize; // account for borders
                    let total = state.filtered_indices.len();
                    let selected_idx = state.selected.selected().unwrap_or(0).min(total.saturating_sub(1));
                    let half = height / 2;
                    let mut start = selected_idx.saturating_sub(half);
                    if start + height > total { start = total.saturating_sub(height); }
                    let end = (start + height).min(total);
                    let list_cols = Layout::default()
                        .direction(Direction::Horizontal)
                        .constraints([Constraint::Min(1), Constraint::Length(1)])
                        .split(body_chunks[0]);
                    let items: Vec<ListItem> = state
                        .filtered_indices
                        .iter()
                        .enumerate()
                        .skip(start)
                        .take(end.saturating_sub(start))
                        .map(|(_, &orig_idx)| {
                            let selected_mark = if state.selected_items.contains(&orig_idx) {
                                "[*] "
                            } else {
                                "[ ] "
                            };
                            let diff = &state.diffs[orig_idx];
                            let status = match diff.status {
                                DiffStatus::Same => "same",
                                DiffStatus::MissingLeft => "missing-left",
                                DiffStatus::MissingRight => "missing-right",
                                DiffStatus::Mismatch => "mismatch",
                                DiffStatus::Conflict => "conflict",
                            };
                            let override_mark = state
                                .action_overrides
                                .get(&diff.path_rel)
                                .map(|action| match action {
                                    ActionType::CopyLeftToRight => " =>L",
                                    ActionType::CopyRightToLeft => " =>R",
                                    ActionType::DeleteLeft => " DEL-L",
                                    ActionType::DeleteRight => " DEL-R",
                                })
                                .unwrap_or("");
                            let copied_mark = if state.copied_recently.contains(&diff.path_rel) {
                                " ✓"
                            } else {
                                ""
                            };
                            let mut content = format!(
                                "{}{} [{}]{}{}",
                                selected_mark,
                                diff.path_rel.display(),
                                status,
                                override_mark,
                                copied_mark
                            );
                            content = truncate_to_width(&content, width.max(10));
                            let lines: Vec<Line> = vec![Line::from(content)];
                            let mut item = ListItem::new(lines);
                            if state.copied_recently.contains(&diff.path_rel)
                                && !state.force_recopy.contains(&diff.path_rel)
                            {
                                item = item.style(Style::default().fg(Color::Green));
                            }
                            item
                        })
                        .collect();
                    let list = List::new(items)
                        .block(Block::default().borders(Borders::ALL).title("Differences"))
                        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
                        .highlight_symbol("> ");
                    // Adjust selection index to the visible window so the cursor aligns with items
                    let mut display_state = ListState::default();
                    display_state.select(Some(selected_idx.saturating_sub(start)));
                    frame.render_stateful_widget(list, list_cols[0], &mut display_state);
                    render_scrollbar(frame, list_cols[1], start, height, total);

                    let details = Paragraph::new(details_text(state, args))
                        .block(Block::default().borders(Borders::ALL).title("Details"))
                        .wrap(Wrap { trim: true });
                    frame.render_widget(details, body_chunks[1]);
                }
                Phase::History => {
                    let body_chunks = Layout::default()
                        .direction(Direction::Vertical)
                        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                        .split(chunks[1]);

                    let width = body_chunks[0].width.saturating_sub(4) as usize;
                    let items: Vec<ListItem> = state
                        .history
                        .iter()
                        .map(|entry| {
                            let line = format!(
                                "#{} {} [{}] actions:{} errors:{}",
                                entry.run_id, entry.started_at, entry.status, entry.actions, entry.errors
                            );
                            let wrapped = wrap_text(&line, width.max(10));
                            let lines: Vec<Line> = wrapped.into_iter().map(Line::from).collect();
                            ListItem::new(lines)
                        })
                        .collect();
                    let list = List::new(items)
                        .block(Block::default().borders(Borders::ALL).title("History"))
                        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
                        .highlight_symbol("> ");
                    frame.render_stateful_widget(list, body_chunks[0], &mut state.history_selected);

                    let details = Paragraph::new(history_details_text(state))
                        .block(Block::default().borders(Borders::ALL).title("Details"))
                        .wrap(Wrap { trim: true });
                    frame.render_widget(details, body_chunks[1]);
                }
                Phase::Syncing => {
                    let bar_width = chunks[1].width.saturating_sub(20) as usize;
                    let overall_bar = progress_bar(state.sync_completed as u64, state.sync_total as u64, bar_width.max(10));
                    let file_bar = progress_bar(state.current_copied, state.current_total, bar_width.max(10));
                    let current_src = state
                        .current_src
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "n/a".to_string());
                    let current_dst = state
                        .current_dst
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "n/a".to_string());
                    let _verifying_line = if state.verifying {
                        let bar = progress_bar(state.verify_done, state.verify_total, bar_width.max(10));
                        let eta = format_eta(eta_seconds(state.verify_done, state.verify_total, state.verify_speed_bps));
                        format!("\nVerifying... {} (ETA {})", bar, eta)
                    } else {
                        String::new()
                    };
                    let elapsed = state.sync_start.map(|s| s.elapsed().as_secs()).unwrap_or(0);
                    let elapsed_fmt = format_eta(elapsed);
                    let (l_free, l_total) = space_info(&args.left);
                    let (r_free, r_total) = space_info(&args.right);
                    let body = Paragraph::new(format!(
                        "Syncing... {}/{} files (elapsed {})\n{}\n{} copied, {}/s\nsrc: {}\ndst: {}\nfile: {}/{} @ {}/s\nfree L: {}/{}, free R: {}/{}\n{}",
                        state.sync_completed,
                        state.sync_total,
                        elapsed_fmt,
                        overall_bar,
                        format_bytes(state.sync_bytes),
                        format_bytes_per_sec(state.sync_speed_bps),
                        current_src,
                        current_dst,
                        format_bytes(state.current_copied),
                        format_bytes(state.current_total),
                        format_bytes_per_sec(state.current_speed_bps),
                        format_bytes(l_free), format_bytes(l_total),
                        format_bytes(r_free), format_bytes(r_total),
                        file_bar
                    ))
                    .block(Block::default().borders(Borders::ALL).title("Progress"))
                    .wrap(Wrap { trim: true });
                    frame.render_widget(body, chunks[1]);
                }
                Phase::Done => {
                    let mut summary = String::new();
                    summary.push_str(&state.status_line);
                    let mut copied: Vec<String> = state
                        .last_results
                        .iter()
                        .filter(|r| r.error.is_none() && (r.outcome == "ok" || r.outcome == "dry-run"))
                        .map(|r| r.action.path_rel.display().to_string())
                        .collect();
                    copied.sort();
                    if !copied.is_empty() {
                        summary.push_str("\nCopied files:\n");
                        for p in copied {
                            summary.push_str("- ");
                            summary.push_str(&p);
                            summary.push('\n');
                        }
                    }
                    let body = Paragraph::new(summary)
                        .block(Block::default().borders(Borders::ALL).title("Summary"))
                        .wrap(Wrap { trim: true });
                    frame.render_widget(body, chunks[1]);
                }
            }

            let footer = Paragraph::new(help_text(state))
                .block(Block::default().borders(Borders::ALL).title("Help"))
                .wrap(Wrap { trim: true });
            frame.render_widget(footer, chunks[2]);
        })?;
            state.last_draw = Instant::now();
            state.dirty = false;
        }

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                state.dirty = true;
                if key.code == KeyCode::Esc {
                    let now = Instant::now();
                    // In Review: if a filter is active, clear it and return to unfiltered view
                    if state.phase == Phase::Review && state.filter != Filter::All {
                        state.filter = Filter::All;
                        recompute_filtered_indices(state);
                        state.selected_items.clear();
                        state.status_line = "Filter cleared (All).".to_string();
                        state.dirty = true;
                        continue;
                    }
                    if let Some(last) = state.last_esc {
                        if now.duration_since(last) <= Duration::from_millis(600) {
                            break;
                        }
                    }
                    state.last_esc = Some(now);
                    state.status_line = "Press Esc again to quit.".to_string();
                    match state.phase {
                        Phase::ConfirmSync
                        | Phase::ChoosingStrategy
                        | Phase::Syncing
                        | Phase::Done
                        | Phase::History => {
                            state.phase = Phase::Review;
                            state.status_line = "Back to review.".to_string();
                        }
                        _ => {}
                    }
                    continue;
                }
                if key.code == KeyCode::Char('h') {
                    state.history = load_history(conn)?;
                    state.history_selected.select(Some(0));
                    state.phase = Phase::History;
                    state.status_line = "History loaded.".to_string();
                    continue;
                }
                if key.code == KeyCode::Char('o') {
                    if let Some(path) = state.last_copied_dst.as_ref() {
                        if let Err(err) = reveal_in_file_manager(path) {
                            state.status_line = format!("Open failed: {}", err);
                        } else {
                            state.status_line = "Revealed in file manager.".to_string();
                        }
                    } else {
                        state.status_line = "No copied file yet.".to_string();
                    }
                    continue;
                }
                if let KeyCode::F(5) = key.code {
                    // Refresh: rescan differences
                    state.phase = Phase::Scanning;
                    state.scanned_left = 0;
                    state.scanned_right = 0;
                    state.status_line = "Refreshing...".to_string();
                    let left_root = args.left.clone();
                    let right_root = args.right.clone();
                    let compare_mode = args.compare;
                    let scan_tx = tx.clone();
                    let exclude = build_exclude_set(&args.exclude)?;
                    let last = load_last_run_diffs(conn, &args.left, &args.right)?;
                    thread::spawn(move || {
                        let _ = scan_worker(
                            left_root,
                            right_root,
                            compare_mode,
                            scan_tx,
                            exclude,
                            last,
                        );
                    });
                    continue;
                }
                match state.phase {
                    Phase::Review => handle_review_input(state, key.code, key.modifiers),
                    Phase::History => handle_history_input(state, key.code),
                    Phase::ChoosingStrategy => handle_strategy_input(state, key.code),
                    Phase::ConfirmSync => {
                        handle_confirm_input(state, key.code, args, conn, run_id, &tx)?
                    }
                    Phase::Done => {
                        if key.code == KeyCode::Char('q') {
                            break;
                        }
                    }
                    Phase::Scanning => {
                        if key.code == KeyCode::Char('q') {
                            state.status_line = "Exiting...".to_string();
                            break;
                        }
                    }
                    Phase::Syncing => {
                        if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                            state.cancel_after_current.store(true, Ordering::Relaxed);
                            state.status_line =
                                "Will stop after current file finishes...".to_string();
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn help_text(state: &AppState) -> Line<'static> {
    match state.phase {
        Phase::Review => Line::from(vec![
            Span::raw("Up/Down: move "),
            Span::raw("Enter: sync selected "),
            Span::raw("Space: toggle "),
            Span::raw("a: select all "),
            Span::raw("c: clear "),
            Span::raw("s: sync all (filter) "),
            Span::raw("l: override L->R "),
            Span::raw("r: override R->L "),
            Span::raw("d: delete extras "),
            Span::raw("f: force recopy "),
            Span::raw("1: all 2: missing-L 3: missing-R 4: mismatch 5: conflict "),
            Span::raw("n: sort name "),
            Span::raw("F5: refresh "),
            Span::raw("h: history "),
            Span::raw("q: quit "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::History => Line::from(vec![
            Span::raw("Up/Down: move "),
            Span::raw("Esc: back "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::ChoosingStrategy => Line::from(vec![
            Span::raw("Pick mismatch strategy: "),
            Span::raw("n=newer, l=left, r=right, k=skip "),
            Span::raw("b: back "),
            Span::raw("Esc: back "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::ConfirmSync => Line::from(vec![
            Span::raw("Apply sync? Enter/y yes, n no "),
            Span::raw("b: back "),
            Span::raw("Esc: back "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::Syncing => Line::from(vec![
            Span::raw("Syncing... q to quit "),
            Span::raw("Esc: back "),
            Span::raw("o: reveal last copy "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::Done => Line::from(vec![
            Span::raw("q: quit "),
            Span::raw("Esc: back "),
            Span::raw("o: reveal last copy "),
            Span::raw("Esc Esc: quit"),
        ]),
        Phase::Scanning => Line::from(vec![
            Span::raw("Scanning... q: quit "),
            Span::raw("Esc Esc: quit"),
        ]),
    }
}

fn apply_override(state: &mut AppState, action: ActionType) {
    if state.diffs.is_empty() {
        return;
    }
    let targets: Vec<usize> = if state.selected_items.is_empty() {
        vec![state.selected.selected().unwrap_or(0)]
    } else {
        state.selected_items.iter().copied().collect()
    };
    for idx in targets {
        if let Some(diff) = state.diffs.get(idx) {
            if diff.status == DiffStatus::Mismatch || diff.status == DiffStatus::Conflict {
                state.action_overrides.insert(diff.path_rel.clone(), action);
            }
        }
    }
    state.status_line = match action {
        ActionType::CopyLeftToRight => "Override: copy left to right.".to_string(),
        ActionType::CopyRightToLeft => "Override: copy right to left.".to_string(),
        ActionType::DeleteLeft => "Override: delete left.".to_string(),
        ActionType::DeleteRight => "Override: delete right.".to_string(),
    };
}

fn apply_delete_override(state: &mut AppState) {
    if state.diffs.is_empty() {
        return;
    }
    let targets: Vec<usize> = if state.selected_items.is_empty() {
        vec![state.selected.selected().unwrap_or(0)]
    } else {
        state.selected_items.iter().copied().collect()
    };
    let mut applied = false;
    for idx in targets {
        if let Some(diff) = state.diffs.get(idx) {
            match diff.status {
                DiffStatus::MissingLeft => {
                    state
                        .action_overrides
                        .insert(diff.path_rel.clone(), ActionType::DeleteRight);
                    applied = true;
                }
                DiffStatus::MissingRight => {
                    state
                        .action_overrides
                        .insert(diff.path_rel.clone(), ActionType::DeleteLeft);
                    applied = true;
                }
                _ => {}
            }
        }
    }
    state.status_line = if applied {
        "Override: delete selected extras.".to_string()
    } else {
        "Delete applies only to missing entries.".to_string()
    };
}

fn handle_history_input(state: &mut AppState, code: KeyCode) {
    match code {
        KeyCode::Down => {
            let len = state.history.len();
            if len == 0 {
                return;
            }
            let next = match state.history_selected.selected() {
                Some(idx) => (idx + 1).min(len - 1),
                None => 0,
            };
            state.history_selected.select(Some(next));
        }
        KeyCode::Up => {
            let len = state.history.len();
            if len == 0 {
                return;
            }
            let prev = match state.history_selected.selected() {
                Some(idx) => idx.saturating_sub(1),
                None => 0,
            };
            state.history_selected.select(Some(prev));
        }
        _ => {}
    }
}

fn details_text(state: &AppState, args: &Args) -> String {
    if state.diffs.is_empty() {
        return "No selection".to_string();
    }
    let mut lines = Vec::new();
    let selection: Vec<usize> = if state.selected_items.is_empty() {
        vec![state.selected.selected().unwrap_or(0)]
    } else {
        state.selected_items.iter().copied().collect()
    };

    for idx in selection {
        if let Some(diff) = state.diffs.get(idx) {
            let status = match diff.status {
                DiffStatus::Same => "same",
                DiffStatus::MissingLeft => "missing-left",
                DiffStatus::MissingRight => "missing-right",
                DiffStatus::Mismatch => "mismatch",
                DiffStatus::Conflict => "conflict",
            };
            let left_path = args.left.join(&diff.path_rel);
            let right_path = args.right.join(&diff.path_rel);
            let left = meta_line("Left", &left_path, diff.left.as_ref());
            let right = meta_line("Right", &right_path, diff.right.as_ref());
            let override_line = state
                .action_overrides
                .get(&diff.path_rel)
                .map(|action| match action {
                    ActionType::CopyLeftToRight => "Override: left -> right",
                    ActionType::CopyRightToLeft => "Override: right -> left",
                    ActionType::DeleteLeft => "Override: delete left",
                    ActionType::DeleteRight => "Override: delete right",
                })
                .unwrap_or("Override: none");
            lines.push(format!(
                "{}\nStatus: {}\n{}\n{}\n{}",
                diff.path_rel.display(),
                status,
                left,
                right,
                override_line
            ));
        }
    }

    if lines.is_empty() {
        "No selection".to_string()
    } else {
        lines.join("\n---\n")
    }
}

fn compute_sync_overview(state: &AppState) -> (u64, u64, usize, usize, usize) {
    // Returns (bytes L->R, bytes R->L, missing_left_count, missing_right_count, mismatch_count)
    let mut lr_bytes = 0u64;
    let mut rl_bytes = 0u64;
    let mut miss_l = 0usize;
    let mut miss_r = 0usize;
    let mut mism = 0usize;
    for d in &state.diffs {
        // Skip if copied and not forced to recopy
        if state.copied_recently.contains(&d.path_rel) && !state.force_recopy.contains(&d.path_rel)
        {
            continue;
        }
        match d.status {
            DiffStatus::MissingLeft => {
                miss_l += 1;
                if let Some(ref r) = d.right {
                    rl_bytes = rl_bytes.saturating_add(r.size);
                }
            }
            DiffStatus::MissingRight => {
                miss_r += 1;
                if let Some(ref l) = d.left {
                    lr_bytes = lr_bytes.saturating_add(l.size);
                }
            }
            DiffStatus::Mismatch | DiffStatus::Conflict => {
                mism += 1;
                // Estimate by newer mtime (default), unless override present
                if let Some(action) = state.action_overrides.get(&d.path_rel) {
                    match action {
                        ActionType::CopyLeftToRight => {
                            if let Some(ref l) = d.left {
                                lr_bytes = lr_bytes.saturating_add(l.size);
                            }
                        }
                        ActionType::CopyRightToLeft => {
                            if let Some(ref r) = d.right {
                                rl_bytes = rl_bytes.saturating_add(r.size);
                            }
                        }
                        ActionType::DeleteLeft | ActionType::DeleteRight => {}
                    }
                } else {
                    let lm = d.left.as_ref().map(|m| m.mtime).unwrap_or(0);
                    let rm = d.right.as_ref().map(|m| m.mtime).unwrap_or(0);
                    if lm >= rm {
                        if let Some(ref l) = d.left {
                            lr_bytes = lr_bytes.saturating_add(l.size);
                        }
                    } else if let Some(ref r) = d.right {
                        rl_bytes = rl_bytes.saturating_add(r.size);
                    }
                }
            }
            DiffStatus::Same => {}
        }
    }
    (lr_bytes, rl_bytes, miss_l, miss_r, mism)
}

fn render_scrollbar(
    frame: &mut ratatui::Frame,
    area: ratatui::layout::Rect,
    start: usize,
    height: usize,
    total: usize,
) {
    let h = area.height as usize;
    if h == 0 || total == 0 {
        return;
    }
    let thumb_size = height.max(1).min(h);
    let max_start = total.saturating_sub(height).max(1);
    let pos = ((start as f64 / max_start as f64) * (h.saturating_sub(thumb_size) as f64)).round()
        as usize;
    let mut lines: Vec<String> = Vec::with_capacity(h);
    for i in 0..h {
        if i >= pos && i < pos + thumb_size {
            lines.push("█".to_string());
        } else {
            lines.push("│".to_string());
        }
    }
    let text = lines.join("\n");
    let para = Paragraph::new(text);
    frame.render_widget(para, area);
}

fn history_details_text(state: &AppState) -> String {
    let idx = match state.history_selected.selected() {
        Some(idx) => idx,
        None => return "No history".to_string(),
    };
    let entry = match state.history.get(idx) {
        Some(entry) => entry,
        None => return "No history".to_string(),
    };
    format!(
        "Run #{}\nStatus: {}\nStarted: {}\nCompleted: {}\nLeft: {}\nRight: {}\nActions: {}\nErrors: {}",
        entry.run_id,
        entry.status,
        entry.started_at,
        entry.completed_at.clone().unwrap_or_else(|| "-".to_string()),
        entry.left_root,
        entry.right_root,
        entry.actions,
        entry.errors
    )
}

fn meta_line(label: &str, path: &Path, meta: Option<&FileMeta>) -> String {
    match meta {
        Some(meta) => {
            let link_info = if meta.is_symlink {
                format!(
                    " | symlink -> {}",
                    meta.link_target
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string())
                )
            } else {
                String::new()
            };
            format!(
                "{}: {} | size {} | mtime {} | hash {}{}",
                label,
                path.display(),
                format_bytes(meta.size),
                format_mtime(meta.mtime),
                meta.hash.clone().unwrap_or_else(|| "n/a".to_string()),
                link_info
            )
        }
        None => format!("{}: {} | missing", label, path.display()),
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.2} MB", b / MB)
    } else if b >= KB {
        format!("{:.2} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn format_mtime(mtime: i64) -> String {
    match chrono::DateTime::<chrono::Utc>::from_timestamp(mtime, 0) {
        Some(dt) => dt.to_rfc3339(),
        None => mtime.to_string(),
    }
}

fn reveal_in_file_manager(path: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg("-R")
            .arg(path)
            .status()
            .context("open -R")?;
        return Ok(());
    }
    #[cfg(target_os = "windows")]
    {
        Command::new("explorer")
            .arg("/select,")
            .arg(path)
            .status()
            .context("explorer /select")?;
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        let parent = path.parent().unwrap_or(path);
        Command::new("xdg-open")
            .arg(parent)
            .status()
            .context("xdg-open")?;
        return Ok(());
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn wrap_text(text: &str, max_width: usize) -> Vec<String> {
    if max_width == 0 {
        return vec![text.to_string()];
    }
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            if word.len() > max_width {
                let mut chunk = word;
                while chunk.len() > max_width {
                    let (head, tail) = chunk.split_at(max_width);
                    lines.push(head.to_string());
                    chunk = tail;
                }
                current = chunk.to_string();
            } else {
                current.push_str(word);
            }
        } else if current.len() + 1 + word.len() <= max_width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(current);
            current = String::new();
            if word.len() > max_width {
                let mut chunk = word;
                while chunk.len() > max_width {
                    let (head, tail) = chunk.split_at(max_width);
                    lines.push(head.to_string());
                    chunk = tail;
                }
                current = chunk.to_string();
            } else {
                current.push_str(word);
            }
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn truncate_to_width(text: &str, max_width: usize) -> String {
    if text.chars().count() <= max_width {
        return text.to_string();
    }
    let ellipsis = "…";
    let target = max_width.saturating_sub(1);
    let mut out = String::new();
    for (i, ch) in text.chars().enumerate() {
        if i >= target {
            break;
        }
        out.push(ch);
    }
    out.push_str(ellipsis);
    out
}

fn handle_review_input(state: &mut AppState, code: KeyCode, modifiers: KeyModifiers) {
    // Shift + Down: select current and move down
    if modifiers.contains(KeyModifiers::SHIFT) {
        match code {
            KeyCode::Down => {
                if !state.diffs.is_empty() {
                    if let Some(local) = state.selected.selected() {
                        let idx = *state.filtered_indices.get(local).unwrap_or(&0);
                        state.selected_items.insert(idx);
                        let next = (local + 1).min(state.filtered_indices.len().saturating_sub(1));
                        state.selected.select(Some(next));
                    }
                }
                return;
            }
            KeyCode::Up => {
                if !state.diffs.is_empty() {
                    if let Some(local) = state.selected.selected() {
                        if let Some(idx) = state.filtered_indices.get(local) {
                            state.selected_items.remove(idx);
                        }
                        let prev = local.saturating_sub(1);
                        state.selected.select(Some(prev));
                    }
                }
                return;
            }
            _ => {}
        }
    }
    match code {
        KeyCode::Down => {
            let len = state.filtered_indices.len();
            if len == 0 {
                return;
            }
            let next = match state.selected.selected() {
                Some(idx) => (idx + 1).min(len - 1),
                None => 0,
            };
            state.selected.select(Some(next));
        }
        KeyCode::Up => {
            let len = state.filtered_indices.len();
            if len == 0 {
                return;
            }
            let prev = match state.selected.selected() {
                Some(idx) => idx.saturating_sub(1),
                None => 0,
            };
            state.selected.select(Some(prev));
        }
        KeyCode::Char('s') => {
            state.sync_scope = SyncScope::All;
            // Preselect all filtered items for visual feedback
            state.selected_items.clear();
            for &idx in &state.filtered_indices {
                state.selected_items.insert(idx);
            }
            // If current filter is purely missing-left or missing-right, skip strategy prompt
            let only_missing_left = state.filter == Filter::MissingLeft;
            let only_missing_right = state.filter == Filter::MissingRight;
            let has_mismatch = if only_missing_left || only_missing_right {
                false
            } else {
                // Check mismatches in the filtered set
                state
                    .filtered_indices
                    .iter()
                    .filter_map(|&i| state.diffs.get(i))
                    .any(|d| d.status == DiffStatus::Mismatch || d.status == DiffStatus::Conflict)
            };

            if has_mismatch {
                state.phase = Phase::ChoosingStrategy;
                state.status_line = "Choose mismatch strategy.".to_string();
            } else {
                state.phase = Phase::ConfirmSync;
                // Set a clear status depending on filter
                state.status_line = match state.filter {
                    Filter::MissingLeft => {
                        "Confirm sync (copy Right -> Left for all filtered).".to_string()
                    }
                    Filter::MissingRight => {
                        "Confirm sync (copy Left -> Right for all filtered).".to_string()
                    }
                    _ => "Confirm sync.".to_string(),
                };
            }
        }
        KeyCode::Enter => {
            if state.diffs.is_empty() {
                return;
            }
            state.sync_scope = SyncScope::Selected;
            let has_mismatch = if state.selected_items.is_empty() {
                let local = state.selected.selected().unwrap_or(0);
                let idx = *state.filtered_indices.get(local).unwrap_or(&0);
                matches!(
                    state.diffs.get(idx).map(|d| &d.status),
                    Some(DiffStatus::Mismatch) | Some(DiffStatus::Conflict)
                )
            } else {
                state
                    .selected_items
                    .iter()
                    .filter_map(|i| state.diffs.get(*i))
                    .any(|d| d.status == DiffStatus::Mismatch || d.status == DiffStatus::Conflict)
            };
            if has_mismatch {
                state.phase = Phase::ChoosingStrategy;
                state.status_line = "Choose mismatch strategy.".to_string();
            } else {
                state.phase = Phase::ConfirmSync;
                state.status_line = "Confirm sync.".to_string();
            }
        }
        KeyCode::Char('1') => {
            state.filter = Filter::All;
            state.selected.select(Some(0));
            recompute_filtered_indices(state);
        }
        KeyCode::Char('2') => {
            state.filter = Filter::MissingLeft;
            state.selected.select(Some(0));
            recompute_filtered_indices(state);
        }
        KeyCode::Char('3') => {
            state.filter = Filter::MissingRight;
            state.selected.select(Some(0));
            recompute_filtered_indices(state);
        }
        KeyCode::Char('4') => {
            state.filter = Filter::Mismatch;
            state.selected.select(Some(0));
            recompute_filtered_indices(state);
        }
        KeyCode::Char('5') => {
            state.filter = Filter::Conflict;
            state.selected.select(Some(0));
            recompute_filtered_indices(state);
        }
        KeyCode::Char('q') => {
            state.phase = Phase::Done;
            state.status_line = "Quit.".to_string();
        }
        KeyCode::Char(' ') => {
            if state.diffs.is_empty() {
                return;
            }
            let idx = state.selected.selected().unwrap_or(0);
            if state.selected_items.contains(&idx) {
                state.selected_items.remove(&idx);
            } else {
                state.selected_items.insert(idx);
            }
        }
        KeyCode::Char('a') => {
            state.selected_items = (0..state.diffs.len()).collect();
            state.status_line = "Selected all.".to_string();
        }
        KeyCode::Char('c') => {
            state.selected_items.clear();
            state.status_line = "Selection cleared.".to_string();
        }
        KeyCode::Char('l') => {
            apply_override(state, ActionType::CopyLeftToRight);
        }
        KeyCode::Char('r') => {
            apply_override(state, ActionType::CopyRightToLeft);
        }
        KeyCode::Char('f') => {
            // toggle force recopy for selected
            let targets: Vec<usize> = if state.selected_items.is_empty() {
                vec![state.selected.selected().unwrap_or(0)]
            } else {
                state.selected_items.iter().copied().collect()
            };
            for idx in targets {
                if let Some(diff) = state.diffs.get(idx) {
                    if state.force_recopy.contains(&diff.path_rel) {
                        state.force_recopy.remove(&diff.path_rel);
                    } else {
                        state.force_recopy.insert(diff.path_rel.clone());
                    }
                }
            }
            state.status_line = "Toggled force recopy.".to_string();
        }
        KeyCode::Char('d') => {
            apply_delete_override(state);
        }
        KeyCode::Char('n') => {
            state.sort_by_name = !state.sort_by_name;
            if state.sort_by_name {
                state.diffs.sort_by(|a, b| a.path_rel.cmp(&b.path_rel));
                state.status_line = "Sorted by name.".to_string();
            } else {
                state.status_line = "Name sort off.".to_string();
            }
        }
        _ => {}
    }
}

fn handle_strategy_input(state: &mut AppState, code: KeyCode) {
    if code == KeyCode::Char('b') {
        state.phase = Phase::Review;
        state.status_line = "Back to review.".to_string();
        return;
    }
    state.mismatch_strategy = match code {
        KeyCode::Char('n') => Some(MismatchStrategy::NewerMtime),
        KeyCode::Char('l') => Some(MismatchStrategy::PreferLeft),
        KeyCode::Char('r') => Some(MismatchStrategy::PreferRight),
        KeyCode::Char('k') => Some(MismatchStrategy::Skip),
        _ => state.mismatch_strategy,
    };
    if state.mismatch_strategy.is_some() {
        state.phase = Phase::ConfirmSync;
        state.status_line = "Confirm sync.".to_string();
    }
}

fn handle_confirm_input(
    state: &mut AppState,
    code: KeyCode,
    args: &Args,
    conn: &Connection,
    run_id: i64,
    tx: &Sender<WorkerEvent>,
) -> Result<()> {
    if code == KeyCode::Char('b') {
        state.phase = Phase::Review;
        state.status_line = "Back to review.".to_string();
        return Ok(());
    }
    match code {
        KeyCode::Char('y') | KeyCode::Enter => {
            let strategy = state
                .mismatch_strategy
                .unwrap_or(MismatchStrategy::NewerMtime);
            let diffs = match state.sync_scope {
                SyncScope::All => {
                    // Sync all items in the current filter view
                    if state.filtered_indices.is_empty() {
                        state.diffs.clone()
                    } else {
                        state
                            .filtered_indices
                            .iter()
                            .filter_map(|&i| state.diffs.get(i).cloned())
                            .collect()
                    }
                }
                SyncScope::Selected => {
                    if state.selected_items.is_empty() {
                        let idx_local = state.selected.selected().unwrap_or(0);
                        let idx = *state.filtered_indices.get(idx_local).unwrap_or(&0);
                        state.diffs.get(idx).cloned().into_iter().collect()
                    } else {
                        state
                            .selected_items
                            .iter()
                            .filter_map(|i| state.diffs.get(*i).cloned())
                            .collect()
                    }
                }
            };
            state.pending_actions = plan_actions(
                &diffs,
                strategy,
                &state.action_overrides,
                &state.copied_recently,
                &state.force_recopy,
            );
            if state.pending_actions.is_empty() {
                state.phase = Phase::Done;
                let has_conflict = diffs.iter().any(|d| d.status == DiffStatus::Conflict);
                state.status_line = if has_conflict {
                    "No actions. Conflicts need override (l/r).".to_string()
                } else {
                    "No actions to apply.".to_string()
                };
                finalize_run(conn, run_id, "done")?;
                return Ok(());
            }
            let actions = state.pending_actions.clone();
            let left = args.left.clone();
            let right = args.right.clone();
            let compare = args.compare;
            let retries = args.retries;
            let dry_run = args.dry_run;
            let sync_tx = tx.clone();
            let cancel_flag = state.cancel_after_current.clone();
            thread::spawn(move || {
                if let Err(err) = sync_worker(
                    left,
                    right,
                    actions,
                    compare,
                    retries,
                    dry_run,
                    cancel_flag,
                    sync_tx.clone(),
                ) {
                    let _ = sync_tx.send(WorkerEvent::Error(err.to_string()));
                }
            });

            state.phase = Phase::Syncing;
            state.status_line = "Sync in progress.".to_string();
            state.sync_start = None;
            state.sync_speed_bps = 0.0;
            state.current_src = None;
            state.current_dst = None;
            state.current_copied = 0;
            state.current_total = 0;
            state.current_start = None;
            state.current_speed_bps = 0.0;
        }
        KeyCode::Char('n') => {
            state.phase = Phase::Review;
            state.status_line = "Sync canceled.".to_string();
        }
        _ => {}
    }
    Ok(())
}

fn scan_worker(
    left: PathBuf,
    right: PathBuf,
    compare: CompareMode,
    tx: Sender<WorkerEvent>,
    exclude: GlobSet,
    last: HashMap<PathBuf, LastEntry>,
) -> Result<()> {
    let mut errors = Vec::new();
    let left_map = scan_dir(
        &left,
        compare,
        Side::Left,
        &tx,
        &mut errors,
        &exclude,
        &last,
    );
    let right_map = scan_dir(
        &right,
        compare,
        Side::Right,
        &tx,
        &mut errors,
        &exclude,
        &last,
    );
    tx.send(WorkerEvent::ScanDone {
        left: left_map,
        right: right_map,
        errors,
    })?;
    Ok(())
}

fn scan_dir(
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
                    if should_exclude(&rel, exclude) {
                        continue;
                    }
                    let last_entry = last.get(&rel);
                    match build_file_meta(path, compare, side, last_entry) {
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

fn build_file_meta(
    path: &Path,
    compare: CompareMode,
    side: Side,
    last: Option<&LastEntry>,
) -> Result<FileMeta> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("metadata for {}", path.display()))?;
    let is_symlink = metadata.file_type().is_symlink();
    let link_target = if is_symlink {
        fs::read_link(path)
            .ok()
            .map(|p| p.to_string_lossy().to_string())
    } else {
        None
    };
    let size = if is_symlink {
        link_target.as_ref().map(|t| t.len() as u64).unwrap_or(0)
    } else {
        metadata.len()
    };
    let mtime = metadata
        .modified()
        .ok()
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
                        Side::Left => (
                            last_entry.size_left,
                            last_entry.mtime_left,
                            last_entry.hash_left.clone(),
                        ),
                        Side::Right => (
                            last_entry.size_right,
                            last_entry.mtime_right,
                            last_entry.hash_right.clone(),
                        ),
                    };
                    if last_size == Some(size) && last_mtime == Some(mtime) {
                        if let Some(hash) = last_hash {
                            return Ok(FileMeta {
                                size,
                                mtime,
                                hash: Some(hash),
                                is_symlink,
                                link_target,
                            });
                        }
                    }
                }
                Some(hash_file(path)?)
            }
        }
        CompareMode::Size => None,
    };
    Ok(FileMeta {
        size,
        mtime,
        hash,
        is_symlink,
        link_target,
    })
}

fn hash_file(path: &Path) -> Result<String> {
    let mut reader = io::BufReader::new(
        fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut buf = vec![0u8; 1024 * 1024];
    let mut hasher = Hasher::new();
    loop {
        let read = reader
            .read(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_file_progress(path: &Path, tx: &Sender<WorkerEvent>) -> Result<String> {
    let total = fs::metadata(path)
        .with_context(|| format!("metadata {}", path.display()))?
        .len();
    let mut reader = io::BufReader::new(
        fs::File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    let mut buf = vec![0u8; 1024 * 1024];
    let mut hasher = Hasher::new();
    let mut done = 0u64;
    // Emit initial 0% so the UI knows total size immediately
    tx.send(WorkerEvent::VerifyProgress { done, total }).ok();
    loop {
        let read = reader
            .read(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        done += read as u64;
        tx.send(WorkerEvent::VerifyProgress { done, total }).ok();
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn compute_diffs(
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
                    if l.link_target != r.link_target {
                        DiffStatus::Mismatch
                    } else {
                        DiffStatus::Same
                    }
                } else if l.size != r.size {
                    DiffStatus::Mismatch
                } else if compare == CompareMode::Hash {
                    if l.hash != r.hash {
                        DiffStatus::Mismatch
                    } else {
                        DiffStatus::Same
                    }
                } else {
                    DiffStatus::Same
                }
            }
            (None, None) => DiffStatus::Same,
        };
        if status == DiffStatus::Mismatch {
            if let Some(last_entry) = last.get(&path) {
                let left_changed = last_entry
                    .mtime_left
                    .map(|t| left_meta.as_ref().map(|m| m.mtime > t).unwrap_or(false))
                    .unwrap_or(false);
                let right_changed = last_entry
                    .mtime_right
                    .map(|t| right_meta.as_ref().map(|m| m.mtime > t).unwrap_or(false))
                    .unwrap_or(false);
                if left_changed && right_changed {
                    status = DiffStatus::Conflict;
                }
            }
        }
        if status != DiffStatus::Same {
            diffs.push(DiffEntry {
                path_rel: path,
                left: left_meta,
                right: right_meta,
                status,
            });
        }
    }
    diffs
}

fn plan_actions(
    diffs: &[DiffEntry],
    strategy: MismatchStrategy,
    overrides: &HashMap<PathBuf, ActionType>,
    copied_recently: &BTreeSet<PathBuf>,
    force_recopy: &BTreeSet<PathBuf>,
) -> Vec<Action> {
    let mut actions = Vec::new();
    for diff in diffs {
        if copied_recently.contains(&diff.path_rel) && !force_recopy.contains(&diff.path_rel) {
            continue;
        }
        if let Some(override_action) = overrides.get(&diff.path_rel) {
            actions.push(Action {
                path_rel: diff.path_rel.clone(),
                action_type: *override_action,
                reason: "override".to_string(),
            });
            continue;
        }
        match diff.status {
            DiffStatus::MissingLeft => actions.push(Action {
                path_rel: diff.path_rel.clone(),
                action_type: ActionType::CopyRightToLeft,
                reason: "missing-left".to_string(),
            }),
            DiffStatus::MissingRight => actions.push(Action {
                path_rel: diff.path_rel.clone(),
                action_type: ActionType::CopyLeftToRight,
                reason: "missing-right".to_string(),
            }),
            DiffStatus::Mismatch => match strategy {
                MismatchStrategy::Skip => {}
                MismatchStrategy::PreferLeft => actions.push(Action {
                    path_rel: diff.path_rel.clone(),
                    action_type: ActionType::CopyLeftToRight,
                    reason: "mismatch-prefer-left".to_string(),
                }),
                MismatchStrategy::PreferRight => actions.push(Action {
                    path_rel: diff.path_rel.clone(),
                    action_type: ActionType::CopyRightToLeft,
                    reason: "mismatch-prefer-right".to_string(),
                }),
                MismatchStrategy::NewerMtime => {
                    let left_mtime = diff.left.as_ref().map(|m| m.mtime).unwrap_or(0);
                    let right_mtime = diff.right.as_ref().map(|m| m.mtime).unwrap_or(0);
                    if left_mtime >= right_mtime {
                        actions.push(Action {
                            path_rel: diff.path_rel.clone(),
                            action_type: ActionType::CopyLeftToRight,
                            reason: "mismatch-newer-left".to_string(),
                        });
                    } else {
                        actions.push(Action {
                            path_rel: diff.path_rel.clone(),
                            action_type: ActionType::CopyRightToLeft,
                            reason: "mismatch-newer-right".to_string(),
                        });
                    }
                }
            },
            DiffStatus::Conflict => {}
            DiffStatus::Same => {}
        }
    }
    actions
}

fn sync_worker(
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
                let result = copy_and_verify(&src, &dst, compare, retries, dry_run, &tx);
                (src, dst, result)
            }
            ActionType::CopyRightToLeft => {
                let src = right.join(&action.path_rel);
                let dst = left.join(&action.path_rel);
                let result = copy_and_verify(&src, &dst, compare, retries, dry_run, &tx);
                (src, dst, result)
            }
            ActionType::DeleteLeft => {
                let target = left.join(&action.path_rel);
                let result = delete_with_retry(&target, retries, dry_run, &tx);
                (target.clone(), target, result)
            }
            ActionType::DeleteRight => {
                let target = right.join(&action.path_rel);
                let result = delete_with_retry(&target, retries, dry_run, &tx);
                (target.clone(), target, result)
            }
        };
        let duration_ms = start.elapsed().as_millis() as i64;
        let (outcome, error, bytes_copied, verified) = match result {
            Ok(outcome) => {
                bytes += outcome.bytes;
                (
                    if dry_run {
                        "dry-run".to_string()
                    } else {
                        "ok".to_string()
                    },
                    None,
                    outcome.bytes,
                    outcome.verified,
                )
            }
            Err(err) => (err.to_string(), Some(err.to_string()), 0, false),
        };
        results.push(ActionResult {
            action: action.clone(),
            outcome,
            error,
            src: src.clone(),
            dst: dst.clone(),
            bytes: bytes_copied,
            duration_ms,
            verified,
        });
        completed += 1;
        let _ = tx.send(WorkerEvent::SyncProgress {
            completed,
            total,
            bytes,
        });
        if cancel_after_current.load(Ordering::Relaxed) {
            break;
        }
    }
    tx.send(WorkerEvent::SyncDone { results })?;
    Ok(())
}

fn copy_and_verify(
    src: &Path,
    dst: &Path,
    compare: CompareMode,
    retries: u32,
    dry_run: bool,
    tx: &Sender<WorkerEvent>,
) -> Result<CopyOutcome> {
    let mut last_err = None;
    for attempt in 0..=retries {
        match copy_once(src, dst, compare, dry_run, tx) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                last_err = Some(err);
                if attempt < retries {
                    thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("copy failed")))
}

fn copy_once(
    src: &Path,
    dst: &Path,
    _compare: CompareMode,
    dry_run: bool,
    tx: &Sender<WorkerEvent>,
) -> Result<CopyOutcome> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let metadata =
        fs::symlink_metadata(src).with_context(|| format!("metadata {}", src.display()))?;
    let is_symlink = metadata.file_type().is_symlink();
    let size = if is_symlink { 0 } else { metadata.len() };

    if dry_run {
        tx.send(WorkerEvent::SyncFileProgress {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            copied: size,
            total: size,
        })
        .ok();
        return Ok(CopyOutcome {
            bytes: size,
            verified: false,
        });
    }

    if is_symlink {
        copy_symlink(src, dst)?;
        return Ok(CopyOutcome {
            bytes: 0,
            verified: true,
        });
    }

    // Pre-check free space on destination filesystem
    let dst_dir = dst.parent().unwrap_or(dst);
    let free = fs2::available_space(dst_dir).unwrap_or(0);
    if size > free {
        return Err(anyhow::anyhow!(
            "not enough space at {}: need {}, have {}",
            dst_dir.display(),
            format_bytes(size),
            format_bytes(free)
        ));
    }

    let src_hash = copy_with_progress(src, dst, size, tx)?;
    let dst_size = fs::metadata(dst)
        .with_context(|| format!("metadata {}", dst.display()))?
        .len();
    if size != dst_size {
        return Err(anyhow::anyhow!("size mismatch after copy"));
    }
    tx.send(WorkerEvent::Verifying).ok();
    let dst_hash = hash_file_progress(dst, tx)?;
    if src_hash != dst_hash {
        return Err(anyhow::anyhow!("hash mismatch after copy"));
    }
    fs::set_permissions(dst, metadata.permissions())
        .with_context(|| format!("permissions {}", dst.display()))?;
    if let Ok(mtime) = metadata.modified() {
        let ft = FileTime::from_system_time(mtime);
        filetime::set_file_mtime(dst, ft).with_context(|| format!("mtime {}", dst.display()))?;
    }
    Ok(CopyOutcome {
        bytes: size,
        verified: true,
    })
}

fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
    let target = fs::read_link(src).with_context(|| format!("read link {}", src.display()))?;
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&target, dst)
            .with_context(|| format!("symlink {}", dst.display()))?;
        return Ok(());
    }
    #[cfg(windows)]
    {
        if target.is_dir() {
            std::os::windows::fs::symlink_dir(&target, dst)
                .with_context(|| format!("symlink {}", dst.display()))?;
        } else {
            std::os::windows::fs::symlink_file(&target, dst)
                .with_context(|| format!("symlink {}", dst.display()))?;
        }
        return Ok(());
    }
    #[allow(unreachable_code)]
    Ok(())
}

fn delete_with_retry(
    path: &Path,
    retries: u32,
    dry_run: bool,
    tx: &Sender<WorkerEvent>,
) -> Result<CopyOutcome> {
    let mut last_err = None;
    for attempt in 0..=retries {
        match delete_once(path, dry_run, tx) {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                last_err = Some(err);
                if attempt < retries {
                    thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("delete failed")))
}

fn delete_once(path: &Path, dry_run: bool, tx: &Sender<WorkerEvent>) -> Result<CopyOutcome> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("metadata {}", path.display()))?;
    let size = metadata.len();
    tx.send(WorkerEvent::SyncFileProgress {
        src: path.to_path_buf(),
        dst: path.to_path_buf(),
        copied: size,
        total: size,
    })
    .ok();
    if dry_run {
        return Ok(CopyOutcome {
            bytes: size,
            verified: false,
        });
    }
    if metadata.is_dir() {
        fs::remove_dir_all(path).with_context(|| format!("remove dir {}", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
    }
    Ok(CopyOutcome {
        bytes: size,
        verified: true,
    })
}

fn copy_with_progress(
    src: &Path,
    dst: &Path,
    total: u64,
    tx: &Sender<WorkerEvent>,
) -> Result<String> {
    let mut reader =
        io::BufReader::new(fs::File::open(src).with_context(|| format!("open {}", src.display()))?);
    let mut writer = io::BufWriter::new(
        fs::File::create(dst).with_context(|| format!("create {}", dst.display()))?,
    );
    let mut buf = vec![0u8; 1024 * 1024];
    let mut copied = 0u64;
    let mut hasher = Hasher::new();
    tx.send(WorkerEvent::SyncFileProgress {
        src: src.to_path_buf(),
        dst: dst.to_path_buf(),
        copied,
        total,
    })
    .ok();
    loop {
        let read = reader
            .read(&mut buf)
            .with_context(|| format!("read {}", src.display()))?;
        if read == 0 {
            break;
        }
        writer
            .write_all(&buf[..read])
            .with_context(|| format!("write {}", dst.display()))?;
        hasher.update(&buf[..read]);
        copied += read as u64;
        tx.send(WorkerEvent::SyncFileProgress {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            copied,
            total,
        })
        .ok();
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", dst.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn format_bytes_per_sec(bps: f64) -> String {
    if bps <= 0.0 {
        return "0 B".to_string();
    }
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    if bps >= GB {
        format!("{:.2} GB", bps / GB)
    } else if bps >= MB {
        format!("{:.2} MB", bps / MB)
    } else if bps >= KB {
        format!("{:.2} KB", bps / KB)
    } else {
        format!("{:.0} B", bps)
    }
}

fn progress_bar(current: u64, total: u64, width: usize) -> String {
    if total == 0 || width == 0 {
        return "[----------]".to_string();
    }
    let ratio = (current as f64 / total as f64).min(1.0);
    let filled = (ratio * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "#".repeat(filled), "-".repeat(empty))
}

fn eta_seconds(done: u64, total: u64, speed_bps: f64) -> u64 {
    if speed_bps <= 0.0 || total == 0 || done >= total {
        return 0;
    }
    let remaining = total - done;
    (remaining as f64 / speed_bps).ceil() as u64
}

fn format_eta(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{}h {:02}m {:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m {:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}

fn space_info(path: &Path) -> (u64, u64) {
    let total = fs2::total_space(path).unwrap_or(0);
    let free = fs2::available_space(path).unwrap_or(0);
    (free, total)
}

fn build_exclude_set(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

fn should_exclude(rel: &Path, exclude: &GlobSet) -> bool {
    exclude.is_match(rel)
}

fn load_last_run_diffs(
    conn: &Connection,
    left_root: &Path,
    right_root: &Path,
) -> Result<HashMap<PathBuf, LastEntry>> {
    let mut map = HashMap::new();
    let run_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM runs WHERE left_root = ?1 AND right_root = ?2 ORDER BY id DESC LIMIT 1",
            params![
                left_root.display().to_string(),
                right_root.display().to_string()
            ],
            |row| row.get(0),
        )
        .optional()?;
    let Some(run_id) = run_id else {
        return Ok(map);
    };

    let mut stmt = conn.prepare(
        "SELECT path_rel, size_left, size_right, mtime_left, mtime_right, hash_left, hash_right FROM diffs WHERE run_id = ?1",
    )?;
    let rows = stmt.query_map(params![run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            LastEntry {
                size_left: row.get(1)?,
                size_right: row.get(2)?,
                mtime_left: row.get(3)?,
                mtime_right: row.get(4)?,
                hash_left: row.get(5)?,
                hash_right: row.get(6)?,
            },
        ))
    })?;
    for row in rows {
        let (path, entry) = row?;
        map.insert(PathBuf::from(path), entry);
    }
    Ok(map)
}

fn load_history(conn: &Connection) -> Result<Vec<HistoryEntry>> {
    let mut entries = Vec::new();
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
    let mut temp = Vec::new();
    for row in rows {
        temp.push(row?);
    }
    for mut entry in temp {
        entry.actions = conn
            .query_row(
                "SELECT COUNT(*) FROM actions WHERE run_id = ?1",
                params![entry.run_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        entry.errors = conn
            .query_row(
                "SELECT COUNT(*) FROM errors WHERE run_id = ?1",
                params![entry.run_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        entries.push(entry);
    }
    Ok(entries)
}

fn init_db(path: &Path) -> Result<Connection> {
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

fn add_column_if_missing(
    conn: &Connection,
    columns: &BTreeSet<String>,
    name: &str,
    col_type: &str,
) -> Result<()> {
    if columns.contains(name) {
        return Ok(());
    }
    let sql = format!("ALTER TABLE actions ADD COLUMN {} {}", name, col_type);
    conn.execute(&sql, [])?;
    Ok(())
}

fn insert_run_start(conn: &Connection, left: &Path, right: &Path) -> Result<i64> {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO runs (started_at, left_root, right_root, status) VALUES (?1, ?2, ?3, ?4)",
        params![
            now,
            left.display().to_string(),
            right.display().to_string(),
            "running"
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn finalize_run(conn: &Connection, run_id: i64, status: &str) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "UPDATE runs SET completed_at = ?1, status = ?2 WHERE id = ?3",
        params![now, status, run_id],
    )?;
    Ok(())
}

fn insert_error(conn: &Connection, run_id: i64, message: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO errors (run_id, message) VALUES (?1, ?2)",
        params![run_id, message],
    )?;
    Ok(())
}

fn insert_diffs(conn: &Connection, run_id: i64, diffs: &[DiffEntry]) -> Result<()> {
    for diff in diffs {
        let status = match diff.status {
            DiffStatus::Same => "same",
            DiffStatus::MissingLeft => "missing-left",
            DiffStatus::MissingRight => "missing-right",
            DiffStatus::Mismatch => "mismatch",
            DiffStatus::Conflict => "conflict",
        };
        let (size_left, mtime_left, hash_left) = diff
            .left
            .as_ref()
            .map(|m| (Some(m.size), Some(m.mtime), m.hash.clone()))
            .unwrap_or((None, None, None));
        let (size_right, mtime_right, hash_right) = diff
            .right
            .as_ref()
            .map(|m| (Some(m.size), Some(m.mtime), m.hash.clone()))
            .unwrap_or((None, None, None));
        conn.execute(
            "INSERT INTO diffs (run_id, path_rel, status, size_left, size_right, mtime_left, mtime_right, hash_left, hash_right) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                run_id,
                diff.path_rel.display().to_string(),
                status,
                size_left,
                size_right,
                mtime_left,
                mtime_right,
                hash_left,
                hash_right,
            ],
        )?;
    }
    Ok(())
}

fn insert_action_result(conn: &Connection, run_id: i64, result: &ActionResult) -> Result<()> {
    let action_type = match result.action.action_type {
        ActionType::CopyLeftToRight => "copy-left-to-right",
        ActionType::CopyRightToLeft => "copy-right-to-left",
        ActionType::DeleteLeft => "delete-left",
        ActionType::DeleteRight => "delete-right",
    };
    conn.execute(
        "INSERT INTO actions (run_id, path_rel, action_type, reason, outcome, error, src_path, dst_path, bytes, duration_ms, verified) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            run_id,
            result.action.path_rel.display().to_string(),
            action_type,
            result.action.reason,
            result.outcome,
            result.error,
            result.src.display().to_string(),
            result.dst.display().to_string(),
            result.bytes,
            result.duration_ms,
            if result.verified { 1 } else { 0 },
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn diff_detects_missing_and_mismatch() {
        let mut left = BTreeMap::new();
        let mut right = BTreeMap::new();
        left.insert(
            PathBuf::from("a.txt"),
            FileMeta {
                size: 10,
                mtime: 1,
                hash: None,
                is_symlink: false,
                link_target: None,
            },
        );
        right.insert(
            PathBuf::from("b.txt"),
            FileMeta {
                size: 10,
                mtime: 1,
                hash: None,
                is_symlink: false,
                link_target: None,
            },
        );
        left.insert(
            PathBuf::from("c.txt"),
            FileMeta {
                size: 10,
                mtime: 1,
                hash: None,
                is_symlink: false,
                link_target: None,
            },
        );
        right.insert(
            PathBuf::from("c.txt"),
            FileMeta {
                size: 12,
                mtime: 1,
                hash: None,
                is_symlink: false,
                link_target: None,
            },
        );

        let diffs = compute_diffs(&left, &right, CompareMode::Size, &HashMap::new());
        let missing_left = diffs.iter().any(|d| d.status == DiffStatus::MissingLeft);
        let missing_right = diffs.iter().any(|d| d.status == DiffStatus::MissingRight);
        let mismatch = diffs.iter().any(|d| d.status == DiffStatus::Mismatch);
        assert!(missing_left);
        assert!(missing_right);
        assert!(mismatch);
    }

    #[test]
    fn plan_actions_uses_strategy() {
        let diffs = vec![DiffEntry {
            path_rel: PathBuf::from("x.txt"),
            left: Some(FileMeta {
                size: 1,
                mtime: 5,
                hash: None,
                is_symlink: false,
                link_target: None,
            }),
            right: Some(FileMeta {
                size: 1,
                mtime: 1,
                hash: None,
                is_symlink: false,
                link_target: None,
            }),
            status: DiffStatus::Mismatch,
        }];
        let actions = plan_actions(
            &diffs,
            MismatchStrategy::NewerMtime,
            &HashMap::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        );
        assert_eq!(actions.len(), 1);
        match actions[0].action_type {
            ActionType::CopyLeftToRight => {}
            _ => panic!("expected left to right"),
        }
    }

    #[test]
    fn copy_and_verify_copies_file() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        fs::write(&src, b"hello").unwrap();
        let (tx, _rx) = mpsc::channel();
        let result = copy_and_verify(&src, &dst, CompareMode::Size, 0, false, &tx).unwrap();
        assert_eq!(result.bytes, 5);
        assert!(dst.exists());
    }
}
