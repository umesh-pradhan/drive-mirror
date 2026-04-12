use crate::input::{
    handle_confirm_delete_input, handle_confirm_input, handle_history_input,
    handle_review_input, handle_strategy_input, AppArgs,
};
use crate::render::{render_frame, reveal_in_file_manager};
use drive_mirror_core::db::{
    finalize_run, insert_action_result, insert_diffs, insert_error, load_history,
    load_last_run_diffs,
};
use drive_mirror_core::models::{
    recompute_filtered_indices, AppState, Filter, Phase, Side, WorkerEvent,
};
use drive_mirror_core::scanner::{build_exclude_set, compute_diffs, scan_worker};
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

pub fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    rx: &Receiver<WorkerEvent>,
    tx: Sender<WorkerEvent>,
    state: &mut AppState,
    conn: &Connection,
    run_id: i64,
    args: &AppArgs,
    last_diffs: &HashMap<PathBuf, drive_mirror_core::models::LastEntry>,
) -> Result<()> {
    loop {
        while let Ok(event) = rx.try_recv() {
            match event {
                WorkerEvent::ScanProgress { side, count } => match side {
                    Side::Left => state.scanned_left = count,
                    Side::Right => state.scanned_right = count,
                },
                WorkerEvent::ScanDone { left, right, errors } => {
                    for error in &errors { insert_error(conn, run_id, error)?; }
                    state.diffs = compute_diffs(&left, &right, args.compare, last_diffs);
                    if state.sort_by_name { state.diffs.sort_by(|a, b| a.path_rel.cmp(&b.path_rel)); }
                    state.selected = 0;
                    state.selected_items.clear();
                    state.action_overrides.clear();
                    recompute_filtered_indices(state);
                    insert_diffs(conn, run_id, &state.diffs)?;
                    state.phase = Phase::Review;
                    state.status_line = "Scan complete. Review differences.".to_string();
                }
                WorkerEvent::SyncProgress { completed, total, bytes } => {
                    state.verifying = false;
                    state.sync_completed = completed;
                    state.sync_total = total;
                    state.sync_bytes = bytes;
                    if state.sync_start.is_none() { state.sync_start = Some(Instant::now()); }
                    if let Some(start) = state.sync_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 { state.sync_speed_bps = bytes as f64 / elapsed; }
                    }
                }
                WorkerEvent::Verifying => {
                    state.verifying = true;
                    state.verify_done = 0; state.verify_total = 0;
                    state.verify_start = Some(Instant::now());
                    state.verify_speed_bps = 0.0;
                }
                WorkerEvent::SyncFileProgress { src, dst, copied, total } => {
                    state.verifying = false;
                    if state.current_src.as_ref() != Some(&src) { state.current_start = Some(Instant::now()); }
                    state.current_src = Some(src);
                    state.current_dst = Some(dst.clone());
                    state.current_copied = copied;
                    state.current_total = total;
                    if total > 0 && copied >= total {
                        state.last_copied_dst = Some(state.current_dst.clone().unwrap_or(dst));
                        if let Some(ref p) = state.last_copied_dst { state.status_line = format!("Copied: {}", p.display()); }
                    }
                    if let Some(start) = state.current_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 { state.current_speed_bps = copied as f64 / elapsed; }
                    }
                }
                WorkerEvent::VerifyProgress { done, total } => {
                    state.verify_done = done; state.verify_total = total;
                    if state.verify_start.is_none() { state.verify_start = Some(Instant::now()); }
                    if let Some(start) = state.verify_start {
                        let elapsed = start.elapsed().as_secs_f64();
                        if elapsed > 0.0 { state.verify_speed_bps = done as f64 / elapsed; }
                    }
                }
                WorkerEvent::SyncDone { results } => {
                    for result in &results { insert_action_result(conn, run_id, result)?; }
                    for r in &results {
                        if r.error.is_none() && (r.outcome == "ok" || r.outcome == "dry-run") {
                            state.copied_recently.insert(r.action.path_rel.clone());
                        }
                    }
                    state.last_results = results;
                    state.phase = Phase::Done;
                    let base = if args.dry_run { "Dry-run complete" } else { "Sync complete" };
                    let copied_count = state.copied_recently.len();
                    state.status_line = match state.last_copied_dst.as_ref() {
                        Some(path) => format!("{}. Last copied: {}. {} item(s) excluded from next sync.", base, path.display(), copied_count),
                        None => format!("{}. {} item(s) excluded from next sync.", base, copied_count),
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

        let should_draw = state.dirty || state.last_draw.elapsed() >= Duration::from_millis(100);
        if should_draw {
            terminal.draw(|frame| render_frame(frame, state, args))?;
            state.last_draw = Instant::now();
            state.dirty = false;
        }

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press { continue; }
                state.dirty = true;

                // Palette intercepts all keys when open
                if state.palette_open {
                    handle_review_input(state, key.code, key.modifiers);
                    continue;
                }

                if key.code == KeyCode::Esc {
                    let now = Instant::now();
                    if state.phase == Phase::Review && state.filter != Filter::All {
                        state.filter = Filter::All;
                        recompute_filtered_indices(state);
                        state.selected_items.clear();
                        state.status_line = "Filter cleared (All).".to_string();
                        continue;
                    }
                    if let Some(last) = state.last_esc {
                        if now.duration_since(last) <= Duration::from_millis(600) { break; }
                    }
                    state.last_esc = Some(now);
                    state.status_line = "Press Esc again to quit.".to_string();
                    match state.phase {
                        Phase::ConfirmSync | Phase::ConfirmDelete | Phase::ChoosingStrategy | Phase::Syncing | Phase::Done | Phase::History => {
                            state.phase = Phase::Review;
                            state.status_line = "Back to review.".to_string();
                        }
                        _ => {}
                    }
                    continue;
                }

                if key.code == KeyCode::Char('h') && state.phase != Phase::Syncing {
                    state.history = load_history(conn)?;
                    state.history_selected = 0;
                    state.phase = Phase::History;
                    state.status_line = "History loaded.".to_string();
                    continue;
                }
                if key.code == KeyCode::Char('o') {
                    if let Some(path) = state.last_copied_dst.as_ref() {
                        if let Err(err) = reveal_in_file_manager(path) { state.status_line = format!("Open failed: {}", err); }
                        else { state.status_line = "Revealed in file manager.".to_string(); }
                    } else { state.status_line = "No copied file yet.".to_string(); }
                    continue;
                }
                if let KeyCode::F(5) = key.code {
                    state.phase = Phase::Scanning;
                    state.scanned_left = 0; state.scanned_right = 0;
                    state.status_line = "Refreshing...".to_string();
                    let left_root = args.left.clone();
                    let right_root = args.right.clone();
                    let compare_mode = args.compare;
                    let scan_tx = tx.clone();
                    let exclude = build_exclude_set(&args.exclude)?;
                    let last = load_last_run_diffs(conn, &args.left, &args.right)?;
                    thread::spawn(move || { let _ = scan_worker(left_root, right_root, compare_mode, scan_tx, exclude, last); });
                    continue;
                }

                match state.phase {
                    Phase::Review => handle_review_input(state, key.code, key.modifiers),
                    Phase::History => handle_history_input(state, key.code),
                    Phase::ChoosingStrategy => handle_strategy_input(state, key.code),
                    Phase::ConfirmSync => handle_confirm_input(state, key.code, args, conn, run_id, &tx)?,
                    Phase::ConfirmDelete => handle_confirm_delete_input(state, key.code, args, conn, run_id, &tx)?,
                    Phase::Done => { if key.code == KeyCode::Char('q') { break; } }
                    Phase::Scanning => { if key.code == KeyCode::Char('q') { state.status_line = "Exiting...".to_string(); break; } }
                    Phase::Syncing => {
                        if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                            state.cancel_after_current.store(true, Ordering::Relaxed);
                            state.status_line = "Will stop after current file finishes...".to_string();
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
