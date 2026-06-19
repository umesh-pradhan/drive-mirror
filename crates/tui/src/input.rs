use crate::palette::handle_palette_input;
use drive_mirror_core::db::finalize_run;
use drive_mirror_core::models::{
    recompute_filtered_indices, ActionType, AppState, DiffStatus, Filter, MismatchStrategy, Phase,
    SyncScope, WorkerEvent,
};
use drive_mirror_core::planner::plan_actions;
use drive_mirror_core::sync::sync_worker;
use anyhow::Result;
use crossterm::event::{KeyCode, KeyModifiers};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::thread;

pub struct AppArgs {
    pub left: PathBuf,
    pub right: PathBuf,
    pub compare: drive_mirror_core::models::CompareMode,
    pub exclude: Vec<String>,
    pub retries: u32,
    pub dry_run: bool,
}

pub fn apply_override(state: &mut AppState, action: ActionType) {
    if state.diffs.is_empty() { return; }
    let targets: Vec<usize> = if state.selected_items.is_empty() {
        state.filtered_indices.get(state.selected).copied().into_iter().collect()
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

pub fn apply_delete_override(state: &mut AppState) {
    if state.diffs.is_empty() { return; }
    let targets: Vec<usize> = if state.selected_items.is_empty() {
        state.filtered_indices.get(state.selected).copied().into_iter().collect()
    } else {
        state.selected_items.iter().copied().collect()
    };
    let mut applied = false;
    for idx in targets {
        if let Some(diff) = state.diffs.get(idx) {
            match diff.status {
                DiffStatus::MissingLeft => { state.action_overrides.insert(diff.path_rel.clone(), ActionType::DeleteRight); applied = true; }
                DiffStatus::MissingRight => { state.action_overrides.insert(diff.path_rel.clone(), ActionType::DeleteLeft); applied = true; }
                _ => {}
            }
        }
    }
    state.status_line = if applied { "Override: delete selected extras.".to_string() } else { "Delete applies only to missing entries.".to_string() };
}

pub fn handle_review_input(state: &mut AppState, code: KeyCode, modifiers: KeyModifiers) {
    if state.palette_open {
        handle_palette_input(state, code);
        return;
    }
    let len = state.filtered_indices.len();
    if modifiers.contains(KeyModifiers::SHIFT) {
        match code {
            KeyCode::Down => {
                if !state.diffs.is_empty() {
                    let idx = *state.filtered_indices.get(state.selected).unwrap_or(&0);
                    state.selected_items.insert(idx);
                    if len > 0 { state.selected = (state.selected + 1).min(len - 1); }
                }
                return;
            }
            KeyCode::Up => {
                if !state.diffs.is_empty() {
                    if let Some(idx) = state.filtered_indices.get(state.selected) { state.selected_items.remove(idx); }
                    state.selected = state.selected.saturating_sub(1);
                }
                return;
            }
            _ => {}
        }
    }
    match code {
        KeyCode::Down => { if len > 0 { state.selected = (state.selected + 1).min(len - 1); } }
        KeyCode::Up => { state.selected = state.selected.saturating_sub(1); }
        KeyCode::Char('/') => { state.palette_open = true; state.palette_query.clear(); state.palette_selected = 0; }
        KeyCode::Char('s') => {
            state.sync_scope = SyncScope::All;
            state.selected_items.clear();
            for &idx in &state.filtered_indices { state.selected_items.insert(idx); }
            state.phase = Phase::ChoosingStrategy;
            state.status_line = "Choose sync strategy.".to_string();
        }
        KeyCode::Enter => {
            if state.diffs.is_empty() { return; }
            state.sync_scope = SyncScope::Selected;
            state.phase = Phase::ChoosingStrategy;
            state.status_line = "Choose sync strategy.".to_string();
        }
        KeyCode::Char('1') => { state.filter = Filter::All; recompute_filtered_indices(state); }
        KeyCode::Char('2') => { state.filter = Filter::MissingLeft; recompute_filtered_indices(state); }
        KeyCode::Char('3') => { state.filter = Filter::MissingRight; recompute_filtered_indices(state); }
        KeyCode::Char('4') => { state.filter = Filter::Mismatch; recompute_filtered_indices(state); }
        KeyCode::Char('5') => { state.filter = Filter::Conflict; recompute_filtered_indices(state); }
        KeyCode::Char('q') => { state.phase = Phase::Done; state.status_line = "Quit.".to_string(); }
        KeyCode::Char(' ') => {
            if state.diffs.is_empty() { return; }
            if let Some(&idx) = state.filtered_indices.get(state.selected) {
                if state.selected_items.contains(&idx) { state.selected_items.remove(&idx); }
                else { state.selected_items.insert(idx); }
            }
        }
        KeyCode::Char('a') => { state.selected_items = (0..state.diffs.len()).collect(); state.status_line = "Selected all.".to_string(); }
        KeyCode::Char('c') => { state.selected_items.clear(); state.status_line = "Selection cleared.".to_string(); }
        KeyCode::Char('l') => apply_override(state, ActionType::CopyLeftToRight),
        KeyCode::Char('r') => apply_override(state, ActionType::CopyRightToLeft),
        KeyCode::Char('f') => {
            let targets: Vec<usize> = if state.selected_items.is_empty() { vec![state.selected] }
                else { state.selected_items.iter().copied().collect() };
            for idx in targets {
                if let Some(diff) = state.diffs.get(idx) {
                    if state.force_recopy.contains(&diff.path_rel) { state.force_recopy.remove(&diff.path_rel); }
                    else { state.force_recopy.insert(diff.path_rel.clone()); }
                }
            }
            state.status_line = "Toggled force recopy.".to_string();
        }
        KeyCode::Char('d') | KeyCode::Delete => { state.phase = Phase::ConfirmDelete; state.status_line = "Confirm delete? y/Enter=yes  n/b=back".to_string(); }
        KeyCode::Char('n') => {
            state.sort_by_name = !state.sort_by_name;
            if state.sort_by_name { state.diffs.sort_by(|a, b| a.path_rel.cmp(&b.path_rel)); state.status_line = "Sorted by name.".to_string(); }
            else { state.status_line = "Name sort off.".to_string(); }
        }
        _ => {}
    }
}

pub fn handle_history_input(state: &mut AppState, code: KeyCode) {
    let len = state.history.len();
    match code {
        KeyCode::Down => { if len > 0 { state.history_selected = (state.history_selected + 1).min(len - 1); } }
        KeyCode::Up => { state.history_selected = state.history_selected.saturating_sub(1); }
        _ => {}
    }
}

pub fn handle_strategy_input(state: &mut AppState, code: KeyCode) {
    if code == KeyCode::Char('b') { state.phase = Phase::Review; state.status_line = "Back to review.".to_string(); return; }
    state.mismatch_strategy = match code {
        KeyCode::Char('n') => Some(MismatchStrategy::NewerMtime),
        KeyCode::Char('l') => Some(MismatchStrategy::PreferLeft),
        KeyCode::Char('r') => Some(MismatchStrategy::PreferRight),
        KeyCode::Char('k') => Some(MismatchStrategy::Skip),
        KeyCode::Char('e') => Some(MismatchStrategy::ExactLeftToRight),
        KeyCode::Char('x') => Some(MismatchStrategy::ExactRightToLeft),
        _ => state.mismatch_strategy,
    };
    if state.mismatch_strategy.is_some() { state.phase = Phase::ConfirmSync; state.status_line = "Confirm sync.".to_string(); }
}

pub fn handle_confirm_input(state: &mut AppState, code: KeyCode, args: &AppArgs, conn: &Connection, run_id: i64, tx: &Sender<WorkerEvent>) -> Result<()> {
    if code == KeyCode::Char('b') { state.phase = Phase::Review; state.status_line = "Back to review.".to_string(); return Ok(()); }
    match code {
        KeyCode::Char('y') | KeyCode::Enter => {
            let strategy = state.mismatch_strategy.unwrap_or(MismatchStrategy::NewerMtime);
            let diffs = match state.sync_scope {
                SyncScope::All => {
                    if state.filtered_indices.is_empty() { state.diffs.clone() }
                    else { state.filtered_indices.iter().filter_map(|&i| state.diffs.get(i).cloned()).collect() }
                }
                SyncScope::Selected => {
                    if state.selected_items.is_empty() {
                        if let Some(&idx) = state.filtered_indices.get(state.selected) { state.diffs.get(idx).cloned().into_iter().collect() }
                        else { state.diffs.get(state.selected).cloned().into_iter().collect() }
                    } else {
                        state.selected_items.iter().filter_map(|i| state.diffs.get(*i).cloned()).collect()
                    }
                }
            };
            let all_actions = plan_actions(&diffs, strategy, &state.action_overrides, &state.copied_recently, &state.force_recopy);
            if all_actions.is_empty() {
                state.phase = Phase::Done;
                state.status_line = if diffs.iter().any(|d| d.status == DiffStatus::Conflict) {
                    "No actions. Conflicts need override (l/r).".to_string()
                } else { "No actions to apply.".to_string() };
                finalize_run(conn, run_id, "done")?;
                return Ok(());
            }

            // For exact strategies: split copies and deletes; copies run first, then confirm deletes
            let is_exact = matches!(strategy, MismatchStrategy::ExactLeftToRight | MismatchStrategy::ExactRightToLeft);
            if is_exact {
                let (copy_actions, delete_actions): (Vec<_>, Vec<_>) = all_actions.into_iter()
                    .partition(|a| matches!(a.action_type, ActionType::CopyLeftToRight | ActionType::CopyRightToLeft));
                state.pending_delete_actions = delete_actions;
                if copy_actions.is_empty() {
                    // No copies needed, go straight to delete confirmation if there are deletes
                    if state.pending_delete_actions.is_empty() {
                        state.phase = Phase::Done;
                        state.status_line = "No actions to apply.".to_string();
                        finalize_run(conn, run_id, "done")?;
                    } else {
                        state.phase = Phase::ConfirmExactDelete;
                        state.status_line = format!("{} file(s) will be deleted. Confirm?", state.pending_delete_actions.len());
                    }
                    return Ok(());
                }
                state.pending_actions = copy_actions.clone();
                spawn_sync(copy_actions, args, state, tx);
            } else {
                state.pending_delete_actions.clear();
                state.pending_actions = all_actions.clone();
                spawn_sync(all_actions, args, state, tx);
            }
        }
        KeyCode::Char('n') => { state.phase = Phase::Review; state.status_line = "Sync canceled.".to_string(); }
        _ => {}
    }
    Ok(())
}

fn spawn_sync(actions: Vec<drive_mirror_core::models::Action>, args: &AppArgs, state: &mut AppState, tx: &Sender<WorkerEvent>) {
    let left = args.left.clone();
    let right = args.right.clone();
    let compare = args.compare;
    let retries = args.retries;
    let dry_run = args.dry_run;
    let sync_tx = tx.clone();
    let cancel_flag = state.cancel_after_current.clone();
    thread::spawn(move || {
        if let Err(err) = sync_worker(left, right, actions, compare, retries, dry_run, cancel_flag, sync_tx.clone()) {
            let _ = sync_tx.send(WorkerEvent::Error(err.to_string()));
        }
    });
    state.phase = Phase::Syncing;
    state.status_line = "Sync in progress.".to_string();
    state.sync_start = None; state.sync_speed_bps = 0.0;
    state.current_src = None; state.current_dst = None;
    state.current_copied = 0; state.current_total = 0;
    state.current_start = None; state.current_speed_bps = 0.0;
}

pub fn handle_confirm_exact_delete_input(state: &mut AppState, code: KeyCode, args: &AppArgs, conn: &Connection, run_id: i64, tx: &Sender<WorkerEvent>) -> Result<()> {
    match code {
        KeyCode::Char('y') | KeyCode::Enter => {
            let deletes = std::mem::take(&mut state.pending_delete_actions);
            if deletes.is_empty() {
                state.phase = Phase::Done;
                finalize_run(conn, run_id, "done")?;
                return Ok(());
            }
            spawn_sync(deletes, args, state, tx);
        }
        KeyCode::Char('n') | KeyCode::Char('b') => {
            state.pending_delete_actions.clear();
            state.phase = Phase::Done;
            state.status_line = "Deletions skipped. Copies complete.".to_string();
            finalize_run(conn, run_id, "done")?;
        }
        _ => {}
    }
    Ok(())
}

pub fn handle_confirm_delete_input(state: &mut AppState, code: KeyCode, args: &AppArgs, conn: &Connection, run_id: i64, tx: &Sender<WorkerEvent>) -> Result<()> {
    match code {
        KeyCode::Char('y') | KeyCode::Enter => {
            apply_delete_override(state);
            state.sync_scope = SyncScope::Selected;
            handle_confirm_input(state, KeyCode::Enter, args, conn, run_id, tx)?;
        }
        KeyCode::Char('n') | KeyCode::Char('b') => { state.phase = Phase::Review; state.status_line = "Delete cancelled.".to_string(); }
        _ => {}
    }
    Ok(())
}
