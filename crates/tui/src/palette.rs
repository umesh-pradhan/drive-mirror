use drive_mirror_core::models::{AppState, Filter, Phase, SyncScope};
use drive_mirror_core::models::recompute_filtered_indices;
use crossterm::event::KeyCode;

pub static COMMANDS: &[(&str, &str)] = &[
    ("sync",          "Sync selected item"),
    ("sync-all",      "Sync all filtered items"),
    ("delete",        "Delete selected extras"),
    ("missing-left",  "Filter: show missing-left"),
    ("missing-right", "Filter: show missing-right"),
    ("mismatch",      "Filter: show mismatches"),
    ("conflict",      "Filter: show conflicts"),
    ("all",           "Filter: show all"),
    ("history",       "Open history"),
    ("refresh",       "Rescan directories"),
    ("quit",          "Quit application"),
];

pub fn filter_commands(query: &str) -> Vec<&'static (&'static str, &'static str)> {
    let q = query.to_lowercase();
    COMMANDS.iter().filter(|(name, desc)| {
        q.is_empty() || name.contains(q.as_str()) || desc.to_lowercase().contains(q.as_str())
    }).collect()
}

pub fn handle_palette_input(state: &mut AppState, code: KeyCode) {
    match code {
        KeyCode::Esc => { state.close_palette(); }
        KeyCode::Enter => {
            let matches = filter_commands(&state.palette_query);
            if let Some(&&(cmd, _)) = matches.get(state.palette_selected) {
                let cmd = cmd.to_string();
                state.close_palette();
                execute_palette_command(state, &cmd);
            }
        }
        KeyCode::Up => {
            if state.palette_selected > 0 { state.palette_selected -= 1; }
        }
        KeyCode::Down => {
            let max = filter_commands(&state.palette_query).len().saturating_sub(1);
            if state.palette_selected < max { state.palette_selected += 1; }
        }
        KeyCode::Backspace => { state.palette_query.pop(); state.palette_selected = 0; }
        KeyCode::Char(c) => { state.palette_query.push(c); state.palette_selected = 0; }
        _ => {}
    }
}

pub fn execute_palette_command(state: &mut AppState, cmd: &str) {
    match cmd {
        "sync" => {
            if !state.diffs.is_empty() {
                state.sync_scope = SyncScope::Selected;
                state.phase = Phase::ConfirmSync;
                state.status_line = "Confirm sync.".to_string();
            }
        }
        "sync-all" => {
            state.sync_scope = SyncScope::All;
            state.selected_items.clear();
            for &idx in &state.filtered_indices { state.selected_items.insert(idx); }
            state.phase = Phase::ConfirmSync;
            state.status_line = "Confirm sync all.".to_string();
        }
        "delete" => {
            state.phase = Phase::ConfirmDelete;
            state.status_line = "Confirm delete? y/Enter=yes  n/b=back".to_string();
        }
        "missing-left" => { state.filter = Filter::MissingLeft; state.selected = 0; recompute_filtered_indices(state); state.status_line = "Filter: Missing Left".to_string(); }
        "missing-right" => { state.filter = Filter::MissingRight; state.selected = 0; recompute_filtered_indices(state); state.status_line = "Filter: Missing Right".to_string(); }
        "mismatch" => { state.filter = Filter::Mismatch; state.selected = 0; recompute_filtered_indices(state); state.status_line = "Filter: Mismatch".to_string(); }
        "conflict" => { state.filter = Filter::Conflict; state.selected = 0; recompute_filtered_indices(state); state.status_line = "Filter: Conflict".to_string(); }
        "all" => { state.filter = Filter::All; state.selected = 0; recompute_filtered_indices(state); state.status_line = "Filter: All".to_string(); }
        "history" => { state.phase = Phase::History; state.status_line = "History.".to_string(); }
        "refresh" => { state.phase = Phase::Scanning; state.status_line = "Refreshing...".to_string(); }
        "quit" => { state.phase = Phase::Done; state.status_line = "Quit.".to_string(); }
        _ => {}
    }
}
