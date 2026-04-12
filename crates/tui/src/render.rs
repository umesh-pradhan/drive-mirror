use crate::input::AppArgs;
use crate::palette::filter_commands;
use drive_mirror_core::models::{ActionType, AppState, DiffStatus, Filter, Phase};
use drive_mirror_core::scanner::{
    format_bytes, format_bytes_per_sec, format_eta, format_mtime, progress_bar, space_info,
};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;
use std::path::Path;
use std::process::Command;

pub fn render_frame(frame: &mut Frame, state: &mut AppState, args: &AppArgs) {
    let size = frame.size();
    frame.render_widget(Clear, size);

    let chunks = if state.palette_open {
        Layout::default().direction(Direction::Vertical)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(50), Constraint::Length(10), Constraint::Min(3)])
            .split(size)
    } else {
        Layout::default().direction(Direction::Vertical)
            .constraints([Constraint::Percentage(20), Constraint::Percentage(60), Constraint::Percentage(20)])
            .split(size)
    };

    // Header
    let title = if args.dry_run { format!("DriveMirror - {:?} (dry-run)", state.phase) }
    else { format!("DriveMirror - {:?}", state.phase) };
    let (lr_bytes, rl_bytes, miss_l, miss_r, mism) = compute_sync_overview(state);
    let hdr = format!("{} | L->R: {}  R->L: {} | missing-L: {}  missing-R: {}  mismatch: {}",
        title, format_bytes(lr_bytes), format_bytes(rl_bytes), miss_l, miss_r, mism);
    let (l_free, l_total) = space_info(&args.left);
    let (r_free, r_total) = space_info(&args.right);
    let filter_label = match state.filter {
        Filter::All => None,
        Filter::MissingLeft => Some("Filter: Missing Left"),
        Filter::MissingRight => Some("Filter: Missing Right"),
        Filter::Mismatch => Some("Filter: Mismatch"),
        Filter::Conflict => Some("Filter: Conflict"),
    };
    let hdr2 = if let Some(f) = filter_label {
        format!("{}\n\nL: {} free/{}    R: {} free/{}\n\n[ {} ]", hdr, format_bytes(l_free), format_bytes(l_total), format_bytes(r_free), format_bytes(r_total), f)
    } else {
        format!("{}\n\nL: {} free/{}    R: {} free/{}", hdr, format_bytes(l_free), format_bytes(l_total), format_bytes(r_free), format_bytes(r_total))
    };
    frame.render_widget(Paragraph::new(hdr2).block(Block::default().borders(Borders::ALL)).wrap(Wrap { trim: true }), chunks[0]);

    match state.phase {
        Phase::Scanning => {
            frame.render_widget(
                Paragraph::new(format!("Scanning... left: {} files, right: {} files", state.scanned_left, state.scanned_right))
                    .block(Block::default().borders(Borders::ALL).title("Progress")).wrap(Wrap { trim: true }),
                chunks[1],
            );
        }
        Phase::Review | Phase::ChoosingStrategy | Phase::ConfirmSync | Phase::ConfirmDelete => render_review(frame, state, args, chunks[1]),
        Phase::History => render_history(frame, state, chunks[1]),
        Phase::Syncing => render_syncing(frame, state, args, chunks[1]),
        Phase::Done => render_done(frame, state, chunks[1]),
    }

    if state.palette_open {
        render_palette(frame, state, chunks[2]);
        frame.render_widget(
            Paragraph::new(Line::from(vec![Span::raw("↑↓: move  Enter: execute  Esc: close")]))
                .block(Block::default().borders(Borders::ALL).title("Palette")),
            chunks[3],
        );
    } else {
        frame.render_widget(
            Paragraph::new(help_text(state)).block(Block::default().borders(Borders::ALL).title("Help")).wrap(Wrap { trim: true }),
            chunks[2],
        );
    }
}

fn render_review(frame: &mut Frame, state: &AppState, args: &AppArgs, area: Rect) {
    let body_chunks = Layout::default().direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)]).split(area);
    let width = body_chunks[0].width.saturating_sub(4) as usize;
    let height = body_chunks[0].height.saturating_sub(2) as usize;
    let total = state.filtered_indices.len();
    let selected_idx = state.selected.min(total.saturating_sub(1));
    let half = height / 2;
    let mut start = selected_idx.saturating_sub(half);
    if start + height > total { start = total.saturating_sub(height); }
    let end = (start + height).min(total);
    let list_cols = Layout::default().direction(Direction::Horizontal)
        .constraints([Constraint::Min(1), Constraint::Length(1)]).split(body_chunks[0]);
    let items: Vec<ListItem> = state.filtered_indices.iter().enumerate().skip(start).take(end.saturating_sub(start))
        .map(|(_, &orig_idx)| {
            let selected_mark = if state.selected_items.contains(&orig_idx) { "[*] " } else { "[ ] " };
            let diff = &state.diffs[orig_idx];
            let status = match diff.status {
                DiffStatus::Same => "same", DiffStatus::MissingLeft => "missing-left",
                DiffStatus::MissingRight => "missing-right", DiffStatus::Mismatch => "mismatch", DiffStatus::Conflict => "conflict",
            };
            let override_mark = state.action_overrides.get(&diff.path_rel).map(|a| match a {
                ActionType::CopyLeftToRight => " =>L", ActionType::CopyRightToLeft => " =>R",
                ActionType::DeleteLeft => " DEL-L", ActionType::DeleteRight => " DEL-R",
            }).unwrap_or("");
            let copied_mark = if state.copied_recently.contains(&diff.path_rel) { " ✓" } else { "" };
            let content = truncate_to_width(&format!("{}{} [{}]{}{}", selected_mark, diff.path_rel.display(), status, override_mark, copied_mark), width.max(10));
            let mut item = ListItem::new(vec![Line::from(content)]);
            if state.copied_recently.contains(&diff.path_rel) && !state.force_recopy.contains(&diff.path_rel) {
                item = item.style(Style::default().fg(Color::Green));
            }
            item
        }).collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("Differences"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD)).highlight_symbol("> ");
    let mut display_state = ListState::default();
    display_state.select(Some(selected_idx.saturating_sub(start)));
    frame.render_stateful_widget(list, list_cols[0], &mut display_state);
    render_scrollbar(frame, list_cols[1], start, height, total);
    frame.render_widget(
        Paragraph::new(details_text(state, args)).block(Block::default().borders(Borders::ALL).title("Details")).wrap(Wrap { trim: true }),
        body_chunks[1],
    );
}

fn render_history(frame: &mut Frame, state: &AppState, area: Rect) {
    let body_chunks = Layout::default().direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)]).split(area);
    let width = body_chunks[0].width.saturating_sub(4) as usize;
    let items: Vec<ListItem> = state.history.iter().map(|entry| {
        let line = format!("#{} {} [{}] actions:{} errors:{}", entry.run_id, entry.started_at, entry.status, entry.actions, entry.errors);
        ListItem::new(wrap_text(&line, width.max(10)).into_iter().map(Line::from).collect::<Vec<_>>())
    }).collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title("History"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD)).highlight_symbol("> ");
    let mut hs = ListState::default();
    hs.select(Some(state.history_selected));
    frame.render_stateful_widget(list, body_chunks[0], &mut hs);
    frame.render_widget(
        Paragraph::new(history_details_text(state)).block(Block::default().borders(Borders::ALL).title("Details")).wrap(Wrap { trim: true }),
        body_chunks[1],
    );
}

fn render_syncing(frame: &mut Frame, state: &AppState, args: &AppArgs, area: Rect) {
    let bar_width = area.width.saturating_sub(20) as usize;
    let overall_bar = progress_bar(state.sync_completed as u64, state.sync_total as u64, bar_width.max(10));
    let file_bar = progress_bar(state.current_copied, state.current_total, bar_width.max(10));
    let current_src = state.current_src.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "n/a".to_string());
    let current_dst = state.current_dst.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "n/a".to_string());
    let elapsed = state.sync_start.map(|s| s.elapsed().as_secs()).unwrap_or(0);
    let (l_free, l_total) = space_info(&args.left);
    let (r_free, r_total) = space_info(&args.right);
    frame.render_widget(
        Paragraph::new(format!(
            "Syncing... {}/{} files (elapsed {})\n{}\n{} copied, {}/s\nsrc: {}\ndst: {}\nfile: {}/{} @ {}/s\nfree L: {}/{}, free R: {}/{}\n{}",
            state.sync_completed, state.sync_total, format_eta(elapsed), overall_bar,
            format_bytes(state.sync_bytes), format_bytes_per_sec(state.sync_speed_bps),
            current_src, current_dst,
            format_bytes(state.current_copied), format_bytes(state.current_total), format_bytes_per_sec(state.current_speed_bps),
            format_bytes(l_free), format_bytes(l_total), format_bytes(r_free), format_bytes(r_total), file_bar
        )).block(Block::default().borders(Borders::ALL).title("Progress")).wrap(Wrap { trim: true }),
        area,
    );
}

fn render_done(frame: &mut Frame, state: &AppState, area: Rect) {
    let mut summary = state.status_line.clone();
    let mut completed: Vec<String> = state.last_results.iter()
        .filter(|r| r.error.is_none() && (r.outcome == "ok" || r.outcome == "dry-run"))
        .map(|r| {
            let label = match r.action.action_type {
                ActionType::CopyLeftToRight => "copy L→R", ActionType::CopyRightToLeft => "copy R→L",
                ActionType::DeleteLeft => "deleted L", ActionType::DeleteRight => "deleted R",
            };
            format!("[{}] {}", label, r.action.path_rel.display())
        }).collect();
    completed.sort();
    if !completed.is_empty() {
        summary.push_str("\nCompleted actions:\n");
        for p in completed { summary.push_str(&format!("- {}\n", p)); }
    }
    frame.render_widget(Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title("Summary")).wrap(Wrap { trim: true }), area);
}

fn render_palette(frame: &mut Frame, state: &AppState, area: Rect) {
    let matches = filter_commands(&state.palette_query);
    let items: Vec<ListItem> = matches.iter().enumerate().map(|(i, &&(name, desc))| {
        let mut item = ListItem::new(format!("  {}  —  {}", name, desc));
        if i == state.palette_selected { item = item.style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Yellow)); }
        item
    }).collect();
    frame.render_widget(List::new(items).block(Block::default().borders(Borders::ALL).title(format!("> {}_", state.palette_query))), area);
}

fn render_scrollbar(frame: &mut Frame, area: Rect, start: usize, height: usize, total: usize) {
    let h = area.height as usize;
    if h == 0 || total == 0 { return; }
    let thumb_size = height.max(1).min(h);
    let max_start = total.saturating_sub(height).max(1);
    let pos = ((start as f64 / max_start as f64) * (h.saturating_sub(thumb_size) as f64)).round() as usize;
    let text = (0..h).map(|i| if i >= pos && i < pos + thumb_size { "█" } else { "│" }).collect::<Vec<_>>().join("\n");
    frame.render_widget(Paragraph::new(text), area);
}

pub fn help_text(state: &AppState) -> Line<'static> {
    match state.phase {
        Phase::Review => Line::from(vec![
            Span::raw("Up/Down: move  Enter: sync  Space: toggle  a: all  c: clear  s: sync-all  "),
            Span::raw("l: L→R  r: R→L  d: delete  f: force  1-5: filter  n: sort  /: palette  F5: refresh  h: history  Esc Esc: quit"),
        ]),
        Phase::History => Line::from(vec![Span::raw("Up/Down: move  Esc: back  Esc Esc: quit")]),
        Phase::ChoosingStrategy => Line::from(vec![Span::raw("n=newer  l=left  r=right  k=skip  b/Esc: back  Esc Esc: quit")]),
        Phase::ConfirmSync => Line::from(vec![Span::raw("Enter/y: yes  n: no  b/Esc: back  Esc Esc: quit")]),
        Phase::ConfirmDelete => Line::from(vec![Span::raw("Confirm delete? Enter/y: delete  n/b/Esc: back  Esc Esc: quit")]),
        Phase::Syncing => Line::from(vec![Span::raw("Syncing... q: cancel  o: reveal  Esc Esc: quit")]),
        Phase::Done => Line::from(vec![Span::raw("q: quit  o: reveal last copy  Esc Esc: quit")]),
        Phase::Scanning => Line::from(vec![Span::raw("Scanning... q: quit  Esc Esc: quit")]),
    }
}

fn details_text(state: &AppState, args: &AppArgs) -> String {
    if state.diffs.is_empty() { return "No selection".to_string(); }
    let selection: Vec<usize> = if state.selected_items.is_empty() {
        state.filtered_indices.get(state.selected).copied().into_iter().collect()
    } else {
        state.selected_items.iter().copied().collect()
    };
    let lines: Vec<String> = selection.iter().filter_map(|&idx| state.diffs.get(idx)).map(|diff| {
        let status = match diff.status {
            DiffStatus::Same => "same", DiffStatus::MissingLeft => "missing-left",
            DiffStatus::MissingRight => "missing-right", DiffStatus::Mismatch => "mismatch", DiffStatus::Conflict => "conflict",
        };
        let left = meta_line("Left", &args.left.join(&diff.path_rel), diff.left.as_ref());
        let right = meta_line("Right", &args.right.join(&diff.path_rel), diff.right.as_ref());
        let override_line = state.action_overrides.get(&diff.path_rel).map(|a| match a {
            ActionType::CopyLeftToRight => "Override: left -> right", ActionType::CopyRightToLeft => "Override: right -> left",
            ActionType::DeleteLeft => "Override: delete left", ActionType::DeleteRight => "Override: delete right",
        }).unwrap_or("Override: none");
        format!("{}\nStatus: {}\n{}\n{}\n{}", diff.path_rel.display(), status, left, right, override_line)
    }).collect();
    if lines.is_empty() { "No selection".to_string() } else { lines.join("\n---\n") }
}

fn history_details_text(state: &AppState) -> String {
    let entry = match state.history.get(state.history_selected) { Some(e) => e, None => return "No history".to_string() };
    format!("Run #{}\nStatus: {}\nStarted: {}\nCompleted: {}\nLeft: {}\nRight: {}\nActions: {}\nErrors: {}",
        entry.run_id, entry.status, entry.started_at,
        entry.completed_at.clone().unwrap_or_else(|| "-".to_string()),
        entry.left_root, entry.right_root, entry.actions, entry.errors)
}

fn meta_line(label: &str, path: &Path, meta: Option<&drive_mirror_core::models::FileMeta>) -> String {
    match meta {
        Some(m) => {
            let link_info = if m.is_symlink { format!(" | symlink -> {}", m.link_target.clone().unwrap_or_else(|| "unknown".to_string())) } else { String::new() };
            format!("{}: {} | size {} | mtime {} | hash {}{}", label, path.display(), format_bytes(m.size), format_mtime(m.mtime), m.hash.clone().unwrap_or_else(|| "n/a".to_string()), link_info)
        }
        None => format!("{}: {} | missing", label, path.display()),
    }
}

fn wrap_text(text: &str, max_width: usize) -> Vec<String> {
    if max_width == 0 { return vec![text.to_string()]; }
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            let mut chunk = word;
            while chunk.len() > max_width { let (h, t) = chunk.split_at(max_width); lines.push(h.to_string()); chunk = t; }
            current = chunk.to_string();
        } else if current.len() + 1 + word.len() <= max_width {
            current.push(' '); current.push_str(word);
        } else {
            lines.push(std::mem::take(&mut current));
            let mut chunk = word;
            while chunk.len() > max_width { let (h, t) = chunk.split_at(max_width); lines.push(h.to_string()); chunk = t; }
            current = chunk.to_string();
        }
    }
    if !current.is_empty() { lines.push(current); }
    if lines.is_empty() { lines.push(String::new()); }
    lines
}

fn truncate_to_width(text: &str, max_width: usize) -> String {
    if text.chars().count() <= max_width { return text.to_string(); }
    let mut out: String = text.chars().take(max_width.saturating_sub(1)).collect();
    out.push('…');
    out
}

pub fn compute_sync_overview(state: &AppState) -> (u64, u64, usize, usize, usize) {
    let mut lr_bytes = 0u64; let mut rl_bytes = 0u64; let mut miss_l = 0; let mut miss_r = 0; let mut mism = 0;
    for d in &state.diffs {
        if state.copied_recently.contains(&d.path_rel) && !state.force_recopy.contains(&d.path_rel) { continue; }
        match d.status {
            DiffStatus::MissingLeft => { miss_l += 1; if let Some(ref r) = d.right { rl_bytes = rl_bytes.saturating_add(r.size); } }
            DiffStatus::MissingRight => { miss_r += 1; if let Some(ref l) = d.left { lr_bytes = lr_bytes.saturating_add(l.size); } }
            DiffStatus::Mismatch | DiffStatus::Conflict => {
                mism += 1;
                if let Some(action) = state.action_overrides.get(&d.path_rel) {
                    match action {
                        ActionType::CopyLeftToRight => { if let Some(ref l) = d.left { lr_bytes = lr_bytes.saturating_add(l.size); } }
                        ActionType::CopyRightToLeft => { if let Some(ref r) = d.right { rl_bytes = rl_bytes.saturating_add(r.size); } }
                        _ => {}
                    }
                } else {
                    let lm = d.left.as_ref().map(|m| m.mtime).unwrap_or(0);
                    let rm = d.right.as_ref().map(|m| m.mtime).unwrap_or(0);
                    if lm >= rm { if let Some(ref l) = d.left { lr_bytes = lr_bytes.saturating_add(l.size); } }
                    else if let Some(ref r) = d.right { rl_bytes = rl_bytes.saturating_add(r.size); }
                }
            }
            DiffStatus::Same => {}
        }
    }
    (lr_bytes, rl_bytes, miss_l, miss_r, mism)
}

pub fn reveal_in_file_manager(path: &Path) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    { Command::new("open").arg("-R").arg(path).status().ok(); return Ok(()); }
    #[cfg(target_os = "windows")]
    { Command::new("explorer").arg("/select,").arg(path).status().ok(); return Ok(()); }
    #[cfg(target_os = "linux")]
    { Command::new("xdg-open").arg(path.parent().unwrap_or(path)).status().ok(); return Ok(()); }
    #[allow(unreachable_code)]
    Ok(())
}
