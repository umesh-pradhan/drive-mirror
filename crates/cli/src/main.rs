use anyhow::{Context, Result};
use clap::Parser;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;
use drive_mirror_core::db::{init_db, insert_run_start, load_last_run_diffs};
use drive_mirror_core::models::{AppState, CompareMode};
use drive_mirror_core::scanner::{build_exclude_set, scan_worker};
use drive_mirror_tui::app::run_app;
use drive_mirror_tui::input::AppArgs;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

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
        if let Err(err) = scan_worker(left_root, right_root, compare_mode, scan_tx.clone(), scan_exclude, scan_last) {
            let _ = scan_tx.send(drive_mirror_core::models::WorkerEvent::Error(err.to_string()));
        }
    });

    enable_raw_mode().context("enable raw mode")?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen).ok();
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("create terminal")?;
    let mut state = AppState::new();

    let app_args = AppArgs {
        left: args.left.clone(),
        right: args.right.clone(),
        compare: args.compare,
        exclude: args.exclude.clone(),
        retries: args.retries,
        dry_run: args.dry_run,
    };

    let res = run_app(&mut terminal, &rx, tx, &mut state, &conn, run_id, &app_args, &last_diffs);

    disable_raw_mode().ok();
    terminal.backend_mut().execute(LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    res
}
