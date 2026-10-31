//! Inline Ratatui saved-network connection command.

use crate::{
    app::AppState,
    config::{self, IconSet},
    error::WifiResult,
    theme,
    wifi::{
        ConnectionEvent, WifiInfo, connect_profile, disconnect, disconnect_and_wait,
        forget_network, get_connected_ssid, get_saved_profiles, get_wifi_networks, scan_networks,
        start_wifi_listener,
    },
};
use color_eyre::eyre::{Result, eyre};
use crossterm::{
    cursor::Show,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    style::Print,
    terminal::disable_raw_mode,
};
use ratatui::{
    DefaultTerminal, TerminalOptions, Viewport,
    layout::Position,
    prelude::{Frame, Line, Modifier, Span, Style},
    widgets::{Block, List, ListItem, ListState},
};
use std::{
    collections::HashSet,
    io::{self, IsTerminal, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Clone, Copy)]
enum RowStatus<'a> {
    Normal,
    Connecting(&'a str),
    Disconnecting(&'a str),
    Connected(&'a str),
    Disconnected(&'a str),
    Failed(&'a str),
}

#[derive(Clone, Copy)]
enum Operation {
    Connect,
    Disconnect,
}

#[derive(Clone, Copy)]
enum WaitOutcome {
    Completed,
    Cancelled,
}

/// Restores raw mode and the cursor without touching the alternate screen.
struct InlineTerminalGuard;

impl Drop for InlineTerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, Show);
        let _ = stdout.flush();
    }
}

pub fn run(search_term: &str, use_ascii_icons: bool) -> Result<()> {
    let saved_profiles = get_saved_profiles()
        .map_err(|error| eyre!(format!("Could not read saved networks: {error}")))?;
    // Open the UI from the current scan cache immediately. A missing saved
    // profile is still shown as a saved fallback row and refined by the
    // background scan started below.
    let network_metadata = read_current_networks();
    let connected_ssid = get_connected_ssid().ok().flatten();
    let mut rows = matching_rows(
        search_term,
        saved_profiles,
        &network_metadata,
        connected_ssid.as_deref(),
    );

    if rows.is_empty() {
        println!("No saved networks matched \"{search_term}\".");
        return Ok(());
    }

    // Watch OS connection notifications so quick connect can react to a
    // failure immediately instead of polling for a full minute.
    let (connection_event_tx, mut connection_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let _connection_listener = match start_wifi_listener(connection_event_tx) {
        Ok(listener) => Some(listener),
        Err(error) => {
            eprintln!("Warning: could not listen for Wi-Fi connection events: {error}");
            None
        }
    };

    let icon_set = if use_ascii_icons {
        IconSet::Ascii
    } else {
        IconSet::Nerd
    };
    let height = rows.len().min(usize::from(u16::MAX)) as u16;
    let mut terminal = ratatui::try_init_with_options(TerminalOptions {
        viewport: Viewport::Inline(height),
    })
    .map_err(|error| eyre!(format!("Could not initialize inline terminal: {error}")))?;
    let _terminal_guard = InlineTerminalGuard;
    terminal
        .hide_cursor()
        .map_err(|error| eyre!(format!("Could not hide terminal cursor: {error}")))?;

    let scan_job = if rows.len() > 1 {
        Some(ScanJob::spawn(
            rows.iter().map(|row| row.ssid.clone()).collect(),
        ))
    } else {
        None
    };

    let selected = if rows.len() > 1 {
        choose_profile(&mut terminal, &mut rows, icon_set, scan_job.as_ref())?
    } else {
        Some(0)
    };

    // Stop the background scan before associating; scans can interfere with
    // the connection attempt.
    drop(scan_job);

    if rows.is_empty() {
        exit_with_message(&mut terminal, "All matching saved networks were forgotten.")?;
        return Ok(());
    }

    // A saved profile may be missing from the latest scan (hidden network or
    // a slow scan). Try connecting anyway: the OS can connect from the saved
    // profile, and a genuinely out-of-range target fails via the OS timeout.
    if let Some(selected) = selected {
        run_inline_operation(
            &mut terminal,
            &rows,
            selected,
            icon_set,
            &mut connection_event_rx,
        )?;
    } else {
        terminal.clear()?;
        draw_rows(
            &mut terminal,
            &rows,
            selected.unwrap_or(0),
            icon_set,
            RowStatus::Normal,
            0,
        )?;
    }

    exit_below_rows(&mut terminal, rows.len())?;
    Ok(())
}

/// Background scan that feeds updated network metadata to the picker without
/// blocking the first frame. Cancelled and joined when the picker closes so a
/// scan cannot interfere with the connection attempt.
struct ScanJob {
    rx: Receiver<Vec<WifiInfo>>,
    cancel: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ScanJob {
    fn spawn(targets: Vec<String>) -> Self {
        let (tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let worker_cancel = Arc::clone(&cancel);
        let handle = thread::spawn(move || {
            let deadline =
                Instant::now() + Duration::from_millis(config::QUICK_CONNECT_SCAN_TIMEOUT_MS);
            let mut next_scan = Instant::now();

            while !worker_cancel.load(Ordering::SeqCst) && Instant::now() < deadline {
                if Instant::now() >= next_scan {
                    let _ = scan_networks();
                    next_scan =
                        Instant::now() + Duration::from_millis(config::QUICK_CONNECT_SCAN_RETRY_MS);
                }

                thread::sleep(Duration::from_millis(config::QUICK_CONNECT_SCAN_POLL_MS));

                if worker_cancel.load(Ordering::SeqCst) {
                    break;
                }

                if let Ok(networks) = get_wifi_networks() {
                    let done = all_profiles_seen_in_scan(&targets, &networks);
                    if tx.send(networks).is_err() || done {
                        break;
                    }
                }
            }
        });

        Self {
            rx,
            cancel,
            handle: Some(handle),
        }
    }
}

impl Drop for ScanJob {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn read_current_networks() -> Vec<WifiInfo> {
    match get_wifi_networks() {
        Ok(networks) => networks,
        Err(error) => {
            eprintln!("Warning: could not read current networks: {error}");
            Vec::new()
        }
    }
}

fn all_profiles_seen_in_scan(profiles: &[String], networks: &[WifiInfo]) -> bool {
    profiles
        .iter()
        .all(|ssid| networks.iter().any(|network| network.ssid == *ssid))
}

/// Copy fresh radio metadata into the picker rows while keeping their saved
/// and connected flags.
fn apply_scan_metadata(rows: &mut [WifiInfo], networks: &[WifiInfo]) {
    for row in rows {
        let Some(network) = networks.iter().find(|network| network.ssid == row.ssid) else {
            continue;
        };

        let was_connected = row.is_connected;
        row.authentication = network.authentication.clone();
        row.encryption = network.encryption.clone();
        row.signal = network.signal;
        row.channel = network.channel;
        row.frequency = network.frequency;
        row.link_speed = network.link_speed;
        row.phy_type = network.phy_type.clone();
        row.bssid = network.bssid.clone();
        row.is_saved = true;
        row.is_connected = was_connected || network.is_connected;
        row.auto_connect = network.auto_connect;
    }
}

/// Clear the viewport and leave a one-line message where the rows were.
fn exit_with_message(terminal: &mut DefaultTerminal, message: &str) -> Result<()> {
    terminal.clear()?;
    let top = terminal.get_frame().area().top();
    terminal.set_cursor_position(Position { x: 0, y: top })?;
    execute!(terminal.backend_mut(), Show)?;
    terminal.backend_mut().flush()?;
    println!("{message}");
    Ok(())
}

/// Park the cursor below the last drawn row so the prompt lands there.
/// A shrunk viewport still reserves its original height, so only a full one
/// needs the extra newline (which scrolls solely at the screen bottom).
fn exit_below_rows(terminal: &mut DefaultTerminal, drawn: usize) -> Result<()> {
    let area = terminal.get_frame().area();
    let drawn = drawn.min(usize::from(area.height));
    if drawn < usize::from(area.height) {
        let target = area.top().saturating_add(drawn as u16);
        terminal.set_cursor_position(Position { x: 0, y: target })?;
        execute!(terminal.backend_mut(), Show)?;
    } else {
        let bottom = area.bottom().saturating_sub(1);
        terminal.set_cursor_position(Position { x: 0, y: bottom })?;
        execute!(terminal.backend_mut(), Print("\n"), Show)?;
    }
    terminal.backend_mut().flush()?;
    Ok(())
}

fn matching_rows(
    search_term: &str,
    saved_profiles: Vec<String>,
    network_metadata: &[WifiInfo],
    connected_ssid: Option<&str>,
) -> Vec<WifiInfo> {
    let mut seen = HashSet::new();
    let query_lower = search_term.to_lowercase();

    let mut scored: Vec<(WifiInfo, bool)> = saved_profiles
        .into_iter()
        .filter_map(|ssid| {
            if !AppState::matches_search(&ssid, search_term) || !seen.insert(ssid.clone()) {
                return None;
            }

            // Exact substrings rank before subsequence-only hits.
            let exact = ssid.to_lowercase().contains(&query_lower);
            // Attempt saved profiles the scan missed (out of range or stale).
            let is_connected = connected_ssid.is_some_and(|current| current == ssid.as_str());
            let row = match network_metadata.iter().find(|network| network.ssid == ssid) {
                Some(network) => {
                    let mut row = network.clone();
                    row.ssid = ssid;
                    row.is_saved = true;
                    row.is_connected |= is_connected;
                    row
                }
                None => WifiInfo::saved_fallback(ssid, is_connected),
            };
            Some((row, exact))
        })
        .collect();
    // Stable: exact hits keep saved order first, fuzzy hits follow.
    scored.sort_by_key(|(_, exact)| !*exact);
    scored.into_iter().map(|(row, _)| row).collect()
}

fn choose_profile(
    terminal: &mut DefaultTerminal,
    rows: &mut Vec<WifiInfo>,
    icon_set: IconSet,
    scan_job: Option<&ScanJob>,
) -> Result<Option<usize>> {
    let mut selected = 0;
    draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;

    if !io::stdin().is_terminal() {
        return Ok(None);
    }

    loop {
        if let Some(job) = scan_job {
            while let Ok(networks) = job.rx.try_recv() {
                apply_scan_metadata(rows, &networks);
                draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            }
        }

        if !event::poll(Duration::from_millis(config::EVENT_POLL_MS))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        match key.code {
            KeyCode::Enter => return Ok(Some(selected)),
            KeyCode::Esc | KeyCode::Char('q') => return Ok(None),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(None);
            }
            KeyCode::Char('[') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(None);
            }
            KeyCode::Char('j') | KeyCode::Down => {
                selected = (selected + 1).min(rows.len() - 1);
                draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            }
            KeyCode::Char('k') | KeyCode::Up => {
                selected = selected.saturating_sub(1);
                draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            }
            KeyCode::Char('g') | KeyCode::Home => {
                selected = 0;
                draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            }
            KeyCode::Char('G') | KeyCode::End => {
                selected = rows.len() - 1;
                draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            }
            // Forget the selected saved network and drop it from the picker,
            // mirroring the TUI `f` key.
            KeyCode::Char('f') | KeyCode::Delete => {
                let ssid = rows[selected].ssid.clone();
                match forget_network(&ssid) {
                    Ok(()) => {
                        rows.remove(selected);
                        if rows.is_empty() {
                            return Ok(None);
                        }
                        selected = selected.min(rows.len() - 1);
                        // Clear the dead lines so the forgotten row doesn't ghost.
                        terminal.clear()?;
                        draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
                    }
                    Err(_) => {
                        draw_rows(
                            terminal,
                            rows,
                            selected,
                            icon_set,
                            RowStatus::Failed(&ssid),
                            0,
                        )?;
                    }
                }
            }
            _ => {}
        }
    }
}

fn run_inline_operation(
    terminal: &mut DefaultTerminal,
    rows: &[WifiInfo],
    selected: usize,
    icon_set: IconSet,
    events: &mut UnboundedReceiver<ConnectionEvent>,
) -> Result<()> {
    let ssid = rows[selected].ssid.clone();
    let operation = if rows[selected].is_connected
        || matches!(get_connected_ssid(), Ok(Some(current)) if current == ssid)
    {
        Operation::Disconnect
    } else {
        Operation::Connect
    };
    let timeout = if is_out_of_range(&rows[selected]) {
        Duration::from_secs(config::UNSEEN_CONNECTION_TIMEOUT_SECS)
    } else {
        Duration::from_secs(config::CONNECTION_TIMEOUT_SECS)
    };
    // Drop notifications accumulated while the picker was open; only events
    // produced by this operation should affect the result.
    while events.try_recv().is_ok() {}
    let (result_tx, result_rx) = mpsc::channel();
    let target_ssid = ssid.clone();
    let cancel = Arc::new(AtomicBool::new(false));
    let worker_cancel = Arc::clone(&cancel);
    let worker = thread::spawn(move || {
        let result = match operation {
            Operation::Connect => connect_saved_profile(&target_ssid, &worker_cancel),
            Operation::Disconnect => disconnect(),
        };
        let _ = result_tx.send(result);
    });

    let result = wait_for_operation(
        terminal, rows, selected, &ssid, operation, icon_set, result_rx, &cancel, events, timeout,
    );
    let status = match (operation, result) {
        (_, Ok(WaitOutcome::Cancelled)) => {
            cancel.store(true, Ordering::SeqCst);
            if matches!(operation, Operation::Connect) {
                // Abort the association that may already be in flight. The
                // worker also re-checks the flag before and after connecting.
                let _ = disconnect();
                // Wait for the worker so a late WlanConnect request cannot
                // outlive the cancelled quick-connect process.
                let _ = worker.join();
            }
            terminal.clear()?;
            draw_rows(terminal, rows, selected, icon_set, RowStatus::Normal, 0)?;
            return Ok(());
        }
        (Operation::Connect, Ok(WaitOutcome::Completed)) => RowStatus::Connected(&ssid),
        (Operation::Disconnect, Ok(WaitOutcome::Completed)) => RowStatus::Disconnected(&ssid),
        (_, Err(_)) => RowStatus::Failed(&ssid),
    };

    terminal.clear()?;
    draw_rows(terminal, rows, selected, icon_set, status, 0)?;
    Ok(())
}

fn connect_saved_profile(ssid: &str, cancel: &AtomicBool) -> WifiResult<()> {
    if cancel.load(Ordering::SeqCst) {
        return Ok(());
    }

    let current_ssid = get_connected_ssid().ok().flatten();
    if current_ssid
        .as_deref()
        .is_some_and(|current| current != ssid)
    {
        disconnect_and_wait()?;
    }

    if cancel.load(Ordering::SeqCst) {
        return Ok(());
    }

    connect_profile(ssid)?;

    // Esc can arrive while WlanConnect is being submitted. Undo the
    // association here as well so a cancelled quick connect does not connect
    // after the picker has already exited.
    if cancel.load(Ordering::SeqCst) {
        let _ = disconnect();
    }
    Ok(())
}

fn wait_for_operation(
    terminal: &mut DefaultTerminal,
    rows: &[WifiInfo],
    selected: usize,
    ssid: &str,
    operation: Operation,
    icon_set: IconSet,
    result_rx: Receiver<WifiResult<()>>,
    cancel: &AtomicBool,
    events: &mut UnboundedReceiver<ConnectionEvent>,
    timeout: Duration,
) -> Result<WaitOutcome> {
    let deadline = Instant::now() + timeout;
    let anim_start = Instant::now();
    draw_rows(
        terminal,
        rows,
        selected,
        icon_set,
        loading_status(operation, ssid),
        spinner_frame(anim_start),
    )?;

    loop {
        if check_cancellation(cancel)? {
            return Ok(WaitOutcome::Cancelled);
        }

        if let Some(outcome) = connection_event_outcome(events, operation, ssid) {
            return outcome;
        }

        match result_rx.try_recv() {
            Ok(Ok(())) => {
                return match operation {
                    Operation::Connect => wait_until_connected(
                        terminal, rows, selected, ssid, icon_set, deadline, anim_start, cancel,
                        events,
                    ),
                    Operation::Disconnect => wait_until_disconnected(
                        terminal, rows, selected, ssid, icon_set, deadline, anim_start, cancel,
                        events,
                    ),
                };
            }
            Ok(Err(error)) => return Err(eyre!(error.to_string())),
            Err(TryRecvError::Disconnected) => {
                return Err(eyre!("Connection worker stopped unexpectedly"));
            }
            Err(TryRecvError::Empty) => {}
        }

        draw_rows(
            terminal,
            rows,
            selected,
            icon_set,
            loading_status(operation, ssid),
            spinner_frame(anim_start),
        )?;
        if Instant::now() >= deadline {
            return Err(eyre!("Connection timed out (No response from OS)"));
        }
        thread::sleep(Duration::from_millis(config::EVENT_POLL_MS));
    }
}

/// Spinner position from wall-clock time so a slow pass never retards it.
fn spinner_frame(started: Instant) -> usize {
    (started.elapsed().as_millis() / u128::from(config::EVENT_POLL_MS)) as usize
}

fn cancellation_requested() -> Result<bool> {
    if !io::stdin().is_terminal() {
        return Ok(false);
    }

    while event::poll(Duration::ZERO)? {
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => return Ok(true),
            KeyCode::Char('c') | KeyCode::Char('[')
                if key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                return Ok(true);
            }
            _ => {}
        }
    }

    Ok(false)
}

/// Check the shared cancel flag first, then drain pending key presses. Any
/// observed cancellation is published to the worker thread.
fn check_cancellation(cancel: &AtomicBool) -> Result<bool> {
    if cancel.load(Ordering::SeqCst) {
        return Ok(true);
    }
    if cancellation_requested()? {
        cancel.store(true, Ordering::SeqCst);
        return Ok(true);
    }
    Ok(false)
}

/// Translate the first relevant OS notification into a wait outcome.
/// `connection_complete` is terminal; earlier attempt failures are ignored by
/// the Windows listener because a later retry can still succeed.
fn connection_event_outcome(
    events: &mut UnboundedReceiver<ConnectionEvent>,
    operation: Operation,
    ssid: &str,
) -> Option<Result<WaitOutcome>> {
    while let Ok(event) = events.try_recv() {
        match event {
            ConnectionEvent::Connected(connected)
                if matches!(operation, Operation::Connect) && connected == ssid =>
            {
                return Some(Ok(WaitOutcome::Completed));
            }
            ConnectionEvent::Failed {
                ssid: failed_ssid,
                reason_str,
                ..
            } if matches!(operation, Operation::Connect)
                && (failed_ssid.is_empty() || failed_ssid == ssid) =>
            {
                return Some(Err(eyre!("Connection failed: {reason_str}")));
            }
            ConnectionEvent::Disconnected if matches!(operation, Operation::Disconnect) => {
                return Some(Ok(WaitOutcome::Completed));
            }
            _ => {}
        }
    }
    None
}

fn loading_status<'a>(operation: Operation, ssid: &'a str) -> RowStatus<'a> {
    match operation {
        Operation::Connect => RowStatus::Connecting(ssid),
        Operation::Disconnect => RowStatus::Disconnecting(ssid),
    }
}

fn wait_until_connected(
    terminal: &mut DefaultTerminal,
    rows: &[WifiInfo],
    selected: usize,
    ssid: &str,
    icon_set: IconSet,
    deadline: Instant,
    anim_start: Instant,
    cancel: &AtomicBool,
    events: &mut UnboundedReceiver<ConnectionEvent>,
) -> Result<WaitOutcome> {
    let mut tick = 0;
    loop {
        if check_cancellation(cancel)? {
            return Ok(WaitOutcome::Cancelled);
        }

        if let Some(outcome) = connection_event_outcome(events, Operation::Connect, ssid) {
            return outcome;
        }

        if tick % config::STATUS_POLL_FRAMES == 0
            && get_connected_ssid()
                .map_err(|error| eyre!(error.to_string()))?
                .as_deref()
                == Some(ssid)
        {
            return Ok(WaitOutcome::Completed);
        }
        tick = tick.wrapping_add(1);

        draw_rows(
            terminal,
            rows,
            selected,
            icon_set,
            RowStatus::Connecting(ssid),
            spinner_frame(anim_start),
        )?;
        if Instant::now() >= deadline {
            return Err(eyre!("Connection timed out (No response from OS)"));
        }
        thread::sleep(Duration::from_millis(config::EVENT_POLL_MS));
    }
}

fn wait_until_disconnected(
    terminal: &mut DefaultTerminal,
    rows: &[WifiInfo],
    selected: usize,
    ssid: &str,
    icon_set: IconSet,
    deadline: Instant,
    anim_start: Instant,
    cancel: &AtomicBool,
    events: &mut UnboundedReceiver<ConnectionEvent>,
) -> Result<WaitOutcome> {
    let mut tick = 0;
    loop {
        if check_cancellation(cancel)? {
            return Ok(WaitOutcome::Cancelled);
        }

        if let Some(outcome) = connection_event_outcome(events, Operation::Disconnect, ssid) {
            return outcome;
        }

        if tick % config::STATUS_POLL_FRAMES == 0
            && get_connected_ssid()
                .map_err(|error| eyre!(error.to_string()))?
                .as_deref()
                != Some(ssid)
        {
            return Ok(WaitOutcome::Completed);
        }
        tick = tick.wrapping_add(1);

        draw_rows(
            terminal,
            rows,
            selected,
            icon_set,
            RowStatus::Disconnecting(ssid),
            spinner_frame(anim_start),
        )?;
        if Instant::now() >= deadline {
            return Err(eyre!("Disconnection timed out (No response from OS)"));
        }
        thread::sleep(Duration::from_millis(config::EVENT_POLL_MS));
    }
}

fn draw_rows(
    terminal: &mut DefaultTerminal,
    rows: &[WifiInfo],
    selected: usize,
    icon_set: IconSet,
    status: RowStatus<'_>,
    loading_frame: usize,
) -> Result<()> {
    terminal.draw(|frame| render_rows(frame, rows, selected, icon_set, status, loading_frame))?;
    Ok(())
}

/// Saved rows without scan metadata (no BSSID/channel/frequency/signal) are
/// either out of range or the scan has not observed them yet.
fn is_out_of_range(network: &WifiInfo) -> bool {
    !network.is_connected
        && network.signal == 0
        && network.channel == 0
        && network.frequency == 0
        && network.bssid.is_none()
}

fn render_rows(
    frame: &mut Frame,
    rows: &[WifiInfo],
    selected: usize,
    icon_set: IconSet,
    status: RowStatus<'_>,
    loading_frame: usize,
) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(theme::BACKGROUND).fg(theme::FOREGROUND)),
        area,
    );

    let spinner = config::LOADING_CHARS[loading_frame % config::LOADING_CHARS.len()];
    let items: Vec<ListItem> = rows
        .iter()
        .map(|network| {
            let is_connecting = matches!(
                status,
                RowStatus::Connecting(target) if target == network.ssid.as_str()
            );
            let is_disconnecting = matches!(
                status,
                RowStatus::Disconnecting(target) if target == network.ssid.as_str()
            );
            let is_failed = matches!(
                status,
                RowStatus::Failed(target) if target == network.ssid.as_str()
            );
            let is_disconnected = matches!(
                status,
                RowStatus::Disconnected(target) if target == network.ssid.as_str()
            );
            let is_connected = !is_connecting
                && !is_disconnecting
                && !is_failed
                && !is_disconnected
                && (network.is_connected
                    || matches!(
                        status,
                        RowStatus::Connected(target) if target == network.ssid.as_str()
                    ));

            let row_style = if is_connecting {
                Style::default()
                    .fg(theme::YELLOW)
                    .add_modifier(Modifier::BOLD)
            } else if is_disconnecting {
                Style::default()
                    .fg(theme::PURPLE)
                    .add_modifier(Modifier::BOLD)
            } else if is_failed {
                Style::default().fg(theme::RED).add_modifier(Modifier::BOLD)
            } else if is_connected {
                Style::default()
                    .fg(theme::GREEN)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::BLUE)
            };

            let prefix = if is_connecting || is_disconnecting {
                spinner
            } else {
                icon_set.saved()
            };
            let mut spans = vec![Span::styled(prefix, row_style)];
            if is_connecting || is_disconnecting {
                spans.push(Span::raw(" "));
            }
            spans.push(Span::styled(network.ssid.clone(), row_style));

            if is_connected {
                spans.push(Span::styled(icon_set.connected(), row_style));
            }
            if is_connecting {
                spans.push(Span::styled(" connecting...", row_style));
            } else if is_disconnecting {
                spans.push(Span::styled(" disconnecting...", row_style));
            } else if is_failed {
                spans.push(Span::styled(" failed", row_style));
            } else if is_out_of_range(network) {
                spans.push(Span::styled(" (not in range)", row_style));
            } else if network.is_saved {
                let auto_icon = if network.auto_connect {
                    icon_set.auto_on()
                } else {
                    icon_set.auto_off()
                };
                spans.push(Span::styled(format!(" {auto_icon}"), row_style));
            }

            ListItem::new(Line::from(spans))
        })
        .collect();

    let mut list_state = ListState::default();
    list_state.select(Some(selected.min(rows.len().saturating_sub(1))));
    // The selection highlight must stay visible even when forgetting shrinks
    // the picker to a single row; the Nerd highlight symbol alone is blank.
    let list = List::new(items)
        .highlight_symbol(icon_set.highlight())
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .bg(theme::SELECTION_BG),
        );
    frame.render_stateful_widget(list, area, &mut list_state);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discovered(ssid: &str, is_connected: bool) -> WifiInfo {
        WifiInfo {
            ssid: ssid.to_string(),
            is_saved: true,
            is_connected,
            signal: 80,
            channel: 6,
            frequency: 2_437_000,
            bssid: Some("aa:bb:cc:dd:ee:ff".to_string()),
            ..WifiInfo::default()
        }
    }

    #[test]
    fn matching_rows_includes_saved_profiles_not_in_current_scan() {
        let rows = matching_rows(
            "204",
            vec!["Current 204".to_string(), "Stale 204".to_string()],
            &[discovered("Current 204", false)],
            None,
        );

        assert_eq!(
            rows.iter().map(|row| row.ssid.as_str()).collect::<Vec<_>>(),
            vec!["Current 204", "Stale 204"]
        );
        let stale = &rows[1];
        assert!(stale.is_saved);
        assert!(!stale.is_connected);
        assert!(is_out_of_range(stale));
        assert!(!is_out_of_range(&rows[0]));
    }

    #[test]
    fn matching_rows_marks_connected_fallback_row() {
        let rows = matching_rows("204", vec!["Stale 204".to_string()], &[], Some("Stale 204"));

        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_saved);
        assert!(rows[0].is_connected);
        assert!(!is_out_of_range(&rows[0]));
    }

    #[test]
    fn matching_rows_matches_subsequence_terms() {
        let rows = matching_rows(
            "hme2",
            vec!["Home 204".to_string()],
            &[discovered("Home 204", false)],
            None,
        );

        assert_eq!(
            rows.iter().map(|row| row.ssid.as_str()).collect::<Vec<_>>(),
            vec!["Home 204"]
        );
    }

    #[test]
    fn matching_rows_rejects_out_of_order_terms() {
        let rows = matching_rows(
            "2emh",
            vec!["Home 204".to_string()],
            &[discovered("Home 204", false)],
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn matching_rows_ranks_exact_hits_before_subsequence() {
        let rows = matching_rows(
            "k4",
            vec!["KP PG 204".to_string(), "K4".to_string()],
            &[],
            None,
        );

        assert_eq!(
            rows.iter().map(|row| row.ssid.as_str()).collect::<Vec<_>>(),
            vec!["K4", "KP PG 204"]
        );
    }

    #[test]
    fn all_profiles_seen_in_scan_requires_every_target() {
        let networks = vec![discovered("Current 204", false)];
        let targets = |names: &[&str]| {
            names
                .iter()
                .map(|ssid| ssid.to_string())
                .collect::<Vec<_>>()
        };

        assert!(all_profiles_seen_in_scan(
            &targets(&["Current 204"]),
            &networks
        ));
        assert!(all_profiles_seen_in_scan(&targets(&[]), &networks));
        assert!(!all_profiles_seen_in_scan(
            &targets(&["Current 204", "Stale 204"]),
            &networks
        ));
        assert!(!all_profiles_seen_in_scan(
            &targets(&["Stale 204"]),
            &networks
        ));
    }

    #[test]
    fn apply_scan_metadata_fills_in_a_saved_fallback_row() {
        let mut row = WifiInfo::saved_fallback("Home 204".to_string(), false);
        let networks = [discovered("Home 204", false)];
        apply_scan_metadata(std::slice::from_mut(&mut row), &networks);

        assert!(row.is_saved);
        assert_eq!(row.signal, 80);
        assert_eq!(row.channel, 6);
        assert_eq!(row.frequency, 2_437_000);
        assert_eq!(row.bssid.as_deref(), Some("aa:bb:cc:dd:ee:ff"));
        assert!(!is_out_of_range(&row));
    }

    #[test]
    fn terminal_connection_failure_matches_target_and_empty_ssids() {
        for failed_ssid in ["Target", ""] {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            tx.send(ConnectionEvent::Failed {
                ssid: failed_ssid.to_string(),
                reason_code: 0x00028002,
                reason_str: "Association Failed".to_string(),
            })
            .unwrap();

            let outcome = connection_event_outcome(&mut rx, Operation::Connect, "Target");
            assert!(matches!(outcome, Some(Err(_))));
        }
    }

    #[test]
    fn unrelated_disconnect_does_not_cancel_a_connect_wait() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(ConnectionEvent::Disconnected).unwrap();

        assert!(connection_event_outcome(&mut rx, Operation::Connect, "Target").is_none());
    }

    #[test]
    fn disconnect_event_completes_disconnect_wait() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(ConnectionEvent::Disconnected).unwrap();

        let outcome = connection_event_outcome(&mut rx, Operation::Disconnect, "Target");
        assert!(matches!(outcome, Some(Ok(WaitOutcome::Completed))));
    }
}
