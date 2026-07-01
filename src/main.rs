use std::{
    cell::RefCell,
    env, fs, io,
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicI32, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::SetTitle,
};
use image::DynamicImage;
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout, Rect, Size},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use ratatui_image::{
    Resize, StatefulImage,
    picker::{Picker, ProtocolType},
    protocol::StatefulProtocol,
    thread::{ResizeRequest, ResizeResponse, ThreadProtocol},
};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Value, json};

mod now_playing;
use now_playing::{MediaCommand, NowPlaying};
mod visualizer;
use visualizer::LiveVisualizer;

// Fail Windows builds with one clear sentence instead of a confusing module
// error from the unconditional `std::os::unix` imports above.
#[cfg(not(unix))]
compile_error!("nts supports macOS and Linux only (mpv IPC uses Unix sockets)");

const LIVE_API: &str = "https://www.nts.live/api/v2/live";
const SCHEDULE_API: &str = "https://www.nts.live/api/v2/radio/schedule";
const MIXTAPE_API: &str = "https://www.nts.live/api/v2/mixtapes";
// How long to wait before retrying after a failed background fetch, and the
// steady cadence for refreshing (rarely-changing) mixtape metadata.
const NETWORK_RETRY_DELAY: Duration = Duration::from_secs(15);
const MIXTAPE_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
const NTS_1_STREAM: &str = "https://stream-relay-geo.ntslive.net/stream?client=direct";
const NTS_2_STREAM: &str = "https://stream-relay-geo.ntslive.net/stream2?client=direct";

const INK: Color = Color::Rgb(248, 247, 242);
const MUTED: Color = Color::Rgb(156, 153, 147);
const SIGNAL: Color = Color::Rgb(255, 72, 104);
// BASE is the app-wide canvas; SURFACE is the slightly darker inset for cards.
// Painting BASE ourselves (rather than relying on the terminal's default
// background) keeps INK text high-contrast everywhere, not just inside cards.
const BASE: Color = Color::Rgb(26, 25, 23);
const SURFACE: Color = Color::Rgb(15, 15, 14);
const BORDER: Color = Color::Rgb(84, 82, 77);
const NTS_ARTWORK_ASPECT: f32 = 1.6;

// mpv's PID, mirrored here so the signal guard (see `spawn_signal_guard`) can
// stop playback when the process is killed by a signal — `cmd+w` on a terminal
// tab sends SIGHUP, which never unwinds the stack, so `App`'s `Drop` (and its
// `stop_player`) never run. 0 means there is no live player.
static MPV_PID: AtomicI32 = AtomicI32::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChannelKind {
    Live,
    Mixtape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum View {
    Listen,
    Schedule,
    Explore,
}

#[derive(Clone)]
struct ScheduleEntry {
    starts_at: String,
    title: String,
}

struct Channel {
    number: &'static str,
    name: &'static str,
    kind: ChannelKind,
    stream: String,
    show: String,
    description: String,
    next_show: String,
    next_starts_at: String,
    ends_at: Option<String>,
    schedule: Vec<ScheduleEntry>,
    artwork_url: Option<String>,
    // Resize+encode is offloaded to a per-channel worker thread (see `run`), so
    // the draw pass never blocks: the protocol resizes itself to any card and
    // the encoded result lands a frame later. RefCell because rendering needs
    // `&mut` while the draw path holds the app immutably.
    artwork: Option<RefCell<ThreadProtocol>>,
}

struct App {
    channels: Vec<Channel>,
    selected: usize,
    explore_selected: usize,
    playing: bool,
    buffering: bool,
    player: Option<Player>,
    picker: Picker,
    now_playing: Option<NowPlaying>,
    visualizer: Option<LiveVisualizer>,
    view: View,
    error: Option<String>,
    // Consecutive failed reconnect attempts; drives the exponential backoff in
    // `poll_player` and resets to zero on any healthy probe or explicit stop.
    reconnect_attempts: u32,
    // Set when a live refresh fails so the footer can show a "retrying" notice
    // instead of leaving the user staring at stale fallback copy (see `run`).
    connection_lost: bool,
    // Workers send (channel index, encoded protocol) back here. Set in `run`;
    // `None` outside the event loop (e.g. tests), where artwork is never built.
    encoded_tx: Option<mpsc::Sender<(usize, ResizeResponse)>>,
}

struct Player {
    child: Child,
    ipc_path: PathBuf,
}

#[derive(Deserialize)]
struct LiveResponse {
    results: Vec<LiveChannel>,
}

#[derive(Deserialize)]
struct LiveChannel {
    channel_name: String,
    now: Broadcast,
    next: Broadcast,
    next2: Option<Broadcast>,
}

#[derive(Deserialize, Clone)]
struct Broadcast {
    broadcast_title: Option<String>,
    start_timestamp: Option<String>,
    end_timestamp: Option<String>,
    #[serde(default)]
    embeds: Embeds,
    // Each broadcast links to its episode resource, which carries the full
    // details even when the inline embed does not. NTS labels that link `rel:
    // "details"` on some payloads and `rel: "self"` on others, so we match on
    // the stable signals instead (see `episode_url`).
    #[serde(default)]
    links: Vec<Link>,
}

impl Broadcast {
    /// URL of the episode resource for this broadcast, identified by its episode
    /// media type (`application/vnd.episode+json`) with the `/episodes/` path as
    /// a fallback — never by `rel`, which NTS varies between "details" and
    /// "self" across responses.
    fn episode_url(&self) -> Option<&str> {
        self.links
            .iter()
            .find(|link| {
                link.media_type
                    .as_deref()
                    .is_some_and(|kind| kind.contains("episode"))
                    || link.href.contains("/episodes/")
            })
            .map(|link| link.href.as_str())
    }
}

struct ChannelUpdate {
    index: usize,
    show: String,
    description: String,
    next_show: String,
    next_starts_at: String,
    ends_at: Option<String>,
    schedule: Option<Vec<ScheduleEntry>>,
    artwork_url: Option<String>,
    // Downloaded and landscape-cropped off-thread; encoded lazily at render.
    artwork: Option<DynamicImage>,
    stream: Option<String>,
}

struct ScheduleUpdate {
    index: usize,
    schedule: Vec<ScheduleEntry>,
}

enum BackgroundUpdate {
    Live(std::result::Result<Vec<ChannelUpdate>, String>),
    Schedules(std::result::Result<Vec<ScheduleUpdate>, String>),
    Mixtapes(std::result::Result<Vec<ChannelUpdate>, String>),
    // A single channel's artwork, fetched on its own thread so a slow or failing
    // image never holds back the text or another channel's cover.
    Artwork {
        index: usize,
        url: String,
        image: DynamicImage,
    },
}

#[derive(Deserialize, Clone, Default)]
struct Embeds {
    details: Option<Details>,
}

#[derive(Deserialize, Clone, Default)]
struct Details {
    name: Option<String>,
    description: Option<String>,
    #[serde(default)]
    media: Media,
}

#[derive(Deserialize, Clone, Default)]
struct Media {
    picture_medium: Option<String>,
    picture_thumb: Option<String>,
}

#[derive(Deserialize, Clone)]
struct Link {
    href: String,
    // The MIME-ish type NTS attaches, e.g. "application/vnd.episode+json". This
    // identifies the episode link reliably where `rel` does not.
    #[serde(rename = "type", default)]
    media_type: Option<String>,
}

#[derive(Deserialize)]
struct ScheduleResponse {
    results: Vec<ScheduleDay>,
}

#[derive(Deserialize)]
struct ScheduleDay {
    broadcasts: Vec<Broadcast>,
}

#[derive(Deserialize)]
struct MixtapeResponse {
    title: String,
    subtitle: Option<String>,
    description: Option<String>,
    audio_stream_endpoint: Option<String>,
    media: MixtapeMedia,
}

#[derive(Deserialize, Default)]
struct MixtapeMedia {
    picture_medium: Option<String>,
    picture_thumb: Option<String>,
}

struct CliOptions {
    station: usize,
    autoplay: bool,
}

/// Parse the command line, exiting for `--help`/`--version` and with status 2
/// (the usual usage-error code) for anything unrecognized.
fn parse_cli() -> CliOptions {
    let mut options = CliOptions {
        station: 0,
        autoplay: true,
    };
    for argument in env::args().skip(1) {
        match argument.as_str() {
            "--version" | "-V" => {
                println!("nts {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "--help" | "-h" => {
                println!(
                    "nts {}\n\nA terminal home for NTS Radio.\n\nUSAGE:\n    nts [station] [--no-autoplay]\n\nSTATIONS:\n    1, 2, poolside, slow-focus, low-key, memory-lane, 4-to-the-floor,\n    island-time, the-tube, sheet-music\n\nOPTIONS:\n        --no-autoplay    Start without playing\n    -h, --help           Print help\n    -V, --version        Print version",
                    env!("CARGO_PKG_VERSION")
                );
                std::process::exit(0);
            }
            "--no-autoplay" => options.autoplay = false,
            other if other.starts_with('-') => {
                eprintln!("unknown option: {other}\nTry `nts --help`");
                std::process::exit(2);
            }
            other => match station_index(other) {
                Some(index) => options.station = index,
                None => {
                    eprintln!(
                        "unknown station: {other}\nStations: 1, 2, poolside, slow-focus, low-key, memory-lane, 4-to-the-floor, island-time, the-tube, sheet-music"
                    );
                    std::process::exit(2);
                }
            },
        }
    }
    options
}

/// Resolve a station argument to a channel index. Matching is forgiving:
/// case-insensitive, and hyphens/spaces are interchangeable ("slow-focus",
/// "Slow Focus", "slowfocus" all work). Mixtapes also answer to their NTS API
/// alias (e.g. "100-percent-hip-hop" for Low Key).
fn station_index(query: &str) -> Option<usize> {
    let normalized = normalize_station(query);
    if normalized.is_empty() {
        return None;
    }
    match normalized.as_str() {
        "1" => return Some(0),
        "2" => return Some(1),
        _ => {}
    }
    if let Some(index) = fallback_channels()
        .iter()
        .position(|channel| normalize_station(channel.name) == normalized)
    {
        return Some(index);
    }
    MIXTAPES
        .iter()
        .position(|alias| normalize_station(alias) == normalized)
        .map(|offset| offset + 2)
}

fn normalize_station(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase()
}

fn main() -> Result<()> {
    let cli = parse_cli();
    let mut terminal = ratatui::try_init().context("initialize terminal")?;
    spawn_signal_guard();
    let result = (|| {
        // ratatui-image queries protocol and font metrics while the alternate
        // screen is active and before event handling begins.
        let picker = Picker::from_query_stdio().context("detect terminal image support")?;
        let (media_tx, media_rx) = mpsc::channel();
        let mut app = App::load(&picker, media_tx);
        app.selected = cli.station;
        drain_startup_responses()?;
        if cli.autoplay {
            app.start_default_playback();
        }
        app.sync_now_playing();

        let result = run(&mut terminal, &mut app, media_rx);
        app.stop_player();
        result
    })();
    // `try_restore` covers raw mode and the alternate screen; restore the
    // cursor separately because the terminal may have been left mid-frame.
    let cursor_result = terminal.show_cursor().context("restore terminal cursor");
    let restore_result = ratatui::try_restore().context("restore terminal");
    // The playback title (see `set_terminal_title`) would otherwise outlive
    // the process; an empty title hands naming back to the terminal.
    let _ = execute!(io::stdout(), SetTitle(""));
    cursor_result?;
    restore_result?;
    result
}

/// Stops mpv when the process is terminated by a signal rather than a clean
/// exit. `cmd+w` on a terminal tab/window sends SIGHUP and `kill` sends SIGTERM;
/// the default action for both ends the process immediately, so `App`'s `Drop`
/// never runs and mpv (a child with no controlling terminal) would be orphaned
/// and keep playing. A dedicated thread waits for those signals, stops mpv,
/// restores the terminal, and exits.
#[cfg(unix)]
fn spawn_signal_guard() {
    use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
    use signal_hook::iterator::Signals;

    let mut signals = match Signals::new([SIGHUP, SIGTERM, SIGINT]) {
        Ok(signals) => signals,
        Err(_) => return,
    };
    thread::spawn(move || {
        if signals.forever().next().is_some() {
            let pid = MPV_PID.load(Ordering::Relaxed);
            if pid > 0 {
                // SAFETY: `kill` is a plain libc call; `pid` is mpv's, recorded
                // in `Player::start` and cleared in `Player::stop`.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
            let _ = ratatui::try_restore();
            std::process::exit(0);
        }
    });
}

#[cfg(not(unix))]
fn spawn_signal_guard() {}

impl App {
    fn load(picker: &Picker, media_sender: mpsc::Sender<MediaCommand>) -> Self {
        // Return immediately with fallback copy so the first frame paints and
        // accepts input without waiting on the network. Live show titles and
        // artwork stream in from a background thread (see `run`).
        Self {
            channels: fallback_channels(),
            selected: 0,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            picker: picker.clone(),
            now_playing: Some(NowPlaying::new(media_sender)),
            visualizer: None,
            view: View::Listen,
            error: None,
            reconnect_attempts: 0,
            connection_lost: false,
            encoded_tx: None,
        }
    }

    /// Whether the terminal has a graphics protocol. On text-only terminals we
    /// omit artwork (see README) rather than render a half-block raster.
    fn supports_artwork(&self) -> bool {
        self.picker.protocol_type() != ProtocolType::Halfblocks
    }

    fn artwork_urls(&self) -> Vec<Option<String>> {
        self.channels
            .iter()
            .map(|channel| channel.artwork_url.clone())
            .collect()
    }

    fn next_refresh_delay(&self) -> Duration {
        let current_time = Utc::now();
        let handover = self
            .channels
            .iter()
            .filter_map(|channel| channel.ends_at.as_deref())
            .filter_map(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc) - current_time)
            .filter_map(|value| value.to_std().ok())
            .min();

        handover
            .map(|value| {
                (value + Duration::from_secs(1))
                    .clamp(Duration::from_secs(5), Duration::from_secs(60))
            })
            .unwrap_or(Duration::from_secs(60))
    }

    fn apply_channel_updates(&mut self, updates: Vec<ChannelUpdate>) {
        let picker = self.picker.clone();
        let encoded_tx = self.encoded_tx.clone();
        let supports_artwork = self.supports_artwork();
        for update in updates {
            let index = update.index;
            // Background responses are external input. A malformed or stale
            // response must never take the terminal down with an out-of-bounds
            // panic; it is safe to discard because the next refresh contains
            // the complete channel snapshot again.
            let Some(channel) = self.channels.get_mut(index) else {
                continue;
            };
            channel.show = update.show;
            channel.description = update.description;
            channel.next_show = update.next_show;
            channel.next_starts_at = update.next_starts_at;
            channel.ends_at = update.ends_at;
            // Channel updates may only seed an empty schedule. The dedicated
            // schedule refresh (apply_schedule_updates) is the sole writer of a
            // populated one; replacing it here would lose entries it fetched.
            if let Some(schedule) = update.schedule
                && channel.schedule.is_empty()
            {
                channel.schedule = schedule;
            }
            // Only remember a URL after its image actually decoded. Recording
            // a transiently failed CDN request here would suppress every later
            // retry because fetch_live_updates sees it as already loaded.
            if update.artwork.is_some() {
                channel.artwork_url = update.artwork_url.clone();
            } else if update.artwork_url.is_none() {
                channel.artwork_url = None;
                channel.artwork = None;
            }
            if let Some(stream) = update.stream {
                channel.stream = stream;
            }
            if supports_artwork
                && let Some(image) = update.artwork
                && let Some(encoded_tx) = &encoded_tx
            {
                let protocol = picker.new_resize_protocol(image);
                match &channel.artwork {
                    // A refresh of an already-shown channel: swap in the new
                    // image; the existing worker re-encodes on the next draw.
                    Some(artwork) => artwork.borrow_mut().replace_protocol(protocol),
                    // First artwork for this channel: spin up its encode worker.
                    None => {
                        channel.artwork = Some(RefCell::new(spawn_artwork_worker(
                            index, protocol, encoded_tx,
                        )));
                    }
                }
            }
        }
    }

    /// Install a freshly decoded cover for one channel. Mirrors the artwork arm
    /// of [`apply_channel_updates`], but arrives independently of the metadata so
    /// the title and description never wait on the image.
    fn apply_artwork(&mut self, index: usize, url: String, image: DynamicImage) {
        if !self.supports_artwork() {
            return;
        }
        let Some(encoded_tx) = self.encoded_tx.clone() else {
            return;
        };
        let picker = self.picker.clone();
        let Some(channel) = self.channels.get_mut(index) else {
            return;
        };
        channel.artwork_url = Some(url);
        let protocol = picker.new_resize_protocol(image);
        match &channel.artwork {
            Some(artwork) => artwork.borrow_mut().replace_protocol(protocol),
            None => {
                channel.artwork = Some(RefCell::new(spawn_artwork_worker(
                    index,
                    protocol,
                    &encoded_tx,
                )));
            }
        }
    }

    fn apply_schedule_updates(&mut self, updates: Vec<ScheduleUpdate>) {
        for update in updates {
            if let Some(channel) = self.channels.get_mut(update.index) {
                channel.schedule = update.schedule;
            }
        }
    }

    fn toggle_playback(&mut self) {
        if self.playing {
            self.stop_player();
            return;
        }

        let stream = self.channels[self.selected].stream.clone();
        match launch_player(&stream) {
            Ok(child) => {
                self.player = Some(child);
                self.playing = true;
                self.buffering = true;
                self.error = None;
            }
            Err(error) => self.error = Some(error.to_string()),
        }
    }

    fn start_default_playback(&mut self) {
        self.toggle_playback();
    }

    fn select_channel(&mut self, index: usize) {
        // Keep this boundary defensive: keyboard shortcuts, media controls,
        // and future UI surfaces all converge here.
        if index >= self.channels.len() || index == self.selected {
            return;
        }

        self.selected = index;

        // A channel selection changes stations, not merely the cursor. Keep the
        // listening state intact and ask the already-running player to load
        // the new stream. This avoids process startup on every switch.
        if self.playing {
            let stream = self.channels[self.selected].stream.clone();
            let result = self
                .player
                .as_mut()
                .context("player process disappeared")
                .and_then(|player| player.change_station(&stream));
            if let Err(error) = result {
                // If the player has died, recover by starting it once more.
                // The common path above is a low-latency IPC station change.
                self.stop_player();
                match launch_player(&stream) {
                    Ok(player) => {
                        self.player = Some(player);
                        self.playing = true;
                        self.buffering = true;
                        self.error = None;
                    }
                    Err(restart_error) => {
                        self.error = Some(format!(
                            "Could not change station: {error}; restart failed: {restart_error}"
                        ))
                    }
                }
            } else {
                self.error = None;
                self.buffering = true;
            }
        }
        self.restart_visualizer();
    }

    fn stop_player(&mut self) {
        self.visualizer = None;
        if let Some(mut player) = self.player.take() {
            player.stop();
        }
        self.playing = false;
        self.buffering = false;
        self.reconnect_attempts = 0;
    }

    fn toggle_visualizer(&mut self) {
        if self.visualizer.take().is_some() {
            self.error = None;
            return;
        }
        if !self.playing {
            self.error = Some("Start a station before opening the visualizer.".to_owned());
            return;
        }
        let identity = self.channels[self.selected].show.clone();
        self.view = View::Listen;
        self.visualizer = Some(LiveVisualizer::new(&identity));
        self.error = None;
    }

    fn restart_visualizer(&mut self) {
        if self.visualizer.is_none() || !self.playing {
            return;
        }
        let identity = self.channels[self.selected].show.clone();
        self.visualizer = Some(LiveVisualizer::new(&identity));
    }

    fn poll_visualizer(&mut self) -> bool {
        self.visualizer.as_mut().is_some_and(LiveVisualizer::poll)
    }

    fn close_visualizer(&mut self) -> bool {
        self.visualizer.take().is_some()
    }

    fn sync_now_playing(&self) {
        let channel = &self.channels[self.selected];
        if let Some(now_playing) = &self.now_playing {
            now_playing.update(&channel.show, channel.name, self.playing && !self.buffering);
        }
    }

    fn pump_now_playing(&self) {
        if let Some(now_playing) = &self.now_playing {
            now_playing.pump();
        }
    }

    fn handle_media_command(&mut self, command: MediaCommand) {
        match command {
            MediaCommand::TogglePlayback => self.toggle_playback(),
            MediaCommand::Play if !self.playing => self.toggle_playback(),
            MediaCommand::Play => {}
            MediaCommand::StopPlayback if self.playing => self.stop_player(),
            MediaCommand::StopPlayback => {}
            MediaCommand::NextStation if self.view != View::Schedule => {
                self.change_station(
                    self.selected
                        .saturating_add(1)
                        .min(self.channels.len().saturating_sub(1)),
                );
            }
            MediaCommand::NextStation => {}
            MediaCommand::PreviousStation if self.view != View::Schedule => {
                self.change_station(self.selected.saturating_sub(1));
            }
            MediaCommand::PreviousStation => {}
        }
    }

    /// Select a station from a direct shortcut or a system media control.
    /// Schedule is intentionally read-only: leaving it should always be an
    /// explicit action rather than an accidental station change.
    fn change_station(&mut self, index: usize) -> bool {
        if self.view == View::Schedule || index >= self.channels.len() {
            return false;
        }
        self.view = View::Listen;
        self.select_channel(index);
        true
    }

    /// Route directional input according to the active surface. Keeping this
    /// in one place prevents key bindings from drifting apart as views grow.
    fn navigate(&mut self, direction: isize) -> bool {
        match self.view {
            View::Listen => {
                if self.channels.is_empty() {
                    return false;
                }
                let target = if direction < 0 {
                    self.selected.saturating_sub(1)
                } else {
                    self.selected.saturating_add(1).min(self.channels.len() - 1)
                };
                self.select_channel(target);
                true
            }
            View::Schedule => false,
            View::Explore => {
                self.move_explore(direction);
                true
            }
        }
    }

    fn toggle_schedule(&mut self) {
        if self.view == View::Schedule {
            self.view = View::Listen;
            return;
        }
        if self.channels[self.selected].kind == ChannelKind::Live {
            self.view = View::Schedule;
            self.error = None;
        } else {
            self.error = Some("Schedules are available on NTS 1 and NTS 2.".to_owned());
        }
    }

    fn toggle_explore(&mut self) {
        if self.view == View::Explore {
            self.view = View::Listen;
            self.error = None;
            return;
        }
        self.explore_selected = self
            .selected
            .saturating_sub(2)
            .min(self.channels.len().saturating_sub(3));
        self.view = View::Explore;
        self.error = None;
    }

    fn move_explore(&mut self, direction: isize) {
        let total = self.channels.len().saturating_sub(2);
        if total == 0 {
            return;
        }
        self.explore_selected =
            (self.explore_selected as isize + direction).rem_euclid(total as isize) as usize;
    }

    fn choose_explore(&mut self) {
        self.select_channel(self.explore_selected + 2);
        self.view = View::Listen;
    }

    fn listen_to_explore(&mut self) {
        let was_playing = self.playing;
        self.choose_explore();
        if !was_playing {
            self.toggle_playback();
        }
    }

    fn dismiss_overlay(&mut self) -> bool {
        if self.view == View::Listen {
            return false;
        }
        self.view = View::Listen;
        self.error = None;
        true
    }

    /// Probe playback health; returns how long the caller should wait before
    /// probing again. Healthy playback re-probes at a steady one-second
    /// cadence. A dead player enters a reconnect loop whose relaunch attempts
    /// back off exponentially (see [`reconnect_delay`]) so a network outage is
    /// a patient retry, not an mpv respawn storm.
    fn poll_player(&mut self) -> Duration {
        const PROBE_INTERVAL: Duration = Duration::from_secs(1);
        if !self.playing {
            return PROBE_INTERVAL;
        }
        let Some(player) = self.player.as_mut() else {
            // A previous probe found the player dead; the backoff has elapsed
            // (the caller waited out our returned delay), so relaunch now.
            // "Reconnecting…" stays up until a healthy probe clears it —
            // spawning mpv is not the same as audio coming back.
            let stream = self.channels[self.selected].stream.clone();
            return match launch_player(&stream) {
                Ok(player) => {
                    self.player = Some(player);
                    self.buffering = true;
                    PROBE_INTERVAL
                }
                Err(error) => {
                    let delay = reconnect_delay(self.reconnect_attempts);
                    self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
                    self.error = Some(format!("Reconnecting… ({error})"));
                    delay
                }
            };
        };
        match player.is_buffering() {
            Ok(buffering) => {
                self.buffering = buffering;
                self.error = None;
                self.reconnect_attempts = 0;
                PROBE_INTERVAL
            }
            Err(_) => {
                if let Some(mut player) = self.player.take() {
                    player.stop();
                }
                self.buffering = true;
                self.error = Some("Reconnecting…".to_owned());
                let delay = reconnect_delay(self.reconnect_attempts);
                self.reconnect_attempts = self.reconnect_attempts.saturating_add(1);
                delay
            }
        }
    }
}

impl Drop for App {
    fn drop(&mut self) {
        // Keep mpv tied to the UI lifetime even if `run` returns early with
        // an error. `Child` itself does not terminate its process on drop.
        self.stop_player();
        if let Some(now_playing) = &self.now_playing {
            now_playing.clear();
        }
    }
}

impl Player {
    fn start(stream: &str) -> Result<Self> {
        let ipc_path = std::env::temp_dir().join(format!(
            "nts-mpv-{}-{}.sock",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let _ = fs::remove_file(&ipc_path);
        let ipc_arg = format!("--input-ipc-server={}", ipc_path.display());
        let child = Command::new("mpv")
            .args([
                "--no-video",
                "--really-quiet",
                "--input-terminal=no",
                // The app owns macOS media controls through Now Playing. If
                // mpv receives the same hardware key, it pauses its own live
                // buffer instead of letting us stop and reconnect at live edge.
                "--input-media-keys=no",
                "--cache-secs=0.2",
                "--demuxer-readahead-secs=0.2",
                "--cache-pause-wait=0.1",
                // NTS's direct streams identify their audio format quickly.
                // Avoid mpv's conservative default probe window on station changes.
                "--demuxer-lavf-probesize=32768",
                "--demuxer-lavf-analyzeduration=0",
                &ipc_arg,
                stream,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => {
                    if cfg!(target_os = "macos") {
                        anyhow::anyhow!("Install mpv first: brew install mpv")
                    } else {
                        anyhow::anyhow!("Install mpv first (e.g. apt install mpv)")
                    }
                }
                _ => anyhow::Error::new(error).context("start mpv"),
            })?;

        MPV_PID.store(child.id() as i32, Ordering::Relaxed);
        Ok(Self { child, ipc_path })
    }

    fn change_station(&mut self, stream: &str) -> Result<()> {
        if let Some(status) = self.child.try_wait().context("check mpv status")? {
            anyhow::bail!("mpv exited with {status}");
        }

        let mut socket = connect_ipc(&self.ipc_path)?;
        let command = json!({ "command": ["loadfile", stream, "replace"] }).to_string();
        socket
            .write_all(command.as_bytes())
            .and_then(|_| socket.write_all(b"\n"))
            .context("send station change command to mpv")
    }

    fn is_buffering(&mut self) -> Result<bool> {
        if let Some(status) = self.child.try_wait().context("check mpv status")? {
            anyhow::bail!("mpv exited with {status}");
        }

        let mut socket = connect_ipc(&self.ipc_path)?;
        socket
            .set_read_timeout(Some(Duration::from_millis(140)))
            .context("set mpv IPC read timeout")?;
        // `core-idle` is the stable mpv IPC signal for a player that has not
        // opened a stream yet (or has returned to idle). Cache-only properties
        // are unavailable during that transition, which made an earlier probe
        // mistake normal startup for a player crash.
        let command = json!({ "command": ["get_property", "core-idle"] }).to_string();
        socket
            .write_all(command.as_bytes())
            .and_then(|_| socket.write_all(b"\n"))
            .context("query mpv playback state")?;

        let mut response = String::new();
        let mut reader = io::BufReader::new(socket);
        use std::io::BufRead;
        reader
            .read_line(&mut response)
            .context("read mpv playback state")?;
        let value: Value = serde_json::from_str(&response).context("decode mpv playback state")?;
        value
            .get("data")
            .and_then(Value::as_bool)
            .context("mpv did not return a playback state")
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        MPV_PID.store(0, Ordering::Relaxed);
        let _ = fs::remove_file(&self.ipc_path);
    }
}

fn connect_ipc(path: &PathBuf) -> Result<UnixStream> {
    let deadline = Instant::now() + Duration::from_millis(350);
    loop {
        match UnixStream::connect(path) {
            Ok(socket) => return Ok(socket),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(15));
            }
            Err(error) => return Err(error).context("connect to mpv IPC socket"),
        }
    }
}

fn fallback_channels() -> Vec<Channel> {
    vec![
        Channel {
            number: "1",
            name: "NTS 1",
            kind: ChannelKind::Live,
            stream: NTS_1_STREAM.to_owned(),
            show: "Live transmission".to_owned(),
            description: "Music to make your day feel a little less ordinary.".to_owned(),
            next_show: "Schedule updating".to_owned(),
            next_starts_at: "—".to_owned(),
            ends_at: None,
            schedule: Vec::new(),
            artwork_url: None,
            artwork: None,
        },
        Channel {
            number: "2",
            name: "NTS 2",
            kind: ChannelKind::Live,
            stream: NTS_2_STREAM.to_owned(),
            show: "Live transmission".to_owned(),
            description: "A second door into the NTS universe.".to_owned(),
            next_show: "Schedule updating".to_owned(),
            next_starts_at: "—".to_owned(),
            ends_at: None,
            schedule: Vec::new(),
            artwork_url: None,
            artwork: None,
        },
        mixtape_channel(
            "Poolside",
            "Poolside",
            "https://stream-mixtape-geo.ntslive.net/mixtape4",
        ),
        mixtape_channel(
            "Slow Focus",
            "Slow Focus",
            "https://stream-mixtape-geo.ntslive.net/mixtape",
        ),
        mixtape_channel(
            "Low Key",
            "100-percent-hip-hop",
            "https://stream-mixtape-geo.ntslive.net/mixtape2",
        ),
        mixtape_channel(
            "Memory Lane",
            "Memory Lane",
            "https://stream-mixtape-geo.ntslive.net/mixtape6",
        ),
        mixtape_channel(
            "4 To The Floor",
            "4 To The Floor",
            "https://stream-mixtape-geo.ntslive.net/mixtape5",
        ),
        mixtape_channel(
            "Island Time",
            "Island Time",
            "https://stream-mixtape-geo.ntslive.net/mixtape21",
        ),
        mixtape_channel(
            "The Tube",
            "The Tube",
            "https://stream-mixtape-geo.ntslive.net/mixtape26",
        ),
        mixtape_channel(
            "Sheet Music",
            "Sheet Music",
            "https://stream-mixtape-geo.ntslive.net/mixtape35",
        ),
    ]
}

fn mixtape_channel(name: &'static str, label: &'static str, stream: &'static str) -> Channel {
    Channel {
        number: "∞",
        name,
        kind: ChannelKind::Mixtape,
        stream: stream.to_owned(),
        show: label.to_owned(),
        description: "Infinite Mixtape · loading NTS details".to_owned(),
        next_show: String::new(),
        next_starts_at: String::new(),
        ends_at: None,
        schedule: Vec::new(),
        artwork_url: None,
        artwork: None,
    }
}

/// A live channel's metadata plus the URLs of its candidate cover art. Artwork
/// is fetched separately (see `request_live_refresh`) so a slow download never
/// delays the title, description, and schedule that ride along in `update`.
struct LiveUpdate {
    update: ChannelUpdate,
    medium: Option<String>,
    thumb: Option<String>,
}

fn fetch_live_updates(client: &Client) -> Result<Vec<LiveUpdate>> {
    let live: LiveResponse = client.get(LIVE_API).send()?.error_for_status()?.json()?;
    let mut updates = Vec::with_capacity(2);

    for result in live.results {
        let Some(index) = (match result.channel_name.as_str() {
            "1" => Some(0),
            "2" => Some(1),
            _ => None,
        }) else {
            continue;
        };
        let (now, next) = active_and_next(result.now, result.next, result.next2);
        let ends_at = now.end_timestamp.clone();
        let Details {
            name: now_name,
            description: now_description,
            media: now_media,
        } = resolve_details(client, &now);
        let Details {
            name: next_name, ..
        } = next.embeds.details.unwrap_or_default();

        updates.push(LiveUpdate {
            update: ChannelUpdate {
                index,
                show: now_name
                    .or(now.broadcast_title)
                    .unwrap_or_else(|| "Live transmission".to_owned()),
                description: now_description.unwrap_or_default(),
                next_show: next_name
                    .or(next.broadcast_title)
                    .unwrap_or_else(|| "Coming up soon".to_owned()),
                next_starts_at: broadcast_time(next.start_timestamp.as_deref()),
                ends_at,
                // The dedicated schedule fetch owns `channel.schedule`; a
                // two-entry now/next list from here would shrink the six-hour
                // modal to two rows for most of each 15-minute window.
                schedule: None,
                // Carry the candidate URL (not a decoded image): apply leaves
                // existing artwork untouched while the real cover streams in via
                // BackgroundUpdate::Artwork, and clears it only when truly absent.
                artwork_url: now_media
                    .picture_medium
                    .clone()
                    .or(now_media.picture_thumb.clone()),
                artwork: None,
                stream: None,
            },
            medium: now_media.picture_medium,
            thumb: now_media.picture_thumb,
        });
    }

    Ok(updates)
}

fn fetch_schedule_updates() -> Result<Vec<ScheduleUpdate>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let current_time = Utc::now();
    let mut updates = Vec::with_capacity(2);

    for index in 0..2 {
        // Fetch each channel's schedule independently so one failure does not
        // discard the other; missing ones are retried on the next refresh.
        let response = client
            .get(format!("{SCHEDULE_API}/{}?past_days=0", index + 1))
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.json::<ScheduleResponse>());
        let Ok(schedule) = response else {
            continue;
        };
        let broadcasts = schedule
            .results
            .into_iter()
            .flat_map(|day| day.broadcasts)
            .filter(|broadcast| !broadcast_has_ended(broadcast, current_time))
            .take(6)
            .collect::<Vec<_>>();
        updates.push(ScheduleUpdate {
            index,
            schedule: schedule_from_broadcasts(&broadcasts),
        });
    }
    Ok(updates)
}

/// NTS API aliases for the eight Infinite Mixtapes, in channel order (indices
/// 2..10 of `fallback_channels`). Used for both metadata fetches and the CLI
/// station argument.
const MIXTAPES: [&str; 8] = [
    "poolside",
    "slow-focus",
    "100-percent-hip-hop",
    "memory-lane",
    "4-to-the-floor",
    "island-time",
    "the-tube",
    "sheet-music",
];

fn fetch_mixtape_updates(known_artwork_urls: Vec<Option<String>>) -> Result<Vec<ChannelUpdate>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let mut updates = Vec::with_capacity(MIXTAPES.len());
    for (offset, alias) in MIXTAPES.iter().enumerate() {
        // Fetch each mixtape independently: a single slow or 404ing alias should
        // not discard the others. Failures are skipped and retried on the next
        // mixtape refresh (see `run`).
        let response = client
            .get(format!("{MIXTAPE_API}/{alias}"))
            .send()
            .and_then(|response| response.error_for_status())
            .and_then(|response| response.json::<MixtapeResponse>());
        let Ok(mixtape) = response else {
            continue;
        };
        let index = offset + 2;
        let (artwork_url, artwork) = fetch_artwork(
            &client,
            mixtape.media.picture_medium.as_deref(),
            mixtape.media.picture_thumb.as_deref(),
            known_artwork_urls[index].as_deref(),
        );
        updates.push(ChannelUpdate {
            index,
            show: mixtape.title,
            description: mixtape.subtitle.or(mixtape.description).unwrap_or_default(),
            next_show: String::new(),
            next_starts_at: String::new(),
            ends_at: None,
            schedule: None,
            artwork_url,
            artwork,
            stream: mixtape.audio_stream_endpoint,
        });
    }
    Ok(updates)
}

fn schedule_from_broadcasts(broadcasts: &[Broadcast]) -> Vec<ScheduleEntry> {
    broadcasts
        .iter()
        .map(|broadcast| ScheduleEntry {
            starts_at: broadcast_time(broadcast.start_timestamp.as_deref()),
            title: broadcast
                .embeds
                .details
                .as_ref()
                .and_then(|details| details.name.clone())
                .or_else(|| broadcast.broadcast_title.clone())
                .unwrap_or_else(|| "Live transmission".to_owned()),
        })
        .collect()
}

fn active_and_next(
    now: Broadcast,
    next: Broadcast,
    next2: Option<Broadcast>,
) -> (Broadcast, Broadcast) {
    let mut broadcasts = vec![now, next];
    if let Some(next2) = next2 {
        broadcasts.push(next2);
    }
    let current_time = Utc::now();
    let active_index = broadcasts
        .iter()
        .position(|broadcast| !broadcast_has_ended(broadcast, current_time))
        .unwrap_or(0);
    let active = broadcasts[active_index].clone();
    let following = broadcasts
        .get(active_index + 1)
        .cloned()
        .unwrap_or_else(|| active.clone());
    (active, following)
}

fn broadcast_has_ended(broadcast: &Broadcast, current_time: DateTime<Utc>) -> bool {
    broadcast
        .end_timestamp
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc) <= current_time)
        .unwrap_or(false)
}

fn broadcast_time(timestamp: Option<&str>) -> String {
    timestamp
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Local).format("%H:%M %Z").to_string())
        .unwrap_or_else(|| "—".to_owned())
}

fn fetch_image(client: &Client, url: &str) -> Result<DynamicImage> {
    let bytes = client
        .get(url)
        // Generous because the largest covers (NTS 1's square art can top
        // 400KB) were timing out at 4s on slower links; artwork now fetches off
        // the metadata path, so a longer wait costs nothing the user sees.
        .timeout(Duration::from_secs(10))
        .send()?
        .error_for_status()?
        .bytes()?;
    image::load_from_memory(&bytes).context("decode NTS artwork")
}

/// Resolve a broadcast's full details — title, description, and artwork. The
/// live API only inlines a details block for the `now`/`next` slots; a promoted
/// (`next2`) or otherwise sparse broadcast arrives with none, so its title,
/// description, and artwork all go missing together. Every broadcast links to
/// its episode resource, which carries the complete details, so fall back to
/// that when the inline block is incomplete. Only the fallback path makes a
/// request.
fn resolve_details(client: &Client, broadcast: &Broadcast) -> Details {
    let inline = broadcast.embeds.details.clone().unwrap_or_default();
    // Title, description, and either artwork size are independent fields; any of
    // them can be absent on its own. Only skip the request when all are present.
    let has_artwork = inline.media.picture_medium.is_some() || inline.media.picture_thumb.is_some();
    if inline.name.is_some() && inline.description.is_some() && has_artwork {
        return inline;
    }
    let Some(url) = broadcast.episode_url() else {
        return inline;
    };
    match fetch_episode_details(client, url) {
        Ok(episode) => merge_details(inline, episode),
        Err(_) => inline,
    }
}

/// Fill in only the fields the inline block was missing, preferring inline data
/// (which reflects the live slot) over the episode resource for every field.
fn merge_details(mut inline: Details, episode: Details) -> Details {
    inline.name = inline.name.or(episode.name);
    inline.description = inline.description.or(episode.description);
    inline.media.picture_medium = inline.media.picture_medium.or(episode.media.picture_medium);
    inline.media.picture_thumb = inline.media.picture_thumb.or(episode.media.picture_thumb);
    inline
}

/// The episode resource exposes `name`, `description`, and `media` at its top
/// level, which deserialize straight into [`Details`].
fn fetch_episode_details(client: &Client, url: &str) -> Result<Details> {
    client
        .get(url)
        .timeout(Duration::from_secs(4))
        .send()?
        .error_for_status()?
        .json()
        .context("decode NTS episode details")
}

fn fetch_artwork(
    client: &Client,
    medium: Option<&str>,
    thumb: Option<&str>,
    known_url: Option<&str>,
) -> (Option<String>, Option<DynamicImage>) {
    for url in [medium, thumb].into_iter().flatten() {
        // The stored URL is only set after a successful decode, so it is safe
        // to skip. A failed URL remains absent and will be retried later.
        if Some(url) == known_url {
            return (Some(url.to_owned()), None);
        }
        if let Ok(image) = fetch_image(client, url) {
            return (Some(url.to_owned()), Some(crop_to_landscape(image)));
        }
    }
    (medium.or(thumb).map(str::to_owned), None)
}

/// Center-crop to NTS's landscape aspect. The cards are artwork-led and expect
/// a consistent landscape crop; the resize protocol then scales this to fit
/// whatever cell area it is rendered into.
fn crop_to_landscape(image: DynamicImage) -> DynamicImage {
    let (width, height) = (image.width(), image.height());
    let target_height = ((width as f32) / NTS_ARTWORK_ASPECT).round() as u32;
    if target_height <= height {
        image.crop_imm(0, (height - target_height) / 2, width, target_height.max(1))
    } else {
        let target_width = ((height as f32) * NTS_ARTWORK_ASPECT).round() as u32;
        let target_width = target_width.clamp(1, width);
        image.crop_imm((width - target_width) / 2, 0, target_width, height)
    }
}

/// Delay before the next reconnect attempt: 1s, 2s, 4s… capped at 30s so a
/// long outage still recovers promptly once the stream returns.
fn reconnect_delay(attempts: u32) -> Duration {
    Duration::from_secs(2u64.pow(attempts.min(5)).min(30))
}

fn launch_player(stream: &str) -> Result<Player> {
    // Spawn directly; a missing binary surfaces as the install hint from
    // `Player::start`. Probing with `mpv --version` first would block the
    // caller on a synchronous subprocess for no added safety.
    Player::start(stream)
}

fn drain_startup_responses() -> Result<()> {
    while event::poll(Duration::ZERO)? {
        let _ = event::read()?;
    }
    Ok(())
}

/// Create a [`ThreadProtocol`] backed by a dedicated worker thread that resizes
/// and encodes artwork off the UI thread. Each encoded result is tagged with
/// `index` so the event loop can route it back to the right channel. The worker
/// exits when the channel's `ThreadProtocol` (and thus its sender) is dropped.
fn spawn_artwork_worker(
    index: usize,
    protocol: StatefulProtocol,
    encoded_tx: &mpsc::Sender<(usize, ResizeResponse)>,
) -> ThreadProtocol {
    let (request_tx, request_rx) = mpsc::channel::<ResizeRequest>();
    let encoded_tx = encoded_tx.clone();
    thread::spawn(move || {
        while let Ok(request) = request_rx.recv() {
            if let Ok(response) = request.resize_encode() {
                let _ = encoded_tx.send((index, response));
            }
        }
    });
    ThreadProtocol::new(request_tx, Some(protocol))
}

fn request_live_refresh(
    sender: &mpsc::Sender<BackgroundUpdate>,
    artwork_urls: Vec<Option<String>>,
) {
    let sender = sender.clone();
    thread::spawn(move || {
        let client = match Client::builder().timeout(Duration::from_secs(10)).build() {
            Ok(client) => client,
            Err(error) => {
                let _ = sender.send(BackgroundUpdate::Live(Err(error.to_string())));
                return;
            }
        };
        let live = match fetch_live_updates(&client) {
            Ok(live) => live,
            Err(error) => {
                let _ = sender.send(BackgroundUpdate::Live(Err(error.to_string())));
                return;
            }
        };
        // Send the text the moment it is parsed so the UI fills in immediately,
        // then chase each channel's cover on its own thread. Splitting the work
        // keeps one slow or oversized image from stalling the others or the text.
        let jobs: Vec<(usize, Option<String>, Option<String>)> = live
            .iter()
            .map(|live| (live.update.index, live.medium.clone(), live.thumb.clone()))
            .collect();
        let metadata = live.into_iter().map(|live| live.update).collect();
        let _ = sender.send(BackgroundUpdate::Live(Ok(metadata)));

        for (index, medium, thumb) in jobs {
            let known = artwork_urls.get(index).cloned().flatten();
            let sender = sender.clone();
            let client = client.clone();
            thread::spawn(move || {
                let (url, image) = fetch_artwork(
                    &client,
                    medium.as_deref(),
                    thumb.as_deref(),
                    known.as_deref(),
                );
                // fetch_artwork yields an image only for a fresh, successful
                // download; an unchanged or failed cover sends nothing and is
                // retried on the next live refresh.
                if let (Some(url), Some(image)) = (url, image) {
                    let _ = sender.send(BackgroundUpdate::Artwork { index, url, image });
                }
            });
        }
    });
}

fn request_schedule_refresh(sender: &mpsc::Sender<BackgroundUpdate>) {
    let sender = sender.clone();
    thread::spawn(move || {
        let _ = sender.send(BackgroundUpdate::Schedules(
            fetch_schedule_updates().map_err(|error| error.to_string()),
        ));
    });
}

fn request_mixtape_refresh(
    sender: &mpsc::Sender<BackgroundUpdate>,
    artwork_urls: Vec<Option<String>>,
) {
    let sender = sender.clone();
    thread::spawn(move || {
        let _ = sender.send(BackgroundUpdate::Mixtapes(
            fetch_mixtape_updates(artwork_urls).map_err(|error| error.to_string()),
        ));
    });
}

fn run(
    terminal: &mut DefaultTerminal,
    app: &mut App,
    media_rx: mpsc::Receiver<MediaCommand>,
) -> Result<()> {
    let mut dirty = true;
    let mut title_frame = 0;
    let mut title_dirty = true;
    let mut next_title_tick = Instant::now();
    let (update_tx, update_rx) = mpsc::channel::<BackgroundUpdate>();
    // Per-channel encode workers resize+encode artwork off the UI thread and
    // send the finished protocol back here, keeping the draw pass non-blocking.
    let (encoded_tx, encoded_rx) = mpsc::channel::<(usize, ResizeResponse)>();
    app.encoded_tx = Some(encoded_tx);
    let mut schedule_in_flight = false;
    // Fetch live data on a background thread right away. `App::load` returns
    // with fallback copy so this first frame is already drawn and interactive;
    // the real show titles and artwork arrive via `BackgroundUpdate::Live`.
    let mut refresh_in_flight = true;
    request_live_refresh(&update_tx, app.artwork_urls());
    let mut next_refresh = Instant::now() + app.next_refresh_delay();
    let mut next_schedule_refresh = Instant::now();
    let mut next_player_probe = Instant::now() + Duration::from_millis(300);

    // Mixtape metadata rarely changes, so it refreshes on a slow cadence; but it
    // must still retry, because a failed startup fetch otherwise leaves the
    // Explore view on "loading" copy for the life of the process.
    let mut mixtape_in_flight = true;
    let mut next_mixtape_refresh = Instant::now() + MIXTAPE_REFRESH_INTERVAL;
    request_mixtape_refresh(&update_tx, app.artwork_urls());
    loop {
        app.pump_now_playing();
        let now = Instant::now();
        while let Ok(command) = media_rx.try_recv() {
            app.handle_media_command(command);
            app.sync_now_playing();
            dirty = true;
            title_dirty = true;
        }
        if app.poll_visualizer() {
            dirty = true;
        }
        while let Ok((index, response)) = encoded_rx.try_recv() {
            if let Some(artwork) = app.channels.get(index).and_then(|c| c.artwork.as_ref())
                && artwork.borrow_mut().update_resized_protocol(response)
            {
                dirty = true;
            }
        }
        if title_dirty || (app.playing && now >= next_title_tick) {
            if app.playing {
                title_frame = (title_frame + 1) % BRAILLE_SPINNER.len();
            } else {
                title_frame = 0;
            }
            set_terminal_title(app, title_frame)?;
            title_dirty = false;
            next_title_tick = now + Duration::from_millis(120);
        }
        while let Ok(update) = update_rx.try_recv() {
            match update {
                BackgroundUpdate::Live(result) => {
                    refresh_in_flight = false;
                    match result {
                        Ok(updates) => {
                            app.apply_channel_updates(updates);
                            app.sync_now_playing();
                            if app.connection_lost {
                                app.connection_lost = false;
                            }
                            dirty = true;
                            title_dirty = true;
                            next_refresh = Instant::now() + app.next_refresh_delay();
                        }
                        // Surface the failure and retry soon rather than sitting
                        // on stale fallback copy until the next 60s tick.
                        Err(_) => {
                            app.connection_lost = true;
                            dirty = true;
                            next_refresh = Instant::now() + NETWORK_RETRY_DELAY;
                        }
                    }
                }
                BackgroundUpdate::Schedules(result) => {
                    schedule_in_flight = false;
                    match result {
                        Ok(updates) => {
                            app.apply_schedule_updates(updates);
                            dirty = true;
                        }
                        Err(_) => next_schedule_refresh = Instant::now() + NETWORK_RETRY_DELAY,
                    }
                }
                BackgroundUpdate::Mixtapes(result) => {
                    mixtape_in_flight = false;
                    match result {
                        Ok(updates) => {
                            app.apply_channel_updates(updates);
                            app.sync_now_playing();
                            dirty = true;
                            next_mixtape_refresh = Instant::now() + MIXTAPE_REFRESH_INTERVAL;
                        }
                        Err(_) => next_mixtape_refresh = Instant::now() + NETWORK_RETRY_DELAY,
                    }
                }
                BackgroundUpdate::Artwork { index, url, image } => {
                    app.apply_artwork(index, url, image);
                    dirty = true;
                }
            }
        }
        if !refresh_in_flight && now >= next_refresh {
            next_refresh = now + Duration::from_secs(60);
            refresh_in_flight = true;
            request_live_refresh(&update_tx, app.artwork_urls());
        }
        if !schedule_in_flight && now >= next_schedule_refresh {
            next_schedule_refresh = now + Duration::from_secs(15 * 60);
            schedule_in_flight = true;
            request_schedule_refresh(&update_tx);
        }
        if !mixtape_in_flight && now >= next_mixtape_refresh {
            next_mixtape_refresh = now + MIXTAPE_REFRESH_INTERVAL;
            mixtape_in_flight = true;
            request_mixtape_refresh(&update_tx, app.artwork_urls());
        }
        if app.playing && now >= next_player_probe {
            let probe_delay = app.poll_player();
            app.sync_now_playing();
            next_player_probe = now + probe_delay;
            dirty = true;
            title_dirty = true;
        }
        if dirty {
            terminal.draw(|frame| {
                draw(frame, app);
            })?;
            dirty = false;
        }
        let mut poll_interval = Duration::from_millis(120);
        if app.playing {
            poll_interval =
                poll_interval.min(next_title_tick.saturating_duration_since(Instant::now()));
        }
        if !refresh_in_flight {
            poll_interval =
                poll_interval.min(next_refresh.saturating_duration_since(Instant::now()));
        }
        if !schedule_in_flight {
            poll_interval =
                poll_interval.min(next_schedule_refresh.saturating_duration_since(Instant::now()));
        }
        if !mixtape_in_flight {
            poll_interval =
                poll_interval.min(next_mixtape_refresh.saturating_duration_since(Instant::now()));
        }
        if app.playing {
            poll_interval =
                poll_interval.min(next_player_probe.saturating_duration_since(Instant::now()));
        }
        if app.visualizer.is_some() {
            poll_interval = poll_interval.min(Duration::from_millis(16));
        }
        if event::poll(poll_interval)? {
            let event = event::read()?;
            if let Event::Resize(_, _) = event {
                // Ratatui resizes its buffers on the next draw; without this
                // a stopped app keeps a stale layout until the next refresh.
                dirty = true;
                continue;
            }
            let Event::Key(key) = event else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            // Raw mode disables ISIG, so ctrl+c arrives as a key event rather
            // than SIGINT; honor the universal interrupt reflex.
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                return Ok(());
            }
            let had_overlay = app.visualizer.is_some() || app.view != View::Listen;
            dirty = match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(()),
                // Esc dismisses; it never quits. Quitting the radio should be
                // deliberate (`q` or ctrl+c), not a reflex keypress.
                KeyCode::Esc => app.close_visualizer() || app.dismiss_overlay(),
                KeyCode::Char(digit @ '1'..='9') => {
                    app.change_station(digit as usize - '1' as usize)
                }
                KeyCode::Char('0') => app.change_station(9),
                KeyCode::Char('e') | KeyCode::Char('E') => {
                    app.toggle_explore();
                    true
                }
                KeyCode::Char('v') | KeyCode::Char('V') => {
                    app.toggle_visualizer();
                    true
                }
                KeyCode::Up | KeyCode::Left | KeyCode::Char('k') | KeyCode::Char('h') => {
                    app.navigate(-1)
                }
                KeyCode::Down | KeyCode::Right | KeyCode::Char('j') | KeyCode::Char('l') => {
                    app.navigate(1)
                }
                KeyCode::Enter | KeyCode::Char(' ') => {
                    if app.view == View::Explore {
                        app.listen_to_explore();
                    } else {
                        app.toggle_playback();
                    }
                    true
                }
                KeyCode::Char('s') | KeyCode::Char('S') => {
                    if app.view == View::Explore {
                        app.error = Some("Choose a live channel to see its schedule.".to_owned());
                    } else {
                        app.toggle_schedule();
                    }
                    true
                }
                _ => false,
            };
            let has_overlay = app.visualizer.is_some() || app.view != View::Listen;
            if dirty && had_overlay != has_overlay {
                // A modal clears cells that may be occupied by a terminal
                // graphics-protocol image. Reset Ratatui's previous buffer so
                // overlays and uncovered artwork start from a clean canvas.
                terminal.clear()?;
            }
            title_dirty = title_dirty || dirty;
            if dirty {
                app.sync_now_playing();
            }
        }
    }
}

const BRAILLE_SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

fn set_terminal_title(app: &App, frame: usize) -> Result<()> {
    let channel = &app.channels[app.selected];
    let prefix = if app.playing {
        BRAILLE_SPINNER[frame]
    } else {
        "⠂"
    };
    let station = if channel.kind == ChannelKind::Live {
        format!("NTS {}", channel.number)
    } else {
        "NTS Mix".to_owned()
    };
    let title = format!("{prefix} {station} — {}", title_copy(&channel.show));
    execute!(io::stdout(), SetTitle(title)).context("update terminal title")
}

fn title_copy(value: &str) -> String {
    const LIMIT: usize = 72;
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.chars().count() <= LIMIT {
        value
    } else {
        format!("{}…", value.chars().take(LIMIT - 1).collect::<String>())
    }
}

fn draw(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let compact = area.width < 110 || area.height < 30;
    // Paint our own dark canvas so INK text stays high-contrast regardless of
    // the terminal's default background colour.
    frame.render_widget(Block::default().style(Style::default().bg(BASE)), area);
    // Terminal image protocols are rendered independently of Ratatui's cell
    // buffer. Render the visualizer as an exclusive surface so an artwork
    // update cannot leap above its braille layer during a station change.
    if app.visualizer.is_some() {
        frame.render_widget(Clear, area);
        frame.render_widget(Block::default().style(Style::default().bg(SURFACE)), area);
        draw_visualizer_modal(frame, app, compact);
        return;
    }
    // Terminal graphics-protocol images do not share Ratatui's z-order. Keep
    // the regular UI behind overlays, but omit its artwork while a modal is
    // open so a late image placement cannot paint through the modal.
    let show_background_artwork = app.view == View::Listen;
    if compact {
        let main = Layout::default()
            .direction(Direction::Vertical)
            .margin(2)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(8),
                Constraint::Length(1),
            ])
            .split(area);
        draw_compact_header(frame, app, main[0]);
        draw_compact(frame, app, main[1], show_background_artwork);
        render_footer(frame, app, main[2], true);
        render_error(frame, app, main[2]);
        render_terminal_hint(frame, app, main[2]);
        match app.view {
            View::Schedule => draw_schedule_modal(frame, app, true),
            View::Explore => draw_explore_modal(frame, app, true),
            View::Listen => {}
        }
        return;
    }

    let main = Layout::default()
        .direction(Direction::Vertical)
        .margin(2)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(area);

    let title = Line::from(vec![
        Span::styled("NTS", Style::default().fg(INK).add_modifier(Modifier::BOLD)),
        Span::styled("  —  DON'T ASSUME", Style::default().fg(MUTED)),
    ]);
    frame.render_widget(Paragraph::new(title), main[0]);
    let status = status_copy(app);
    frame.render_widget(
        Paragraph::new(status).style(Style::default().fg(MUTED)),
        main[1],
    );

    draw_wide_channels(frame, app, main[2], show_background_artwork);
    render_footer(frame, app, main[3], false);
    render_error(frame, app, main[3]);
    render_terminal_hint(frame, app, main[3]);
    match app.view {
        View::Schedule => draw_schedule_modal(frame, app, false),
        View::Explore => draw_explore_modal(frame, app, false),
        View::Listen => {}
    }
}

fn render_footer(frame: &mut Frame<'_>, app: &App, area: Rect, compact: bool) {
    let footer = if compact {
        if app.playing {
            "↑↓ browse  •  e explore  •  s schedule  •  v visualizer  •  space stop"
        } else {
            "↑↓ browse  •  e explore  •  s schedule  •  v visualizer  •  space listen"
        }
    } else if app.playing {
        "↑↓ / j k change station    •    1-0 stations    •    e explore    •    s schedule    •    v visualizer    •    space stop    •    q quit"
    } else {
        "↑↓ / j k select    •    1-0 stations    •    e explore    •    s schedule    •    v visualizer    •    space listen    •    q quit"
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        area,
    );
}

fn render_error(frame: &mut Frame<'_>, app: &App, footer_area: Rect) {
    // An explicit notice (playback errors, user hints) takes priority; otherwise
    // fall back to the connectivity notice so a failed fetch never looks like a
    // silent hang.
    let notice = app.error.as_deref().or_else(|| {
        app.connection_lost
            .then_some("◌ Couldn't reach NTS — retrying…")
    });
    if let Some(notice) = notice {
        let error_area = Rect {
            y: footer_area.y.saturating_sub(1),
            height: 1,
            ..footer_area
        };
        frame.render_widget(
            Paragraph::new(notice).style(Style::default().fg(SIGNAL)),
            error_area,
        );
    }
}

/// Nudge text-only terminals toward an image-capable one. Shares the row above
/// the footer with the error line, which takes priority when present.
fn render_terminal_hint(frame: &mut Frame<'_>, app: &App, footer_area: Rect) {
    if app.supports_artwork() || app.error.is_some() || app.connection_lost {
        return;
    }
    let hint_area = Rect {
        y: footer_area.y.saturating_sub(1),
        height: 1,
        ..footer_area
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("◌ ", Style::default().fg(SIGNAL)),
            Span::styled(
                "Artwork hidden — open NTS in Ghostty, iTerm2, or Kitty to see cover art.",
                Style::default().fg(MUTED),
            ),
        ])),
        hint_area,
    );
}

fn status_copy(app: &App) -> String {
    if !app.playing {
        return "● READY · PRESS SPACE TO LISTEN".to_owned();
    }
    let station = &app.channels[app.selected];
    if app.buffering {
        format!("◌ BUFFERING · {}", station.name)
    } else if station.kind == ChannelKind::Live {
        format!("● ON AIR · {}", station.name)
    } else {
        format!("● PLAYING · {}", station.name)
    }
}

fn draw_visualizer_modal(frame: &mut Frame<'_>, app: &App, compact: bool) {
    let area = frame.area();
    let modal = centered_rect(
        area,
        if compact {
            area.width.saturating_sub(4)
        } else {
            96
        },
        if compact {
            area.height.saturating_sub(4)
        } else {
            22
        },
    );
    frame.render_widget(Clear, modal);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(BORDER))
        .style(Style::default().bg(SURFACE))
        .padding(ratatui::widgets::Padding::new(2, 2, 1, 1));
    let inner = block.inner(modal);
    frame.render_widget(block, modal);
    if let Some(visualizer) = &app.visualizer {
        frame.render_widget(
            Paragraph::new(visualizer.braille(inner.width, inner.height))
                .style(Style::default().fg(SIGNAL)),
            inner,
        );
    }
}

fn visible_channels(selected: usize, total: usize, slots: usize) -> Vec<usize> {
    if total <= slots {
        return (0..total).collect();
    }
    let start = selected.saturating_sub(slots / 2).min(total - slots);
    (start..start + slots).collect()
}

fn draw_schedule_modal(frame: &mut Frame<'_>, app: &App, compact: bool) {
    let area = frame.area();
    let modal = centered_rect(
        area,
        if compact {
            area.width.saturating_sub(4)
        } else {
            86
        },
        if compact {
            area.height.saturating_sub(6)
        } else {
            18
        },
    );
    frame.render_widget(Clear, modal);
    let channel = &app.channels[app.selected];
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(INK))
        .style(Style::default().bg(SURFACE))
        .title(Line::styled(
            format!(" SCHEDULE  /  {} ", channel.name),
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ))
        .padding(ratatui::widgets::Padding::new(2, 2, 1, 1));
    let inner = block.inner(modal);
    frame.render_widget(block, modal);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(6),
            Constraint::Length(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("LOCAL TIME", Style::default().fg(MUTED)),
            Span::styled("  ·  NEXT SIX HOURS", Style::default().fg(MUTED)),
        ])),
        sections[0],
    );
    render_schedule(frame, channel, sections[1], compact);
    frame.render_widget(
        Paragraph::new(if compact {
            "s / esc close  •  space play / stop"
        } else {
            "s / esc close    •    space play / stop"
        })
        .style(Style::default().fg(MUTED)),
        sections[2],
    );
    render_error(frame, app, sections[2]);
}

fn render_schedule(frame: &mut Frame<'_>, channel: &Channel, area: Rect, compact: bool) {
    let limit = if compact { 4 } else { 6 };
    let entries = channel.schedule.iter().take(limit).collect::<Vec<_>>();
    if entries.is_empty() {
        frame.render_widget(
            Paragraph::new("Loading the local schedule…").style(Style::default().fg(MUTED)),
            area,
        );
        return;
    }

    let lines = entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| {
            let marker = if index == 0 { "→" } else { " " };
            Line::from(vec![
                Span::styled(format!("{marker}  "), Style::default().fg(SIGNAL)),
                Span::styled(
                    format!("{}  ", entry.starts_at),
                    Style::default().fg(if index == 0 { INK } else { MUTED }),
                ),
                Span::styled(
                    &entry.title,
                    Style::default()
                        .fg(if index == 0 { INK } else { MUTED })
                        .add_modifier(if index == 0 {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                ),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::LEFT)
                    .border_style(Style::default().fg(SIGNAL))
                    .padding(ratatui::widgets::Padding::left(2)),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn draw_explore_modal(frame: &mut Frame<'_>, app: &App, compact: bool) {
    let area = frame.area();
    let modal = centered_rect(
        area,
        if compact {
            area.width.saturating_sub(4)
        } else {
            100
        },
        if compact {
            area.height.saturating_sub(4)
        } else {
            28
        },
    );
    frame.render_widget(Clear, modal);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(INK))
        .style(Style::default().bg(SURFACE))
        .title(Line::styled(
            " EXPLORE  /  8 GENRE STATIONS ",
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ))
        .padding(ratatui::widgets::Padding::new(2, 2, 1, 1));
    let inner = block.inner(modal);
    frame.render_widget(block, modal);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new("Eight endless NTS worlds. Browse without disturbing what is playing.")
            .style(Style::default().fg(MUTED)),
        sections[0],
    );
    if !compact && sections[1].width >= 72 && sections[1].height >= 20 {
        render_explore_grid(frame, app, sections[1]);
    } else {
        render_explore_list(frame, app, sections[1]);
    }
    frame.render_widget(
        Paragraph::new("↑↓ / j k browse   •   space / enter listen   •   e / esc close")
            .style(Style::default().fg(MUTED)),
        sections[2],
    );
}

fn centered_rect(area: Rect, preferred_width: u16, preferred_height: u16) -> Rect {
    let width = preferred_width.min(area.width.saturating_sub(2));
    let height = preferred_height.min(area.height.saturating_sub(2));
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

fn render_explore_grid(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(5),
        ])
        .split(area);
    for (index, channel) in app.channels[2..].iter().enumerate() {
        let column = columns[index % 2];
        let row = rows[index / 2];
        let tile = Rect {
            x: column.x,
            width: column.width.saturating_sub(1),
            ..row
        };
        render_explore_tile(frame, channel, tile, index == app.explore_selected);
    }
}

/// Render artwork scaled to fill `area` (preserving aspect) and vertically
/// centered. `Scale` resizes up or down — unlike `Fit`, which would cap a
/// small source at its native pixel size and leave the card half-empty.
fn render_artwork(frame: &mut Frame<'_>, area: Rect, artwork: &RefCell<ThreadProtocol>) {
    let mut protocol = artwork.borrow_mut();
    // `size_for` is `None` while no encoded image is ready (initial encode or a
    // pending resize). Draw nothing this frame; the worker's result arrives soon.
    let Some(fitted) = protocol.size_for(Resize::Scale(None), Size::new(area.width, area.height))
    else {
        return;
    };
    let area = vcentered(area, fitted.height);
    frame.render_stateful_widget(
        StatefulImage::new().resize(Resize::Scale(None)),
        area,
        &mut *protocol,
    );
}

/// Shrink `area` to `height` rows, centered within the original vertical span.
fn vcentered(area: Rect, height: u16) -> Rect {
    let height = height.min(area.height);
    Rect {
        y: area.y + area.height.saturating_sub(height) / 2,
        height,
        ..area
    }
}

fn render_explore_tile(frame: &mut Frame<'_>, channel: &Channel, area: Rect, selected: bool) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(if selected { SIGNAL } else { BORDER }))
        .padding(ratatui::widgets::Padding::left(1));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(16),
            Constraint::Length(1),
            Constraint::Min(12),
        ])
        .split(inner);
    if let Some(artwork) = &channel.artwork {
        render_artwork(frame, columns[0], artwork);
    }
    let marker = if selected { "→ " } else { "  " };
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!("{marker}{}", channel.name),
                Style::default()
                    .fg(if selected { INK } else { MUTED })
                    .add_modifier(Modifier::BOLD),
            ),
            Line::styled(&channel.description, Style::default().fg(MUTED)),
        ])
        .wrap(Wrap { trim: true }),
        columns[2],
    );
}

fn render_explore_list(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let channels = &app.channels[2..];
    let columns = if area.width >= 64 { 2 } else { 1 };
    let groups = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(vec![Constraint::Ratio(1, columns); columns as usize])
        .split(area);
    let chunk_size = channels.len().div_ceil(columns as usize);
    for (column, group) in groups.iter().enumerate() {
        let start = column * chunk_size;
        let lines = channels[start..channels.len().min(start + chunk_size)]
            .iter()
            .enumerate()
            .map(|(offset, channel)| {
                let index = start + offset;
                Line::styled(
                    format!(
                        "{} {}",
                        if index == app.explore_selected {
                            "→"
                        } else {
                            " "
                        },
                        channel.name
                    ),
                    Style::default()
                        .fg(if index == app.explore_selected {
                            INK
                        } else {
                            MUTED
                        })
                        .add_modifier(if index == app.explore_selected {
                            Modifier::BOLD
                        } else {
                            Modifier::empty()
                        }),
                )
            })
            .collect::<Vec<_>>();
        frame.render_widget(Paragraph::new(lines), *group);
    }
}

/// Draw the wide now-playing card and station rail. The card sizes itself to
/// its content rather than being capped, so long descriptions are shown in
/// full.
fn draw_wide_channels(frame: &mut Frame<'_>, app: &App, area: Rect, paint_artwork: bool) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(64),
            Constraint::Length(2),
            Constraint::Min(34),
        ])
        .split(area);
    render_now_playing_card(
        frame,
        &app.channels[app.selected],
        columns[0],
        app.playing,
        paint_artwork,
    );

    let switcher = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(6),
            Constraint::Min(0),
        ])
        .split(columns[2]);
    for (slot, index) in visible_channels(app.selected, app.channels.len(), 3)
        .into_iter()
        .enumerate()
    {
        render_channel_switcher(
            frame,
            &app.channels[index],
            switcher[slot * 2],
            index == app.selected,
            app.playing,
            app.buffering,
        );
    }
}

fn draw_compact_header(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let selected = &app.channels[app.selected];
    let header = vec![
        Span::styled("NTS", Style::default().fg(INK).add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled(
            status_copy(app),
            Style::default().fg(if app.playing { SIGNAL } else { MUTED }),
        ),
        Span::raw("   "),
        Span::styled("→ ", Style::default().fg(INK)),
        Span::styled(
            selected.name,
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            if selected.kind == ChannelKind::Mixtape {
                "  ·  EXPLORE"
            } else {
                "  ·  RADIO"
            },
            Style::default().fg(MUTED),
        ),
    ];
    frame.render_widget(Paragraph::new(Line::from(header)), area);
}

/// Draw the compact now-playing card. It grows to fit its copy rather than
/// being capped to a fixed height, so long descriptions are never truncated.
fn draw_compact(frame: &mut Frame<'_>, app: &App, area: Rect, paint_artwork: bool) {
    const ART_WIDTH: u16 = 18;
    const GUTTER: u16 = 2;
    const TEXT_MIN: u16 = 20;

    let selected = &app.channels[app.selected];
    // The inner width sits past the left border (1) and left padding (2).
    let inner_width = area.width.saturating_sub(3);
    let show_art = selected.artwork.is_some() && inner_width >= ART_WIDTH + GUTTER + TEXT_MIN;
    let text_width = if show_art {
        inner_width.saturating_sub(ART_WIDTH + GUTTER)
    } else {
        inner_width
    };

    let text_rows = now_text_height(selected, text_width, false);
    let art_rows = if show_art {
        selected.artwork.as_ref().map_or(0, |artwork| {
            artwork
                .borrow()
                .size_for(Resize::Scale(None), Size::new(ART_WIDTH, area.height))
                .map_or(0, |size| size.height)
        })
    } else {
        0
    };
    let content = Rect {
        height: text_rows.max(art_rows).clamp(1, area.height),
        ..area
    };

    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(SIGNAL))
        .padding(ratatui::widgets::Padding::left(2));
    let now_area = block.inner(content);
    frame.render_widget(block, content);

    if show_art {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(ART_WIDTH),
                Constraint::Length(GUTTER),
                Constraint::Min(TEXT_MIN),
            ])
            .split(now_area);
        if paint_artwork {
            if let Some(artwork) = &selected.artwork {
                render_artwork(frame, columns[0], artwork);
            }
        } else {
            frame.render_widget(
                Block::default().style(Style::default().bg(SURFACE)),
                columns[0],
            );
        }
        let text_rows = now_text_height(selected, columns[2].width, false);
        render_now_text(frame, selected, vcentered(columns[2], text_rows), false);
    } else {
        render_now_text(frame, selected, now_area, false);
    }
}

/// Draw the wide now-playing card. Like the compact card, it grows to fit its
/// copy instead of being capped, so long show descriptions are never
/// truncated when there is room below.
fn render_now_playing_card(
    frame: &mut Frame<'_>,
    channel: &Channel,
    area: Rect,
    playing: bool,
    paint_artwork: bool,
) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(if playing { SIGNAL } else { BORDER }))
        .style(Style::default().bg(SURFACE))
        .padding(ratatui::widgets::Padding::new(2, 2, 1, 1));
    // Widths are independent of height, so measure them against the full area.
    let inner = block.inner(area);
    // The artwork resizes itself to whatever column it gets, so we just pick a
    // width that scales with the card and keep a 20-cell minimum for the copy.
    let artwork_width = inner.width.saturating_sub(24).clamp(20, 56);
    let show_art = channel.artwork.is_some() && inner.width >= artwork_width + 23;
    let text_width = if show_art {
        inner.width.saturating_sub(artwork_width + 3)
    } else {
        inner.width
    };

    let text_rows = now_text_height(channel, text_width, true);
    let art_rows = if show_art {
        channel.artwork.as_ref().map_or(0, |artwork| {
            artwork
                .borrow()
                .size_for(Resize::Scale(None), Size::new(artwork_width, area.height))
                .map_or(0, |size| size.height)
        })
    } else {
        0
    };
    // +2 for the block's top and bottom padding.
    let content = Rect {
        height: (text_rows.max(art_rows) + 2).clamp(3, area.height),
        ..area
    };

    let inner = block.inner(content);
    frame.render_widget(block, content);

    if show_art {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(artwork_width),
                Constraint::Length(3),
                Constraint::Min(20),
            ])
            .split(inner);
        if paint_artwork {
            if let Some(artwork) = &channel.artwork {
                render_artwork(frame, columns[0], artwork);
            }
        } else {
            frame.render_widget(
                Block::default().style(Style::default().bg(SURFACE)),
                columns[0],
            );
        }
        let text_rows = now_text_height(channel, columns[2].width, true);
        render_now_text(frame, channel, vcentered(columns[2], text_rows), true);
    } else {
        render_now_text(frame, channel, inner, true);
    }
}

fn render_channel_switcher(
    frame: &mut Frame<'_>,
    channel: &Channel,
    area: Rect,
    selected: bool,
    playing: bool,
    buffering: bool,
) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(if selected { SIGNAL } else { BORDER }))
        .style(Style::default().bg(if selected { SURFACE } else { BASE }))
        .padding(ratatui::widgets::Padding::new(1, 1, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let label = if selected { "→" } else { " " };
    let activity = if selected && playing && buffering {
        "◌ BUFFERING"
    } else if selected && playing && channel.kind == ChannelKind::Live {
        "● ON AIR"
    } else if selected && playing {
        "● PLAYING"
    } else if channel.kind == ChannelKind::Live {
        "LIVE NOW"
    } else {
        "INFINITE MIXTAPE"
    };
    let text = vec![
        Line::styled(
            format!("{label} {}", channel.name),
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ),
        Line::styled(
            activity,
            Style::default().fg(if selected { SIGNAL } else { MUTED }),
        ),
        Line::styled(
            &channel.show,
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ),
    ];
    frame.render_widget(Paragraph::new(text).wrap(Wrap { trim: true }), inner);
}

fn render_now_text(frame: &mut Frame<'_>, channel: &Channel, area: Rect, show_next: bool) {
    let mut copy = vec![
        Line::styled(
            if channel.kind == ChannelKind::Live {
                format!("NOW ON NTS {}", channel.number)
            } else {
                "INFINITE MIXTAPE".to_owned()
            },
            Style::default().fg(SIGNAL).add_modifier(Modifier::BOLD),
        ),
        Line::styled(
            &channel.show,
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ),
        Line::styled(&channel.description, Style::default().fg(MUTED)),
    ];
    if show_next && channel.kind == ChannelKind::Live {
        copy.extend([
            Line::default(),
            Line::styled(
                format!("NEXT  {}  ·  {}", channel.next_starts_at, channel.next_show),
                Style::default().fg(MUTED),
            ),
        ]);
    }
    frame.render_widget(Paragraph::new(copy).wrap(Wrap { trim: true }), area);
}

fn now_text_height(channel: &Channel, width: u16, show_next: bool) -> u16 {
    // Use the exact same wrapper that paints the paragraph. The old local
    // approximation disagreed on Unicode and long words, which could make a
    // card one row too short and leave its text visually unbalanced.
    let wrapped_lines = |text: &str| {
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .line_count(width)
            .min(usize::from(u16::MAX)) as u16
    };
    // The show title wraps too — NTS titles routinely exceed a narrow card's
    // width — so measure it like the description instead of assuming one row.
    let title_lines = wrapped_lines(&channel.show);
    let description_lines = wrapped_lines(&channel.description);
    let next_lines = if show_next && channel.kind == ChannelKind::Live {
        2 // Spacer plus the schedule line.
    } else {
        0
    };
    1u16.saturating_add(title_lines)
        .saturating_add(description_lines)
        .saturating_add(next_lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn episode_url_is_resolved_by_media_type_not_rel() {
        // The real sparse next2 shape: NTS links the episode by its episode
        // media type. The rel varies between responses ("self" here, "details"
        // elsewhere), so resolution must not depend on it.
        let self_rel: Broadcast = serde_json::from_str(
            r#"{"broadcast_title":"Show","links":[
                {"rel":"self","href":"https://nts/api/v2/shows/x/episodes/y",
                 "type":"application/vnd.episode+json;charset=utf-8"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            self_rel.episode_url(),
            Some("https://nts/api/v2/shows/x/episodes/y")
        );

        let details_rel: Broadcast = serde_json::from_str(
            r#"{"broadcast_title":"Show","links":[
                {"rel":"details","href":"https://nts/api/v2/shows/x/episodes/y",
                 "type":"application/vnd.episode+json"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            details_rel.episode_url(),
            Some("https://nts/api/v2/shows/x/episodes/y")
        );

        // No type present: fall back to the /episodes/ path.
        let path_only: Broadcast = serde_json::from_str(
            r#"{"broadcast_title":"Show","links":[
                {"rel":"self","href":"https://nts/api/v2/shows/x/episodes/y"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            path_only.episode_url(),
            Some("https://nts/api/v2/shows/x/episodes/y")
        );

        // A non-episode self link (e.g. a channel) must not be mistaken for one.
        let channel: Broadcast = serde_json::from_str(
            r#"{"broadcast_title":"Show","links":[
                {"rel":"self","href":"https://nts/api/v2/live","type":"application/vnd.channel+json"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(channel.episode_url(), None);
    }

    #[test]
    fn resolve_details_uses_complete_inline_block_without_a_request() {
        // All fields present inline: resolve_details returns them as-is and never
        // touches the episode link, so this stays offline and deterministic.
        let broadcast: Broadcast = serde_json::from_str(
            r#"{
                "broadcast_title": "Show",
                "embeds": {"details": {
                    "name": "Inline Name",
                    "description": "Inline description",
                    "media": {"picture_medium": "https://nts/art.png"}
                }},
                "links": [{"href":"https://nts/api/v2/shows/x/episodes/should-not-fetch",
                           "type":"application/vnd.episode+json"}]
            }"#,
        )
        .unwrap();
        let details = resolve_details(&Client::new(), &broadcast);
        assert_eq!(details.name.as_deref(), Some("Inline Name"));
        assert_eq!(details.description.as_deref(), Some("Inline description"));
        assert_eq!(
            details.media.picture_medium.as_deref(),
            Some("https://nts/art.png")
        );
    }

    #[test]
    fn merge_details_fills_only_missing_inline_fields() {
        let inline = Details {
            name: Some("Inline".to_owned()),
            description: None,
            media: Media {
                picture_medium: Some("inline.png".to_owned()),
                picture_thumb: None,
            },
        };
        let episode = Details {
            name: Some("Episode".to_owned()),
            description: Some("Episode desc".to_owned()),
            media: Media {
                picture_medium: Some("episode.png".to_owned()),
                picture_thumb: Some("episode_thumb.png".to_owned()),
            },
        };
        let merged = merge_details(inline, episode);
        assert_eq!(merged.name.as_deref(), Some("Inline")); // kept
        assert_eq!(merged.description.as_deref(), Some("Episode desc")); // filled
        assert_eq!(merged.media.picture_medium.as_deref(), Some("inline.png")); // kept
        assert_eq!(
            merged.media.picture_thumb.as_deref(),
            Some("episode_thumb.png")
        ); // filled
    }

    fn test_app(selected: usize) -> App {
        App {
            channels: fallback_channels(),
            selected,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            picker: Picker::halfblocks(),
            now_playing: None,
            visualizer: None,
            view: View::Listen,
            error: None,
            reconnect_attempts: 0,
            connection_lost: false,
            encoded_tx: None,
        }
    }

    fn test_channel_update(
        index: usize,
        artwork_url: Option<&str>,
        artwork: Option<DynamicImage>,
    ) -> ChannelUpdate {
        ChannelUpdate {
            index,
            show: "Updated show".to_owned(),
            description: "Updated description".to_owned(),
            next_show: "Updated next show".to_owned(),
            next_starts_at: "—".to_owned(),
            ends_at: None,
            schedule: None,
            artwork_url: artwork_url.map(str::to_owned),
            artwork,
            stream: None,
        }
    }

    #[test]
    fn selecting_while_stopped_changes_only_the_selection() {
        let mut app = test_app(0);

        app.select_channel(1);

        assert_eq!(app.selected, 1);
        assert!(!app.playing);
        assert!(app.player.is_none());
    }

    #[test]
    fn artwork_is_cropped_to_nts_landscape_aspect_ratio() {
        let cropped = crop_to_landscape(DynamicImage::new_rgba8(400, 400));
        let ratio = cropped.width() as f32 / cropped.height() as f32;

        assert_eq!(cropped.width(), 400);
        assert!(
            (ratio - NTS_ARTWORK_ASPECT).abs() < 0.02,
            "aspect ratio {ratio} should match {NTS_ARTWORK_ASPECT}"
        );
    }

    #[test]
    fn next_up_time_is_rendered_in_the_local_timezone() {
        let source = "2026-06-21T05:00:00+01:00";
        let expected = DateTime::parse_from_rfc3339(source)
            .expect("valid RFC 3339 timestamp")
            .with_timezone(&Local)
            .format("%H:%M %Z")
            .to_string();

        assert_eq!(broadcast_time(Some(source)), expected);
    }

    #[test]
    fn stale_now_broadcast_is_promoted_to_the_scheduled_current_show() {
        let current_time = Utc::now();
        let stale = test_broadcast(
            "Stale show",
            current_time - chrono::Duration::hours(2),
            current_time - chrono::Duration::minutes(1),
        );
        let current = test_broadcast(
            "Current show",
            current_time - chrono::Duration::minutes(1),
            current_time + chrono::Duration::hours(1),
        );
        let following = test_broadcast(
            "Following show",
            current_time + chrono::Duration::hours(1),
            current_time + chrono::Duration::hours(2),
        );

        let (active, next) = active_and_next(stale, current, Some(following));

        assert_eq!(active.broadcast_title.as_deref(), Some("Current show"));
        assert_eq!(next.broadcast_title.as_deref(), Some("Following show"));
    }

    #[test]
    fn catalog_contains_two_live_channels_and_eight_mixtapes() {
        let channels = fallback_channels();

        assert_eq!(channels.len(), 10);
        assert_eq!(
            channels
                .iter()
                .filter(|channel| channel.kind == ChannelKind::Live)
                .count(),
            2
        );
        assert!(
            channels[2..]
                .iter()
                .all(|channel| channel.kind == ChannelKind::Mixtape)
        );
    }

    #[test]
    fn schedule_is_available_only_for_live_channels_and_keeps_its_station() {
        let mut app = test_app(2);

        app.toggle_schedule();
        assert_eq!(app.view, View::Listen);
        assert!(app.error.is_some());

        app.selected = 0;
        app.toggle_schedule();
        assert_eq!(app.view, View::Schedule);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn explore_is_a_non_disruptive_overlay_until_a_station_is_chosen() {
        let mut app = test_app(1);

        app.toggle_explore();
        assert_eq!(app.view, View::Explore);
        assert_eq!(app.selected, 1);

        app.move_explore(2);
        app.choose_explore();
        assert_eq!(app.view, View::Listen);
        assert_eq!(app.selected, 4);
    }

    #[test]
    fn explore_toggles_without_changing_the_station() {
        let mut app = test_app(1);

        app.toggle_explore();
        app.toggle_explore();

        assert_eq!(app.view, View::Listen);
        assert_eq!(app.selected, 1);
    }

    #[test]
    fn schedule_blocks_every_station_change_path() {
        let mut app = test_app(0);
        app.toggle_schedule();

        assert!(!app.navigate(1));
        assert!(!app.change_station(1));
        app.handle_media_command(MediaCommand::NextStation);
        app.handle_media_command(MediaCommand::PreviousStation);

        assert_eq!(app.view, View::Schedule);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn invalid_background_channel_updates_are_ignored() {
        let mut app = test_app(0);
        let original_show = app.channels[0].show.clone();

        app.apply_channel_updates(vec![test_channel_update(usize::MAX, None, None)]);

        assert_eq!(app.channels[0].show, original_show);
        assert_eq!(app.channels.len(), 10);
    }

    #[test]
    fn failed_artwork_download_is_not_cached_as_loaded() {
        let mut app = test_app(0);

        app.apply_channel_updates(vec![test_channel_update(
            0,
            Some("https://media.ntslive.co.uk/transient.jpg"),
            None,
        )]);

        assert!(app.channels[0].artwork_url.is_none());
    }

    #[test]
    fn a_populated_schedule_is_never_replaced_by_a_channel_update() {
        let mut app = test_app(0);
        let existing = ScheduleEntry {
            starts_at: "10:00".to_owned(),
            title: "Existing".to_owned(),
        };
        app.channels[0].schedule = vec![existing; 6];
        let mut update = test_channel_update(0, None, None);
        update.schedule = Some(vec![ScheduleEntry {
            starts_at: "11:00".to_owned(),
            title: "Two-entry now/next".to_owned(),
        }]);

        app.apply_channel_updates(vec![update]);

        assert_eq!(app.channels[0].schedule.len(), 6);
        assert_eq!(app.channels[0].schedule[0].title, "Existing");
    }

    #[test]
    fn an_empty_schedule_may_be_seeded_by_a_channel_update() {
        let mut app = test_app(0);
        let mut update = test_channel_update(0, None, None);
        update.schedule = Some(vec![ScheduleEntry {
            starts_at: "11:00".to_owned(),
            title: "Seed".to_owned(),
        }]);

        app.apply_channel_updates(vec![update]);

        assert_eq!(app.channels[0].schedule.len(), 1);
    }

    #[test]
    fn card_height_accounts_for_a_wrapped_show_title() {
        let mut channels = fallback_channels();
        let channel = &mut channels[2]; // Mixtape: no next-up block.
        channel.show = "A Show Title That Definitely Wraps Onto Several Lines".to_owned();
        channel.description = "Short.".to_owned();
        let width = 20;
        let title_lines = Paragraph::new(channel.show.as_str())
            .wrap(Wrap { trim: true })
            .line_count(width) as u16;
        assert!(title_lines >= 2, "test title must wrap at width {width}");

        // Kicker row + wrapped title rows + one description row.
        assert_eq!(now_text_height(channel, width, false), 1 + title_lines + 1);
    }

    #[test]
    fn station_arguments_resolve_forgivingly() {
        assert_eq!(station_index("1"), Some(0));
        assert_eq!(station_index("2"), Some(1));
        assert_eq!(station_index("NTS 2"), Some(1));
        assert_eq!(station_index("Poolside"), Some(2));
        assert_eq!(station_index("slow-focus"), Some(3));
        assert_eq!(station_index("SLOW FOCUS"), Some(3));
        assert_eq!(station_index("low key"), Some(4));
        assert_eq!(station_index("100-percent-hip-hop"), Some(4));
        assert_eq!(station_index("sheet-music"), Some(9));
        assert_eq!(station_index("bogus"), None);
        assert_eq!(station_index(""), None);
    }

    #[test]
    fn reconnect_delays_back_off_and_cap_at_thirty_seconds() {
        let seconds: Vec<u64> = (0..7).map(|a| reconnect_delay(a).as_secs()).collect();
        assert_eq!(seconds, vec![1, 2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn station_rail_keeps_the_selection_in_view() {
        assert_eq!(visible_channels(0, 10, 4), vec![0, 1, 2, 3]);
        assert_eq!(visible_channels(5, 10, 4), vec![3, 4, 5, 6]);
        assert_eq!(visible_channels(9, 10, 4), vec![6, 7, 8, 9]);
    }

    fn test_broadcast(title: &str, starts_at: DateTime<Utc>, ends_at: DateTime<Utc>) -> Broadcast {
        Broadcast {
            broadcast_title: Some(title.to_owned()),
            start_timestamp: Some(starts_at.to_rfc3339()),
            end_timestamp: Some(ends_at.to_rfc3339()),
            embeds: Embeds::default(),
            links: Vec::new(),
        }
    }
}
