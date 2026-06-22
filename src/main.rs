use std::{
    fs, io,
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, SetTitle, disable_raw_mode, enable_raw_mode,
    },
};
use image::{DynamicImage, imageops::FilterType};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect, Size},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use ratatui_image::{
    FontSize, Image, Resize,
    picker::{Picker, ProtocolType},
};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use tachyonfx::{EffectManager, EffectTimer, Interpolation, Motion, fx};

mod now_playing;
use now_playing::{MediaCommand, NowPlaying};
mod visualizer;
use visualizer::LiveVisualizer;

const LIVE_API: &str = "https://www.nts.live/api/v2/live";
const SCHEDULE_API: &str = "https://www.nts.live/api/v2/radio/schedule";
const MIXTAPE_API: &str = "https://www.nts.live/api/v2/mixtapes";
const NTS_1_STREAM: &str = "https://stream-relay-geo.ntslive.net/stream?client=direct";
const NTS_2_STREAM: &str = "https://stream-relay-geo.ntslive.net/stream2?client=direct";

const INK: Color = Color::Rgb(248, 247, 242);
const MUTED: Color = Color::Rgb(142, 139, 133);
const SIGNAL: Color = Color::Rgb(255, 72, 104);
const SURFACE: Color = Color::Rgb(15, 15, 14);
const BORDER: Color = Color::Rgb(54, 53, 50);
const NTS_ARTWORK_ASPECT: f32 = 1.6;

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
    artwork_primary: Option<ratatui_image::protocol::Protocol>,
    artwork_compact: Option<ratatui_image::protocol::Protocol>,
}

struct App {
    channels: Vec<Channel>,
    selected: usize,
    explore_selected: usize,
    playing: bool,
    buffering: bool,
    player: Option<Player>,
    effects: EffectManager<&'static str>,
    picker: Picker,
    now_playing: Option<NowPlaying>,
    visualizer: Option<LiveVisualizer>,
    view: View,
    error: Option<String>,
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

fn main() -> Result<()> {
    let mut terminal = setup_terminal()?;
    let result = (|| {
        // ratatui-image queries protocol and font metrics while the alternate
        // screen is active and before event handling begins.
        let picker = Picker::from_query_stdio().context("detect terminal image support")?;
        let (media_tx, media_rx) = mpsc::channel();
        let mut app = App::load(&picker, media_tx)?;
        drain_startup_responses()?;
        app.start_default_playback();
        app.sync_now_playing();

        let result = run(&mut terminal, &mut app, media_rx);
        app.stop_player();
        result
    })();
    restore_terminal(&mut terminal)?;
    result
}

impl App {
    fn load(picker: &Picker, media_sender: mpsc::Sender<MediaCommand>) -> Result<Self> {
        let mut app = Self {
            channels: fallback_channels(),
            selected: 0,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            effects: EffectManager::default(),
            picker: picker.clone(),
            now_playing: Some(NowPlaying::new(media_sender)),
            visualizer: None,
            view: View::Listen,
            error: None,
        };
        match fetch_live_updates(app.artwork_urls()) {
            Ok(updates) => app.apply_channel_updates(updates),
            Err(error) => app.error = Some(format!("Live data unavailable: {error}")),
        }
        app.animate_selection();
        Ok(app)
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
        // The wide card is deliberately artwork-led: at 50 cells this fills
        // its usable height while retaining NTS's landscape crop.
        let primary_size = nts_artwork_size(picker.font_size(), 50);
        let compact_size = nts_artwork_size(picker.font_size(), 16);

        for update in updates {
            let channel = &mut self.channels[update.index];
            channel.show = update.show;
            channel.description = update.description;
            channel.next_show = update.next_show;
            channel.next_starts_at = update.next_starts_at;
            channel.ends_at = update.ends_at;
            if let Some(schedule) = update.schedule {
                channel.schedule = schedule;
            }
            channel.artwork_url = update.artwork_url;
            if let Some(stream) = update.stream {
                channel.stream = stream;
            }

            if let Some(image) = update.artwork
                && picker.protocol_type() != ProtocolType::Halfblocks
            {
                let primary = artwork_to_cells(&image, picker.font_size(), primary_size);
                let compact = artwork_to_cells(&image, picker.font_size(), compact_size);
                channel.artwork_primary = picker
                    .new_protocol(primary, primary_size, Resize::Fit(None))
                    .ok();
                channel.artwork_compact = picker
                    .new_protocol(compact, compact_size, Resize::Fit(None))
                    .ok();
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
        debug_assert_eq!(self.selected, 0);
        self.toggle_playback();
    }

    fn select_channel(&mut self, index: usize) {
        if index == self.selected {
            return;
        }

        self.selected = index;
        self.animate_selection();

        // A channel selection is a retune, not merely a cursor move. Keep the
        // listening state intact and ask the already-running player to load
        // the new stream. This avoids process startup on every switch.
        if self.playing {
            let stream = self.channels[self.selected].stream.clone();
            let result = self
                .player
                .as_mut()
                .context("player process disappeared")
                .and_then(|player| player.retune(&stream));
            if let Err(error) = result {
                // If the player has died, recover by starting it once more.
                // The common path above is a low-latency IPC retune.
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
                            "Could not retune: {error}; restart failed: {restart_error}"
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

    fn handle_media_command(&mut self, command: MediaCommand) {
        match command {
            MediaCommand::TogglePlayback => self.toggle_playback(),
            MediaCommand::NextStation => {
                self.view = View::Listen;
                self.select_channel((self.selected + 1).min(self.channels.len() - 1));
            }
            MediaCommand::PreviousStation => {
                self.view = View::Listen;
                self.select_channel(self.selected.saturating_sub(1));
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

    fn open_explore(&mut self) {
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

    fn move_schedule_channel(&mut self, direction: isize) {
        let target = (self.selected as isize + direction)
            .clamp(0, self.channels.len().saturating_sub(1) as isize) as usize;
        if self.channels[target].kind == ChannelKind::Live {
            self.select_channel(target);
        } else {
            // Preserve the effortless linear station walk. Leaving NTS 2 for
            // a mixtape simply closes the schedule lens before retuning.
            self.view = View::Listen;
            self.select_channel(target);
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

    fn poll_player(&mut self) {
        if !self.playing {
            return;
        }
        let result = self
            .player
            .as_mut()
            .context("player process disappeared")
            .and_then(Player::is_buffering);
        match result {
            Ok(buffering) => {
                self.buffering = buffering;
                self.error = None;
            }
            Err(error) => {
                let stream = self.channels[self.selected].stream.clone();
                self.stop_player();
                match launch_player(&stream) {
                    Ok(player) => {
                        self.player = Some(player);
                        self.playing = true;
                        self.buffering = true;
                        self.error = Some("Stream reconnected.".to_owned());
                    }
                    Err(restart_error) => {
                        self.error = Some(format!(
                            "Playback stopped: {error}; reconnect failed: {restart_error}"
                        ));
                    }
                }
            }
        }
    }

    fn animate_selection(&mut self) {
        self.effects.add_unique_effect(
            "selection",
            fx::sweep_in(
                Motion::LeftToRight,
                8,
                0,
                SURFACE,
                EffectTimer::from_ms(220, Interpolation::QuadOut),
            ),
        );
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
                "--cache-secs=0.2",
                "--demuxer-readahead-secs=0.2",
                "--cache-pause-wait=0.1",
                &ipc_arg,
                stream,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("start mpv")?;

        Ok(Self { child, ipc_path })
    }

    fn retune(&mut self, stream: &str) -> Result<()> {
        if let Some(status) = self.child.try_wait().context("check mpv status")? {
            anyhow::bail!("mpv exited with {status}");
        }

        let mut socket = connect_ipc(&self.ipc_path)?;
        let command = json!({ "command": ["loadfile", stream, "replace"] }).to_string();
        socket
            .write_all(command.as_bytes())
            .and_then(|_| socket.write_all(b"\n"))
            .context("send retune command to mpv")
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
            artwork_primary: None,
            artwork_compact: None,
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
            artwork_primary: None,
            artwork_compact: None,
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
        artwork_primary: None,
        artwork_compact: None,
    }
}

fn fetch_live_updates(known_artwork_urls: Vec<Option<String>>) -> Result<Vec<ChannelUpdate>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
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
        let schedule = schedule_from_broadcasts(&[now.clone(), next.clone()]);
        let ends_at = now.end_timestamp.clone();
        let Details {
            name: now_name,
            description: now_description,
            media: now_media,
        } = now.embeds.details.unwrap_or_default();
        let Details {
            name: next_name, ..
        } = next.embeds.details.unwrap_or_default();
        let artwork_url = now_media.picture_medium.or(now_media.picture_thumb);
        let artwork = if artwork_url != known_artwork_urls[index] {
            artwork_url
                .as_deref()
                .and_then(|url| fetch_image(&client, url).ok())
        } else {
            None
        };

        updates.push(ChannelUpdate {
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
            schedule: Some(schedule),
            artwork_url,
            artwork,
            stream: None,
        });
    }

    Ok(updates)
}

fn fetch_schedule_updates() -> Result<Vec<ScheduleUpdate>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let current_time = Utc::now();
    let mut updates = Vec::with_capacity(2);

    for index in 0..2 {
        let schedule: ScheduleResponse = client
            .get(format!("{SCHEDULE_API}/{}?past_days=0", index + 1))
            .send()?
            .error_for_status()?
            .json()?;
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

fn fetch_mixtape_updates(known_artwork_urls: Vec<Option<String>>) -> Result<Vec<ChannelUpdate>> {
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

    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let mut updates = Vec::with_capacity(MIXTAPES.len());
    for (offset, alias) in MIXTAPES.iter().enumerate() {
        let mixtape: MixtapeResponse = client
            .get(format!("{MIXTAPE_API}/{alias}"))
            .send()?
            .error_for_status()?
            .json()?;
        let artwork_url = mixtape.media.picture_medium.or(mixtape.media.picture_thumb);
        let index = offset + 2;
        let artwork = if artwork_url != known_artwork_urls[index] {
            artwork_url
                .as_deref()
                .and_then(|url| fetch_image(&client, url).ok())
        } else {
            None
        };
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
    let bytes = client.get(url).send()?.error_for_status()?.bytes()?;
    image::load_from_memory(&bytes).context("decode NTS artwork")
}

fn artwork_to_cells(image: &DynamicImage, font_size: FontSize, cells: Size) -> DynamicImage {
    let width = u32::from(cells.width) * u32::from(font_size.width);
    let height = u32::from(cells.height) * u32::from(font_size.height);
    image.resize_to_fill(width.max(1), height.max(1), FilterType::Lanczos3)
}

fn nts_artwork_size(font_size: FontSize, width_cells: u16) -> Size {
    let pixel_width = f32::from(width_cells) * f32::from(font_size.width);
    let height_cells = (pixel_width / (NTS_ARTWORK_ASPECT * f32::from(font_size.height)))
        .round()
        .max(1.0) as u16;
    Size::new(width_cells, height_cells)
}

fn launch_player(stream: &str) -> Result<Player> {
    if Command::new("mpv")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
    {
        return Player::start(stream);
    }
    anyhow::bail!("Install mpv first: brew install mpv")
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout)).context("start terminal UI")
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn drain_startup_responses() -> Result<()> {
    while event::poll(Duration::ZERO)? {
        let _ = event::read()?;
    }
    Ok(())
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
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    media_rx: mpsc::Receiver<MediaCommand>,
) -> Result<()> {
    let mut dirty = true;
    let mut last_frame = Instant::now();
    let mut title_frame = 0;
    let mut title_dirty = true;
    let mut next_title_tick = Instant::now();
    let (update_tx, update_rx) = mpsc::channel::<BackgroundUpdate>();
    let mut refresh_in_flight = false;
    let mut schedule_in_flight = false;
    let mut next_refresh = Instant::now() + app.next_refresh_delay();
    let mut next_schedule_refresh = Instant::now();
    let mut next_player_probe = Instant::now() + Duration::from_millis(300);

    request_mixtape_refresh(&update_tx, app.artwork_urls());
    loop {
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
                    if let Ok(updates) = result {
                        app.apply_channel_updates(updates);
                        app.sync_now_playing();
                        dirty = true;
                        title_dirty = true;
                        next_refresh = Instant::now() + app.next_refresh_delay();
                    }
                }
                BackgroundUpdate::Schedules(result) => {
                    schedule_in_flight = false;
                    if let Ok(updates) = result {
                        app.apply_schedule_updates(updates);
                        dirty = true;
                    }
                }
                BackgroundUpdate::Mixtapes(result) => {
                    if let Ok(updates) = result {
                        app.apply_channel_updates(updates);
                        app.sync_now_playing();
                        dirty = true;
                    }
                }
            }
        }
        if !refresh_in_flight && now >= next_refresh {
            next_refresh = now + Duration::from_secs(60);
            refresh_in_flight = true;
            let tx = update_tx.clone();
            let known_artwork_urls = app.artwork_urls();
            thread::spawn(move || {
                let _ = tx.send(BackgroundUpdate::Live(
                    fetch_live_updates(known_artwork_urls).map_err(|error| error.to_string()),
                ));
            });
        }
        if !schedule_in_flight && now >= next_schedule_refresh {
            next_schedule_refresh = now + Duration::from_secs(15 * 60);
            schedule_in_flight = true;
            request_schedule_refresh(&update_tx);
        }
        if app.playing && now >= next_player_probe {
            app.poll_player();
            app.sync_now_playing();
            next_player_probe = now + Duration::from_secs(1);
            dirty = true;
            title_dirty = true;
        }
        if dirty || app.effects.is_running() {
            let elapsed = if dirty {
                Duration::ZERO
            } else {
                last_frame.elapsed()
            };
            terminal.draw(|frame| {
                let effect_area = draw(frame, app);
                app.effects
                    .process_effects(elapsed.into(), frame.buffer_mut(), effect_area);
            })?;
            dirty = false;
            last_frame = Instant::now();
        }
        let mut poll_interval = if app.effects.is_running() {
            Duration::from_millis(16)
        } else {
            Duration::from_millis(120)
        };
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
        if app.playing {
            poll_interval =
                poll_interval.min(next_player_probe.saturating_duration_since(Instant::now()));
        }
        if app.visualizer.is_some() {
            poll_interval = poll_interval.min(Duration::from_millis(16));
        }
        if event::poll(poll_interval)?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            dirty = match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(()),
                KeyCode::Esc => {
                    if !app.close_visualizer() && !app.dismiss_overlay() {
                        return Ok(());
                    }
                    true
                }
                KeyCode::Char('1') => {
                    app.view = View::Listen;
                    app.select_channel(0);
                    true
                }
                KeyCode::Char('2') => {
                    app.view = View::Listen;
                    app.select_channel(1);
                    true
                }
                KeyCode::Char('e') | KeyCode::Char('E') => {
                    app.open_explore();
                    true
                }
                KeyCode::Char('v') | KeyCode::Char('V') => {
                    app.toggle_visualizer();
                    true
                }
                KeyCode::Up | KeyCode::Left | KeyCode::Char('k') | KeyCode::Char('h') => {
                    match app.view {
                        View::Listen => app.select_channel(app.selected.saturating_sub(1)),
                        View::Schedule => app.move_schedule_channel(-1),
                        View::Explore => app.move_explore(-1),
                    }
                    true
                }
                KeyCode::Down | KeyCode::Right | KeyCode::Char('j') | KeyCode::Char('l') => {
                    match app.view {
                        View::Listen => {
                            app.select_channel((app.selected + 1).min(app.channels.len() - 1))
                        }
                        View::Schedule => app.move_schedule_channel(1),
                        View::Explore => app.move_explore(1),
                    }
                    true
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

fn draw(frame: &mut Frame<'_>, app: &App) -> Rect {
    let area = frame.area();
    let compact = area.width < 110 || area.height < 30;
    // Terminal image protocols are rendered independently of Ratatui's cell
    // buffer. Render the visualizer as an exclusive surface so an artwork
    // update cannot leap above its braille layer during a station change.
    if app.visualizer.is_some() {
        frame.render_widget(Clear, area);
        frame.render_widget(Block::default().style(Style::default().bg(SURFACE)), area);
        draw_visualizer_modal(frame, app, compact);
        return Rect::default();
    }
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
        draw_compact(frame, app, main[1]);
        render_footer(frame, app, main[2], true);
        render_error(frame, app, main[2]);
        match app.view {
            View::Schedule => draw_schedule_modal(frame, app, true),
            View::Explore => draw_explore_modal(frame, app, true),
            View::Listen => {}
        }
        return compact_animation_area(main[1]);
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
        Span::styled(
            "  —  LIVE, EVERYWHERE",
            Style::default().fg(INK).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "    independent radio from london & beyond",
            Style::default().fg(MUTED),
        ),
    ]);
    frame.render_widget(Paragraph::new(title), main[0]);
    let status = status_copy(app);
    frame.render_widget(
        Paragraph::new(status).style(Style::default().fg(MUTED)),
        main[1],
    );

    draw_wide_channels(frame, app, main[2]);
    render_footer(frame, app, main[3], false);
    render_error(frame, app, main[3]);
    match app.view {
        View::Schedule => draw_schedule_modal(frame, app, false),
        View::Explore => draw_explore_modal(frame, app, false),
        View::Listen => {}
    }

    wide_animation_area(main[2])
}

fn render_footer(frame: &mut Frame<'_>, app: &App, area: Rect, compact: bool) {
    let footer = if compact {
        if app.playing {
            "↑↓ browse  •  e explore  •  s schedule  •  v visualizer  •  space stop"
        } else {
            "↑↓ browse  •  e explore  •  s schedule  •  v visualizer  •  space listen"
        }
    } else if app.playing {
        "↑↓ / j k retune    •    1 2 radio    •    e explore    •    s schedule    •    v visualizer    •    space stop    •    q quit"
    } else {
        "↑↓ / j k select    •    1 2 radio    •    e explore    •    s schedule    •    v visualizer    •    space listen    •    q quit"
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().fg(MUTED)),
        area,
    );
}

fn render_error(frame: &mut Frame<'_>, app: &App, footer_area: Rect) {
    if let Some(error) = &app.error {
        let error_area = Rect {
            y: footer_area.y.saturating_sub(1),
            height: 1,
            ..footer_area
        };
        frame.render_widget(
            Paragraph::new(error.as_str()).style(Style::default().fg(SIGNAL)),
            error_area,
        );
    }
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
            "s / esc close  •  ↑↓ switch station  •  space play / stop"
        } else {
            "s / esc close    •    ↑↓ / j k switch station    •    space play / stop"
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
        Paragraph::new("↑↓ / j k browse   •   space / enter listen   •   esc close")
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
    if let Some(artwork) = &channel.artwork_compact {
        frame.render_widget(Image::new(artwork), columns[0]);
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

fn wide_animation_area(area: Rect) -> Rect {
    let content = wide_content_area(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(64),
            Constraint::Length(2),
            Constraint::Min(34),
        ])
        .split(content)[0]
}

fn draw_wide_channels(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let content = wide_content_area(area);
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(64),
            Constraint::Length(2),
            Constraint::Min(34),
        ])
        .split(content);
    render_now_playing_card(frame, &app.channels[app.selected], columns[0], app.playing);

    let switcher = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(1),
            Constraint::Length(5),
            Constraint::Length(1),
            Constraint::Length(5),
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

fn wide_content_area(area: Rect) -> Rect {
    Rect {
        height: area.height.min(18),
        ..area
    }
}

fn compact_animation_area(area: Rect) -> Rect {
    Rect {
        height: area.height.min(8),
        ..area
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

fn draw_compact(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let content = Rect {
        height: area.height.min(8),
        ..area
    };

    let selected = &app.channels[app.selected];
    let now = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(SIGNAL))
        .padding(ratatui::widgets::Padding::left(2));
    let now_area = now.inner(content);
    frame.render_widget(now, content);

    if let Some(artwork) = &selected.artwork_compact
        && now_area.width >= 44
        && now_area.height >= 5
    {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(18),
                Constraint::Length(2),
                Constraint::Min(20),
            ])
            .split(now_area);
        frame.render_widget(Image::new(artwork), columns[0]);
        render_now_text(frame, selected, columns[2], false);
    } else {
        render_now_text(frame, selected, now_area, false);
    }
}

fn render_now_playing_card(frame: &mut Frame<'_>, channel: &Channel, area: Rect, playing: bool) {
    let block = Block::default()
        .borders(Borders::LEFT)
        .border_style(Style::default().fg(if playing { SIGNAL } else { BORDER }))
        .style(Style::default().bg(SURFACE))
        .padding(ratatui::widgets::Padding::new(2, 2, 1, 1));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if let Some(artwork) = &channel.artwork_primary {
        let artwork_width = inner.width.saturating_sub(24).clamp(30, 50);
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(artwork_width),
                Constraint::Length(3),
                Constraint::Min(20),
            ])
            .split(inner);
        frame.render_widget(Image::new(artwork), columns[0]);
        render_now_text(frame, channel, columns[2], true);
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
        .border_style(Style::default().fg(if selected { INK } else { BORDER }))
        .style(Style::default().bg(Color::Reset))
        .padding(ratatui::widgets::Padding::new(1, 1, 0, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(10),
            Constraint::Length(1),
            Constraint::Min(20),
        ])
        .split(inner);
    if let Some(artwork) = &channel.artwork_compact {
        frame.render_widget(Image::new(artwork), columns[0]);
    }

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
    frame.render_widget(Paragraph::new(text).wrap(Wrap { trim: true }), columns[2]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selecting_while_stopped_changes_only_the_selection() {
        let mut app = App {
            channels: fallback_channels(),
            selected: 0,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            effects: EffectManager::default(),
            picker: Picker::halfblocks(),
            now_playing: None,
            visualizer: None,
            view: View::Listen,
            error: None,
        };

        app.select_channel(1);

        assert_eq!(app.selected, 1);
        assert!(!app.playing);
        assert!(app.player.is_none());
        assert!(app.effects.is_running());
    }

    #[test]
    fn artwork_is_cropped_to_nts_landscape_aspect_ratio() {
        let source = DynamicImage::new_rgba8(320, 180);
        let size = nts_artwork_size(FontSize::new(8, 16), 32);
        let artwork = artwork_to_cells(&source, FontSize::new(8, 16), size);

        assert_eq!((size.width, size.height), (32, 10));
        assert_eq!((artwork.width(), artwork.height()), (256, 160));
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
    fn schedule_is_available_only_for_live_channels() {
        let mut app = App {
            channels: fallback_channels(),
            selected: 2,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            effects: EffectManager::default(),
            picker: Picker::halfblocks(),
            now_playing: None,
            visualizer: None,
            view: View::Listen,
            error: None,
        };

        app.toggle_schedule();
        assert_eq!(app.view, View::Listen);
        assert!(app.error.is_some());

        app.selected = 0;
        app.toggle_schedule();
        assert_eq!(app.view, View::Schedule);

        app.selected = 1;
        app.move_schedule_channel(1);
        assert_eq!(app.selected, 2);
        assert_eq!(app.view, View::Listen);
        assert_eq!(app.channels[app.selected].kind, ChannelKind::Mixtape);
    }

    #[test]
    fn explore_is_a_non_disruptive_overlay_until_a_station_is_chosen() {
        let mut app = App {
            channels: fallback_channels(),
            selected: 1,
            explore_selected: 0,
            playing: false,
            buffering: false,
            player: None,
            effects: EffectManager::default(),
            picker: Picker::halfblocks(),
            now_playing: None,
            visualizer: None,
            view: View::Listen,
            error: None,
        };

        app.open_explore();
        assert_eq!(app.view, View::Explore);
        assert_eq!(app.selected, 1);

        app.move_explore(2);
        app.choose_explore();
        assert_eq!(app.view, View::Listen);
        assert_eq!(app.selected, 4);
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
        }
    }
}
