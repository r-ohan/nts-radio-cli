use std::sync::mpsc::Sender;

#[derive(Clone, Copy, Debug)]
// These commands are produced by macOS's remote-command center. Other
// platforms deliberately provide a no-op Now Playing implementation, so the
// variants are unused there but remain part of the shared application API.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub enum MediaCommand {
    TogglePlayback,
    Play,
    StopPlayback,
    NextStation,
    PreviousStation,
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{MediaCommand, Sender};
    use block2::RcBlock;
    use objc2::{rc::Retained, runtime::AnyObject};
    use objc2_foundation::{
        NSDate, NSDefaultRunLoopMode, NSDictionary, NSNumber, NSRunLoop, NSString,
    };
    use objc2_media_player::{
        MPMediaItemPropertyAlbumTitle, MPMediaItemPropertyArtist, MPMediaItemPropertyTitle,
        MPNowPlayingInfoCenter, MPNowPlayingInfoPropertyIsLiveStream,
        MPNowPlayingInfoPropertyPlaybackRate, MPNowPlayingPlaybackState,
        MPRemoteCommandHandlerStatus,
    };

    pub struct NowPlaying {
        center: Retained<MPNowPlayingInfoCenter>,
        // MPRemoteCommandCenter keeps the callbacks alive, but retaining the
        // opaque command targets makes that ownership explicit on our side.
        _command_targets: Vec<Retained<AnyObject>>,
    }

    impl NowPlaying {
        pub fn new(sender: Sender<MediaCommand>) -> Self {
            // MediaPlayer is an Apple system framework and is only linked on
            // macOS; all uses stay on the application's main thread.
            let center = unsafe { MPNowPlayingInfoCenter::defaultCenter() };
            let commands =
                unsafe { objc2_media_player::MPRemoteCommandCenter::sharedCommandCenter() };
            let mut targets = Vec::new();
            for (command, event) in [
                (
                    unsafe { commands.togglePlayPauseCommand() },
                    MediaCommand::TogglePlayback,
                ),
                (unsafe { commands.playCommand() }, MediaCommand::Play),
                (
                    unsafe { commands.pauseCommand() },
                    MediaCommand::StopPlayback,
                ),
                (
                    unsafe { commands.stopCommand() },
                    MediaCommand::StopPlayback,
                ),
                (
                    unsafe { commands.nextTrackCommand() },
                    MediaCommand::NextStation,
                ),
                (
                    unsafe { commands.previousTrackCommand() },
                    MediaCommand::PreviousStation,
                ),
            ] {
                // Command-center defaults vary by source and macOS release;
                // make the live-radio controls explicitly actionable.
                unsafe { command.setEnabled(true) };
                let sender = sender.clone();
                let handler = RcBlock::new(move |_| {
                    let _ = sender.send(event);
                    MPRemoteCommandHandlerStatus::Success
                });
                let target = unsafe { command.addTargetWithHandler(&handler) };
                targets.push(target);
            }
            Self {
                center,
                _command_targets: targets,
            }
        }

        pub fn update(&self, title: &str, station: &str, playing: bool) {
            let title = NSString::from_str(title);
            let station = NSString::from_str(station);
            let live = NSNumber::new_bool(true);
            let rate = NSNumber::new_f64(if playing { 1.0 } else { 0.0 });
            let values: Vec<Retained<AnyObject>> = vec![
                title.into_super().into(),
                station.clone().into_super().into(),
                station.into_super().into(),
                live.into_super().into(),
                rate.into_super().into(),
            ];
            let info = unsafe {
                NSDictionary::from_retained_objects(
                    &[
                        MPMediaItemPropertyTitle,
                        MPMediaItemPropertyArtist,
                        MPMediaItemPropertyAlbumTitle,
                        MPNowPlayingInfoPropertyIsLiveStream,
                        MPNowPlayingInfoPropertyPlaybackRate,
                    ],
                    &values,
                )
            };
            unsafe {
                self.center.setNowPlayingInfo(Some(&info));
                self.center.setPlaybackState(if playing {
                    MPNowPlayingPlaybackState::Playing
                } else {
                    MPNowPlayingPlaybackState::Stopped
                });
            }
        }

        /// `MPRemoteCommandCenter` delivers command handlers through Cocoa's
        /// main run loop. A Ratatui app owns its own event loop, so service one
        /// non-blocking Cocoa turn alongside each terminal tick.
        pub fn pump(&self) {
            let run_loop = NSRunLoop::mainRunLoop();
            let limit = NSDate::dateWithTimeIntervalSinceNow(0.0);
            let mode = unsafe { NSDefaultRunLoopMode };
            let _ = run_loop.runMode_beforeDate(mode, &limit);
        }

        pub fn clear(&self) {
            unsafe { self.center.setNowPlayingInfo(None) }
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use super::{MediaCommand, Sender};

    pub struct NowPlaying;

    impl NowPlaying {
        pub fn new(_: Sender<MediaCommand>) -> Self {
            Self
        }

        pub fn update(&self, _: &str, _: &str, _: bool) {}

        pub fn pump(&self) {}

        pub fn clear(&self) {}
    }
}

pub use platform::NowPlaying;
