# nts

An unofficial, rich terminal home for [NTS Radio](https://www.nts.live), built with Rust, Ratatui, and `ratatui-image`.

Listen to NTS 1 and NTS 2, browse eight Infinite Mixtapes, see the current show and local-time schedule, and discover a little braille visualizer. In terminals with an image protocol, it renders the actual show artwork without compromising the layout.

`mpv` stays running while you browse: station changes reuse the player process rather than restart it.

> NTS is a trademark of NTS. This is an independent, unofficial client and is not affiliated with NTS.

## Features

- Starts NTS 1 on launch; switch stations without stopping playback.
- Live now-playing metadata, scheduled handovers, and local-time schedules.
- Eight NTS Infinite Mixtapes in a dedicated Explore overlay.
- Native macOS Now Playing integration and media-key controls.
- Artwork via Kitty, iTerm2, and Sixel terminal graphics protocols.
- A responsive compact layout, plus a hidden `v` visualizer moment.

## Install

Install from the Homebrew tap:

```sh
brew install r-ohan/nts/nts
nts
```

Or install directly from source:

```sh
brew install mpv
cargo install --git https://github.com/r-ohan/nts-radio-cli.git
```

The formula installs `mpv` automatically. Source installs require Rust 1.85+ and `mpv`.

## Controls

| Key | Action |
| --- | --- |
| `space` / `enter` | play or stop; in Explore, listen to the highlighted station |
| `1`, `2` | select or change to a radio station |
| `↑↓`, `j k` | move selection / change station while listening |
| `e` | open Infinite Mixtapes Explore |
| `s` | toggle the selected live channel's local-time schedule |
| `v` | open or close the visualizer |
| `esc` | close Schedule or Explore; quit from the main view |
| `q` | quit |

## Playback and metadata

Playback requires `mpv`; it is controlled through its local JSON IPC socket so a channel change can use `loadfile … replace` in the existing player process. The new station still has to establish its own network stream, but process startup is no longer part of the wait. `nts` queries mpv's playback state, surfaces stream startup as buffering, and will make one clean reconnect attempt if the player disappears.

On macOS, `nts` also publishes the current show to the system Now Playing interface. System play/pause and next/previous commands control the same active station session.

NTS's live metadata occasionally lags a handover. `nts` reconciles the timestamped broadcasts locally and refreshes at handover boundaries, so the current show and the schedule remain correct in your local timezone.

## Artwork

The app uses `ratatui-image` to query the terminal’s image capabilities and map artwork to its layout rectangle. The Kitty, iTerm2, and Sixel graphics protocols are supported, so terminals like Ghostty, iTerm2, Kitty, and WezTerm display cover art. On a text-only terminal, artwork is deliberately omitted rather than reduced to an ugly text raster.

NTS publishes the direct NTS 1 and NTS 2 streams in its [support documentation](https://ntslive.freshdesk.com/support/solutions/articles/77000587257-tunein). The app uses NTS’s public live endpoint for display metadata.

## Development

```sh
cargo test
cargo clippy -- -D warnings
cargo run --release
```

## License

MIT. See [LICENSE](LICENSE).
