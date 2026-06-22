# nts

A rich, responsive terminal home for [NTS Radio](https://www.nts.live), built with Rust, Ratatui, and `ratatui-image`.

Listen to NTS 1 and 2, browse eight NTS Infinite Mixtapes, see the current show and local-time schedule, and—in terminals with an image protocol—view actual artwork without breaking the terminal layout.
`mpv` stays running while you browse, so changing station retunes the player instead of restarting it.

## Local development

```sh
brew install mpv
cargo run
```

To install a local binary named `nts`:

```sh
cargo install --path .
nts
```

## Controls

| Key | Action |
| --- | --- |
| `space` / `enter` | play or stop; in Explore, listen to the highlighted station |
| `1`, `2` | select or retune a channel |
| `↑↓`, `j k` | move selection / retune while listening |
| `e` | open Infinite Mixtapes Explore |
| `s` | toggle the selected live channel's local-time schedule |
| `esc` | close Schedule or Explore; quit from the main view |
| `q` | quit |

## Motion and playback

The app uses TachyonFX for brief selection sweeps rather than a constantly busy UI.
Playback requires `mpv`; it is controlled through its local JSON IPC socket so a channel change can use `loadfile … replace` in the existing player process. The new station still has to establish its own network stream, but process startup is no longer part of the wait. `nts` queries mpv's playback state, surfaces stream startup as buffering, and will make one clean reconnect attempt if the player disappears.

On macOS, `nts` also publishes the current show to the system Now Playing interface. System play/pause and next/previous commands control the same active station session.

NTS's live metadata occasionally lags a handover. `nts` reconciles the timestamped broadcasts locally and refreshes at handover boundaries, so the current show and the schedule remain correct in your local timezone.

## Artwork

The app uses `ratatui-image` to query the terminal’s image capabilities and map artwork to its layout rectangle. The Kitty, iTerm2, and Sixel graphics protocols are supported, so terminals like Ghostty, iTerm2, Kitty, and WezTerm display cover art. On a text-only terminal, artwork is deliberately omitted rather than reduced to an ugly text raster.

NTS publishes the direct NTS 1 and NTS 2 streams in its [support documentation](https://ntslive.freshdesk.com/support/solutions/articles/77000587257-tunein). The app uses NTS’s public live endpoint for display metadata.

## Release

The formula will declare `mpv` as a dependency, so `brew install nts` will install it automatically. Publishing needs the eventual GitHub repository and Homebrew tap location to generate a real, checksum-pinned formula.
