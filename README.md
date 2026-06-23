# nts

### NTS Radio, in your terminal.

<p align="center">
  <img src="assets/screenshot.png" alt="nts — a live NTS 2 show with artwork, now/next details, and station list" width="900">
</p>

`nts` is an unofficial NTS Radio player for the terminal, written in Rust.

## Install

```sh
brew install r-ohan/nts/nts
```

or, with Cargo, install from crates.io:

```sh
cargo install nts-radio-cli
```

## Inside

- NTS 1, NTS 2, and eight genre-specific Infinite Mixtapes.
- Live show details, next-up information, and schedules in your local time.
- Artwork in image-capable terminals; a clean text-first experience everywhere else.
- A layout that adapts from a small terminal to a full-screen listening room.
- macOS Now Playing and media-key support.


## Notes

Artwork appears in terminals with Kitty, iTerm2, or Sixel image support—such
as Ghostty, iTerm2, Kitty, and WezTerm. It is intentionally omitted in
text-only terminals rather than turned into an ugly raster.

NTS publishes the direct live streams and public schedule metadata used by the
app. See NTS’s [listening guide](https://ntslive.freshdesk.com/support/solutions/articles/77000587257-tunein).

`nts` is independent and unofficial. NTS is a trademark of NTS and this
project is not affiliated with or endorsed by NTS.


## License

[MIT](LICENSE)
