use std::time::Instant;

/// A lightweight, show-seeded braille world. It is intentionally visual-only:
/// playback owns the audio stream and this never creates another process.
pub struct LiveVisualizer {
    started_at: Instant,
    parameters: [f32; 8],
}

impl LiveVisualizer {
    pub fn new(identity: &str) -> Self {
        let hash = identity.bytes().fold(2166136261_u32, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(16777619)
        });
        Self {
            started_at: Instant::now(),
            parameters: std::array::from_fn(|index| seed_value(hash, index as u32)),
        }
    }

    /// The main loop calls this at its normal 60fps cadence.
    pub fn poll(&mut self) -> bool {
        true
    }

    pub fn braille(&self, width: u16, height: u16) -> String {
        if width == 0 || height == 0 {
            return String::new();
        }

        let cells_w = width as usize;
        let cells_h = height as usize;
        let dots_w = cells_w * 2;
        let dots_h = cells_h * 4;
        let time = self.started_at.elapsed().as_secs_f32();
        let mut output = Vec::with_capacity(cells_h);

        for cell_y in 0..cells_h {
            let mut line = String::with_capacity(cells_w);
            for cell_x in 0..cells_w {
                let mut dots = 0u8;
                for dot_y in 0..4 {
                    for dot_x in 0..2 {
                        let x = (cell_x * 2 + dot_x) as f32 / dots_w.max(1) as f32;
                        let y = (cell_y * 4 + dot_y) as f32 / dots_h.max(1) as f32;
                        if field(x, y, time, self.parameters) > 0.56 {
                            dots |= braille_dot(dot_x, dot_y);
                        }
                    }
                }
                line.push(char::from_u32(0x2800 + u32::from(dots)).unwrap_or(' '));
            }
            output.push(line);
        }
        output.join("\n")
    }
}

fn seed_value(hash: u32, index: u32) -> f32 {
    let mut value = hash.wrapping_add(index.wrapping_mul(0x9E37_79B9));
    value ^= value >> 16;
    value = value.wrapping_mul(0x85EB_CA6B);
    value ^= value >> 13;
    value as f32 / u32::MAX as f32
}

fn field(x: f32, y: f32, time: f32, parameters: [f32; 8]) -> f32 {
    let [
        phase,
        tempo,
        curve,
        density,
        orbit,
        contrast,
        texture,
        drift,
    ] = parameters;
    let time = time * (0.38 + tempo * 0.72);
    let phase = phase * std::f32::consts::TAU;
    let bent_x =
        x + (y * (7.0 + density * 11.0) - time * (0.7 + drift)).sin() * (0.025 + curve * 0.08);

    let ribbon_a = ((bent_x * (4.0 + density * 8.0) + time + phase).sin() * (0.10 + curve * 0.16))
        + ((bent_x * (2.0 + orbit * 5.0) - time * 0.43).cos() * 0.07)
        + 0.34
        + drift * 0.20;
    let ribbon_b = ((bent_x * (6.0 + orbit * 7.0) - time * 0.68 + phase * 1.7).sin()
        * (0.08 + contrast * 0.13))
        + 0.65
        - drift * 0.17;
    let ribbons = (1.0 - (y - ribbon_a).abs() * (5.5 + contrast * 5.0))
        .max(1.0 - (y - ribbon_b).abs() * (6.0 + density * 5.0));

    let center_x = 0.5 + (time * 0.37 + phase).sin() * (0.015 + orbit * 0.055);
    let center_y = 0.5 + (time * 0.29 + phase).cos() * (0.015 + curve * 0.045);
    let dx = (x - center_x) * 1.65;
    let dy = y - center_y;
    let radius = (dx * dx + dy * dy).sqrt();
    let angle = dy.atan2(dx);
    let ring_radius = 0.18 + orbit * 0.19;
    let ring = 1.0
        - ((radius - ring_radius - 0.022 * (angle * (3.0 + density * 5.0) - time * 1.7).sin())
            .abs()
            * (10.0 + contrast * 8.0));
    let grain = ((x * (29.0 + texture * 31.0) + y * (13.0 + density * 19.0) + time * 2.0).sin()
        + 1.0)
        * (0.035 + texture * 0.10);
    let scanner_y =
        (time * (0.055 + drift * 0.065) + phase / std::f32::consts::TAU).rem_euclid(1.15) - 0.075;
    let scanner =
        (1.0 - (y - scanner_y).abs() * (18.0 + contrast * 18.0)).max(0.0) * (0.38 + texture * 0.30);
    let tracers = (1.0
        - ((x * (12.0 + density * 15.0) + y * (4.0 + curve * 6.0) - time * 1.4 + phase)
            .sin()
            .abs()
            * 9.0))
        .max(0.0)
        * 0.24;

    ribbons
        .max(ring * (0.35 + orbit * 0.55))
        .max(scanner)
        .max(tracers)
        + grain
}

fn braille_dot(x: usize, y: usize) -> u8 {
    match (x, y) {
        (0, 0) => 0x01,
        (0, 1) => 0x02,
        (0, 2) => 0x04,
        (0, 3) => 0x40,
        (1, 0) => 0x08,
        (1, 1) => 0x10,
        (1, 2) => 0x20,
        (1, 3) => 0x80,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_fills_the_requested_braille_grid() {
        let visualizer = LiveVisualizer::new("NTS 1");
        let output = visualizer.braille(12, 3);
        assert_eq!(output.lines().count(), 3);
        assert!(output.lines().all(|line| line.chars().count() == 12));
    }

    #[test]
    fn show_seeds_produce_distinct_fields() {
        let first = LiveVisualizer::new("NTS 1").braille(30, 6);
        let second = LiveVisualizer::new("NTS 2").braille(30, 6);
        assert_ne!(first, second);
    }
}
