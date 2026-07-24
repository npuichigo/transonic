//! A simple frame-based voice activity detector.
//!
//! Used by the `smartvadstartsilencetrimmer` element.  It is an energy-based
//! detector with a simple call shape: feed bytes, get per-frame speech
//! decisions.  Swap this implementation out for a model-based VAD if higher
//! accuracy is required.

/// Frame length used for VAD decisions.
const FRAME_MS: u32 = 20;

/// An energy (RMS) based voice activity detector for S16LE mono audio.
pub struct EnergyVad {
    threshold: f64,
    frame_bytes: usize,
    pending: Vec<u8>,
}

impl EnergyVad {
    /// Creates a detector for the given RMS threshold (in normalized
    /// `[0.0, 1.0]` full-scale units) and sample rate.
    pub fn new(threshold: f64, sample_rate: u32) -> Self {
        let frame_samples = (sample_rate * FRAME_MS / 1000).max(1) as usize;
        EnergyVad {
            threshold,
            frame_bytes: frame_samples * 2,
            pending: Vec::new(),
        }
    }

    /// The number of bytes the detector wants before it can produce the next
    /// frame decision.
    #[allow(dead_code)]
    pub fn bytes_needed(&self) -> usize {
        self.frame_bytes.saturating_sub(self.pending.len())
    }

    /// Feeds audio bytes into the detector and returns the speech decisions
    /// for every completed frame.
    pub fn process(&mut self, data: &[u8]) -> Vec<bool> {
        self.pending.extend_from_slice(data);
        let mut decisions = Vec::new();
        while self.pending.len() >= self.frame_bytes {
            let frame: Vec<u8> = self.pending.drain(..self.frame_bytes).collect();
            decisions.push(self.is_speech_frame(&frame));
        }
        decisions
    }

    fn is_speech_frame(&self, frame: &[u8]) -> bool {
        let sample_count = frame.len() / 2;
        if sample_count == 0 {
            return false;
        }
        let mut square_sum = 0.0f64;
        for chunk in frame.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]) as f64;
            square_sum += sample * sample;
        }
        // Normalize to a [0.0, 1.0] full-scale range (2^30 == (2^15)^2).
        let normalized_mean_square = square_sum / f64::from(1u32 << 30) / sample_count as f64;
        normalized_mean_square.sqrt() >= self.threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples_to_bytes(samples: &[i16]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    #[test]
    fn silence_is_not_speech() {
        let mut vad = EnergyVad::new(0.1, 16_000);
        let decisions = vad.process(&samples_to_bytes(&[0i16; 320]));
        assert_eq!(decisions, vec![false]);
    }

    #[test]
    fn loud_signal_is_speech() {
        let mut vad = EnergyVad::new(0.1, 16_000);
        let samples: Vec<i16> = (0..320)
            .map(|i| if i % 2 == 0 { 20_000 } else { -20_000 })
            .collect();
        let decisions = vad.process(&samples_to_bytes(&samples));
        assert_eq!(decisions, vec![true]);
    }

    #[test]
    fn partial_frames_are_buffered() {
        let mut vad = EnergyVad::new(0.1, 16_000);
        assert!(vad.process(&samples_to_bytes(&[0i16; 100])).is_empty());
        assert!(vad.bytes_needed() > 0);
        assert_eq!(vad.process(&samples_to_bytes(&[0i16; 220])), vec![false]);
    }
}
