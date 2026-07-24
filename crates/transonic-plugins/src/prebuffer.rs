//! Pre-record buffer shared by the silence-trimming elements.
//!
//! While the stream is still considered silent, incoming buffers are held in
//! a bounded pre-record window.  Data older than the window is either pushed
//! downstream (when not trimming) or dropped (when trimming).  When speech
//! is detected the whole window is flushed downstream.

use std::collections::VecDeque;

use gstreamer as gst;

/// A time-bounded FIFO of buffers.
#[derive(Default)]
pub struct PreBuffer {
    buffers: VecDeque<gst::Buffer>,
    /// Total buffered duration, in nanoseconds.
    run_length_ns: u64,
}

impl PreBuffer {
    /// Total buffered duration, in nanoseconds.
    pub fn run_length_ns(&self) -> u64 {
        self.run_length_ns
    }

    /// Appends a buffer covering `duration_ns` nanoseconds.
    pub fn push(&mut self, buffer: gst::Buffer, duration_ns: u64) {
        self.buffers.push_back(buffer);
        self.run_length_ns += duration_ns;
    }

    /// Removes and returns all buffered data (used when speech is detected
    /// or at EOS).
    pub fn take_all(&mut self) -> Vec<gst::Buffer> {
        self.run_length_ns = 0;
        self.buffers.drain(..).collect()
    }

    /// Clears the buffered data without returning it.
    pub fn clear(&mut self) {
        self.run_length_ns = 0;
        self.buffers.clear();
    }

    /// Evicts data so that at most `pre_length_ns` nanoseconds stay
    /// buffered.  Returns the evicted buffers (oldest first); buffers may be
    /// split so the retained window is exact.
    pub fn drain_excess(
        &mut self,
        pre_length_ns: u64,
        bytes_per_frame: usize,
        rate: u64,
    ) -> Vec<gst::Buffer> {
        let mut evicted = Vec::new();

        while self.run_length_ns > pre_length_ns {
            let Some(buffer) = self.buffers.pop_front() else {
                break;
            };
            let size = buffer.size();
            let frames = (size / bytes_per_frame.max(1)) as u64;
            let duration_ns = frames
                .checked_mul(gst::ClockTime::SECOND.nseconds())
                .map(|n| n / rate.max(1))
                .unwrap_or(0);

            if self.run_length_ns.saturating_sub(duration_ns) < pre_length_ns {
                // Removing the whole buffer would leave less than the
                // configured window, so remove only part of it.
                let split_duration_ns = if duration_ns > pre_length_ns {
                    duration_ns - pre_length_ns
                } else {
                    self.run_length_ns - pre_length_ns
                };
                let split_offset = if duration_ns > 0 {
                    ((split_duration_ns as u128 * size as u128) / duration_ns as u128) as usize
                } else {
                    0
                };
                let split_offset = split_offset.min(size);

                let part_to_remove = buffer
                    .copy_region(gst::BufferCopyFlags::all(), 0..split_offset)
                    .unwrap_or_else(|_| buffer.copy());
                let part_to_keep = buffer
                    .copy_region(gst::BufferCopyFlags::all(), split_offset..size)
                    .unwrap_or_else(|_| buffer.copy());

                self.buffers.push_front(part_to_keep);
                self.run_length_ns = self.run_length_ns.saturating_sub(split_duration_ns);
                evicted.push(part_to_remove);
            } else {
                self.run_length_ns = self.run_length_ns.saturating_sub(duration_ns);
                evicted.push(buffer);
            }
        }

        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 16 kHz mono S16LE: 32 bytes per millisecond.
    const BPF: usize = 2;
    const RATE: u64 = 16_000;
    const MS: u64 = 1_000_000;

    fn init() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| gst::init().expect("gstreamer initialization"));
    }

    fn buffer_of_ms(ms: usize) -> (gst::Buffer, u64) {
        let bytes = vec![0u8; 32 * ms];
        (gst::Buffer::from_mut_slice(bytes), ms as u64 * MS)
    }

    #[test]
    fn take_all_returns_everything_in_order() {
        init();
        let mut pre = PreBuffer::default();
        let (a, dur) = buffer_of_ms(10);
        let (b, _) = buffer_of_ms(20);
        pre.push(a, dur);
        pre.push(b, 20 * MS);
        assert_eq!(pre.run_length_ns(), 30 * MS);

        let all = pre.take_all();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].size(), 32 * 10);
        assert_eq!(all[1].size(), 32 * 20);
        assert_eq!(pre.run_length_ns(), 0);
    }

    #[test]
    fn drain_evicts_whole_buffers_beyond_the_window() {
        init();
        let mut pre = PreBuffer::default();
        for _ in 0..5 {
            let (buffer, dur) = buffer_of_ms(100);
            pre.push(buffer, dur);
        }

        // Keep 200 ms: exactly three whole 100 ms buffers must go.
        let evicted = pre.drain_excess(200 * MS, BPF, RATE);
        assert_eq!(evicted.len(), 3);
        assert_eq!(pre.run_length_ns(), 200 * MS);
    }

    #[test]
    fn drain_splits_a_buffer_for_an_exact_window() {
        init();
        let mut pre = PreBuffer::default();
        let (buffer, dur) = buffer_of_ms(300);
        pre.push(buffer, dur);

        // Keep 200 ms out of a single 300 ms buffer: the leading 100 ms are
        // split off and evicted.
        let evicted = pre.drain_excess(200 * MS, BPF, RATE);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].size(), 32 * 100);
        assert_eq!(pre.run_length_ns(), 200 * MS);

        let kept = pre.take_all();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].size(), 32 * 200);
    }

    #[test]
    fn drain_keeps_data_within_the_window_untouched() {
        init();
        let mut pre = PreBuffer::default();
        let (buffer, dur) = buffer_of_ms(100);
        pre.push(buffer, dur);
        assert!(pre.drain_excess(200 * MS, BPF, RATE).is_empty());
        assert_eq!(pre.run_length_ns(), 100 * MS);
    }

    #[test]
    fn clear_discards_buffered_data() {
        init();
        let mut pre = PreBuffer::default();
        let (buffer, dur) = buffer_of_ms(50);
        pre.push(buffer, dur);
        pre.clear();
        assert_eq!(pre.run_length_ns(), 0);
        assert!(pre.take_all().is_empty());
    }
}
