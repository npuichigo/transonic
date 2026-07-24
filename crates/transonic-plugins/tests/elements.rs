//! Integration tests for the custom GStreamer elements, mirroring the scope
//! of the original `StartSilenceCutterTests.cpp` / `BackgroundAudioMixerTests.cpp`
//! suites (plus coverage for `demuxtomultichannel` and `passthroughdecoder`).

use std::sync::Once;

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        gst::init().expect("gstreamer initialization");
        transonic_plugins::plugin_register_static().expect("plugin registration");
    });
}

const S16_MONO_16K: &str = "audio/x-raw,format=S16LE,rate=16000,channels=1,layout=interleaved";

fn samples_to_bytes(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}

fn bytes_to_samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

/// Runs a pipeline description with named appsrc feeds and appsink drains.
/// Feeds are pushed in order (all buffers of the first source, then EOS on
/// it, then the second source, ...).  Returns the collected bytes and buffer
/// list per sink, plus all element messages posted on the bus.
fn run_pipeline(
    description: &str,
    feeds: &[(&str, Vec<Vec<u8>>)],
    sinks: &[&str],
) -> (
    std::collections::HashMap<String, Vec<gst::Buffer>>,
    Vec<gst::Structure>,
) {
    init();
    let pipeline = gst::parse::launch(description)
        .expect("pipeline parses")
        .downcast::<gst::Pipeline>()
        .expect("is a pipeline");

    let bus = pipeline.bus().expect("bus");
    pipeline
        .set_state(gst::State::Playing)
        .expect("pipeline starts");

    // Feed sources on a separate thread: some elements (the mixer) block
    // the streaming thread until another source completes, so pushing must
    // not depend on downstream progress.
    let feed_pipeline = pipeline.clone();
    let feeds: Vec<(String, Vec<Vec<u8>>)> = feeds
        .iter()
        .map(|(name, buffers)| (name.to_string(), buffers.clone()))
        .collect();
    let feeder = std::thread::spawn(move || {
        for (name, buffers) in feeds {
            let appsrc = feed_pipeline
                .by_name(&name)
                .expect("appsrc exists")
                .downcast::<gst_app::AppSrc>()
                .expect("is an appsrc");
            for data in buffers {
                appsrc
                    .push_buffer(gst::Buffer::from_mut_slice(data))
                    .expect("push succeeds");
            }
            appsrc.end_of_stream().expect("eos succeeds");
        }
    });

    // Drain the sinks.
    let mut outputs = std::collections::HashMap::new();
    for name in sinks {
        let appsink = pipeline
            .by_name(name)
            .expect("appsink exists")
            .downcast::<gst_app::AppSink>()
            .expect("is an appsink");
        let mut buffers = Vec::new();
        // Pull until EOS or error.
        while let Ok(sample) = appsink.pull_sample() {
            if let Some(buffer) = sample.buffer_owned() {
                buffers.push(buffer);
            }
        }
        outputs.insert(name.to_string(), buffers);
    }

    feeder.join().expect("feeder thread");

    // Collect element messages posted so far.
    let mut element_messages = Vec::new();
    while let Some(message) = bus.pop() {
        if let gst::MessageView::Element(element) = message.view() {
            if let Some(structure) = element.structure() {
                element_messages.push(structure.to_owned());
            }
        }
    }

    pipeline
        .set_state(gst::State::Null)
        .expect("pipeline stops");
    (outputs, element_messages)
}

fn concat_bytes(buffers: &[gst::Buffer]) -> Vec<u8> {
    buffers
        .iter()
        .flat_map(|b| {
            let map = b.map_readable().expect("map");
            map.as_slice().to_vec()
        })
        .collect()
}

///////////////////////////////////////////////////////////////////////////
// passthroughdecoder
///////////////////////////////////////////////////////////////////////////

#[test]
fn passthroughdecoder_passes_data_through() {
    let data: Vec<u8> = (0..3200u32).map(|i| (i % 251) as u8).collect();
    let (outputs, _) = run_pipeline(
        "appsrc name=src caps=audio/passthrough ! passthroughdecoder ! appsink name=sink",
        &[("src", vec![data.clone()])],
        &["sink"],
    );
    assert_eq!(concat_bytes(&outputs["sink"]), data);
}

///////////////////////////////////////////////////////////////////////////
// startsilencecutter
///////////////////////////////////////////////////////////////////////////

fn silence(ms: usize) -> Vec<i16> {
    vec![0i16; 16 * ms]
}

fn tone(ms: usize) -> Vec<i16> {
    (0..16 * ms)
        .map(|i| if i % 2 == 0 { 20_000 } else { -20_000 })
        .collect()
}

#[test]
fn startsilencecutter_trims_leading_silence() {
    let leading_silence = samples_to_bytes(&silence(1000));
    let speech = samples_to_bytes(&tone(100));

    let (outputs, messages) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! startsilencecutter trim=true \
             pre-length=200000000 ! appsink name=sink"
        ),
        &[("src", vec![leading_silence, speech.clone()])],
        &["sink"],
    );

    let output = concat_bytes(&outputs["sink"]);
    // The retained window is at most `pre-length` (200 ms) of silence plus
    // the speech itself; the ~800 ms of older silence must be gone.
    assert!(output.len() < samples_to_bytes(&silence(1000)).len());
    assert!(output.len() >= speech.len());
    assert!(output.ends_with(&speech));

    assert!(messages
        .iter()
        .any(|s| s.name() == "startsilenceenddetected"));
}

#[test]
fn startsilencecutter_passes_loud_signal_through() {
    let speech = samples_to_bytes(&tone(200));
    let (outputs, messages) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! startsilencecutter trim=true ! \
             appsink name=sink"
        ),
        &[("src", vec![speech.clone()])],
        &["sink"],
    );
    assert_eq!(concat_bytes(&outputs["sink"]), speech);
    assert!(messages
        .iter()
        .any(|s| s.name() == "startsilenceenddetected"));
}

#[test]
fn startsilencecutter_flushes_window_on_eos_while_silent() {
    let quiet = samples_to_bytes(&silence(100));
    let (outputs, messages) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! startsilencecutter trim=true ! \
             appsink name=sink"
        ),
        &[("src", vec![quiet.clone()])],
        &["sink"],
    );
    // On EOS while silent the retained pre-record window is flushed.
    assert_eq!(concat_bytes(&outputs["sink"]), quiet);
    assert!(messages
        .iter()
        .any(|s| s.name() == "startsilenceenddetected"));
}

///////////////////////////////////////////////////////////////////////////
// smartvadstartsilencetrimmer
///////////////////////////////////////////////////////////////////////////

#[test]
fn smartvad_trims_and_tags_silence() {
    let leading_silence = samples_to_bytes(&silence(1000));
    let speech = samples_to_bytes(&tone(100));

    let (outputs, messages) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! smartvadstartsilencetrimmer trim=true \
             ! appsink name=sink"
        ),
        &[("src", vec![leading_silence, speech.clone()])],
        &["sink"],
    );

    let output = concat_bytes(&outputs["sink"]);
    assert!(output.ends_with(&speech));
    assert!(output.len() < samples_to_bytes(&silence(1000)).len() + speech.len());

    // Buffers that leave the element after detection carry a
    // non-silence flag in the custom audio meta.
    let last = outputs["sink"].last().expect("at least one buffer");
    let meta = transonic_meta::get_custom_audio_meta(last);
    assert_eq!(meta.map(|m| m.silence), Some(false));

    let detection = messages
        .iter()
        .find(|s| s.name() == "startsilenceenddetected")
        .expect("detection message");
    assert_eq!(detection.get::<bool>("onlysilencedetected"), Ok(false));
}

#[test]
fn smartvad_reports_only_silence_at_eos() {
    let quiet = samples_to_bytes(&silence(100));
    let (_, messages) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! smartvadstartsilencetrimmer ! \
             appsink name=sink"
        ),
        &[("src", vec![quiet])],
        &["sink"],
    );
    let detection = messages
        .iter()
        .find(|s| s.name() == "startsilenceenddetected")
        .expect("detection message");
    assert_eq!(detection.get::<bool>("onlysilencedetected"), Ok(true));
}

///////////////////////////////////////////////////////////////////////////
// backgroundaudiomixer
///////////////////////////////////////////////////////////////////////////

#[test]
fn mixer_mixes_background_into_audio() {
    let audio = vec![1000i16; 1600];
    let background = vec![100i16; 1600];

    let (outputs, _) = run_pipeline(
        &format!(
            "backgroundaudiomixer name=mixer volume=2.0 \
             appsrc name=bg caps={S16_MONO_16K} ! queue ! mixer.sink-backgroundaudio \
             appsrc name=main caps={S16_MONO_16K} ! queue ! mixer.sink-audio \
             mixer.src-mixed ! appsink name=sink"
        ),
        // The background source is fed (and completed) first; the mixer
        // blocks the audio chain until the background is complete.
        &[
            ("bg", vec![samples_to_bytes(&background)]),
            ("main", vec![samples_to_bytes(&audio)]),
        ],
        &["sink"],
    );

    let mixed = bytes_to_samples(&concat_bytes(&outputs["sink"]));
    assert_eq!(mixed.len(), audio.len());
    // audio + volume * background = 1000 + 2 * 100.
    assert!(
        mixed.iter().all(|&s| s == 1200),
        "unexpected mix: {:?}",
        &mixed[..8]
    );
}

#[test]
fn mixer_loops_short_background() {
    let audio = vec![0i16; 3200];
    // A short background track: 800 samples of value 100.
    let background = vec![100i16; 800];

    let (outputs, _) = run_pipeline(
        &format!(
            "backgroundaudiomixer name=mixer volume=1.0 \
             appsrc name=bg caps={S16_MONO_16K} ! queue ! mixer.sink-backgroundaudio \
             appsrc name=main caps={S16_MONO_16K} ! queue ! mixer.sink-audio \
             mixer.src-mixed ! appsink name=sink"
        ),
        &[
            ("bg", vec![samples_to_bytes(&background)]),
            ("main", vec![samples_to_bytes(&audio)]),
        ],
        &["sink"],
    );

    let mixed = bytes_to_samples(&concat_bytes(&outputs["sink"]));
    assert_eq!(mixed.len(), audio.len());
    // The background loops across the whole output.
    assert!(mixed.iter().all(|&s| s == 100));
}

///////////////////////////////////////////////////////////////////////////
// demuxtomultichannel
///////////////////////////////////////////////////////////////////////////

#[test]
fn demux_splits_stereo_into_mono_streams() {
    // Interleaved stereo: left = 1, right = 2.
    let mut interleaved = Vec::new();
    for _ in 0..800 {
        interleaved.push(1i16);
        interleaved.push(2i16);
    }

    let (outputs, _) = run_pipeline(
        "appsrc name=src caps=audio/x-raw,format=S16LE,rate=16000,channels=2,layout=interleaved \
         ! demuxtomultichannel name=d channels=2 \
         d.src_0 ! queue ! appsink name=left \
         d.src_1 ! queue ! appsink name=right",
        &[("src", vec![samples_to_bytes(&interleaved)])],
        &["left", "right"],
    );

    let left = bytes_to_samples(&concat_bytes(&outputs["left"]));
    let right = bytes_to_samples(&concat_bytes(&outputs["right"]));
    assert_eq!(left.len(), 800);
    assert_eq!(right.len(), 800);
    assert!(left.iter().all(|&s| s == 1));
    assert!(right.iter().all(|&s| s == 2));
}

#[test]
fn startsilencecutter_forwards_silence_when_not_trimming() {
    let leading_silence = samples_to_bytes(&silence(500));
    let speech = samples_to_bytes(&tone(100));

    let (outputs, _) = run_pipeline(
        &format!(
            "appsrc name=src caps={S16_MONO_16K} ! startsilencecutter trim=false ! \
             appsink name=sink"
        ),
        &[("src", vec![leading_silence.clone(), speech.clone()])],
        &["sink"],
    );

    // Without trimming, evicted pre-record data is forwarded, so nothing is
    // lost.
    let output = concat_bytes(&outputs["sink"]);
    assert_eq!(output.len(), leading_silence.len() + speech.len());
    assert!(output.ends_with(&speech));
}

#[test]
fn startsilencecutter_threshold_db_maps_to_linear_threshold() {
    init();
    let element = gst::ElementFactory::make("startsilencecutter")
        .build()
        .expect("element exists");
    // -20 dB corresponds to a linear threshold of 0.1.
    element.set_property("threshold-dB", -20.0f64);
    let linear: f64 = element.property("threshold");
    assert!((linear - 0.1).abs() < 1e-9, "got {linear}");
}
