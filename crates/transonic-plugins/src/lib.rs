//! Custom GStreamer elements:
//!
//! * `backgroundaudiomixer` — mixes a looping background audio track into a
//!   primary audio stream, with volume, fade-in and fade-out support.
//! * `startsilencecutter` — RMS-based leading-silence detection/trimming.
//! * `smartvadstartsilencetrimmer` — VAD-based leading-silence
//!   detection/trimming that also tags buffers with silence metadata.
//! * `demuxtomultichannel` — splits a multichannel audio stream into many
//!   mono streams.
//! * `passthroughdecoder` — presents arbitrary data as raw S16LE/16kHz mono
//!   audio (transitional tool).
//!
//! The plugin can be loaded as a regular GStreamer plugin (cdylib) or
//! registered statically via [`plugin_register_static`].

use gstreamer as gst;

mod backgroundaudiomixer;
mod demuxtomultichannel;
mod passthroughdecoder;
mod prebuffer;
mod smartvadstartsilencetrimmer;
mod startsilencecutter;
mod vad;

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    transonic_meta::register();
    backgroundaudiomixer::register(plugin)?;
    demuxtomultichannel::register(plugin)?;
    passthroughdecoder::register(plugin)?;
    smartvadstartsilencetrimmer::register(plugin)?;
    startsilencecutter::register(plugin)?;
    Ok(())
}

gst::plugin_define!(
    transonic,
    env!("CARGO_PKG_DESCRIPTION"),
    plugin_init,
    concat!(env!("CARGO_PKG_VERSION"), "-", "release"),
    "MIT/X11",
    env!("CARGO_PKG_NAME"),
    env!("CARGO_PKG_NAME"),
    "https://github.com/transonic"
);
