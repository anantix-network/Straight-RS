use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

macro_rules! opt_struct {
    ($(#[$m:meta])* $name:ident { $($field:ident),* $(,)? }) => {
        $(#[$m])*
        #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            $( #[serde(default, skip_serializing_if = "Option::is_none")] pub $field: Option<f64>, )*
        }
    };
}

opt_struct!(Karaoke { level, mono_level, filter_band, filter_width });
opt_struct!(Timescale { speed, pitch, rate });
opt_struct!(Tremolo { frequency, depth });
opt_struct!(Vibrato { frequency, depth });
opt_struct!(Rotation { rotation_hz });
opt_struct!(Distortion { sin_offset, sin_scale, cos_offset, cos_scale, tan_offset, tan_scale, offset, scale });
opt_struct!(ChannelMix { left_to_left, left_to_right, right_to_left, right_to_right });
opt_struct!(LowPass { smoothing });

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct EqBand {
    pub band: u8,
    pub gain: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equalizer: Option<Vec<EqBand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub karaoke: Option<Karaoke>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timescale: Option<Timescale>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tremolo: Option<Tremolo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vibrato: Option<Vibrato>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation: Option<Rotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distortion: Option<Distortion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_mix: Option<ChannelMix>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub low_pass: Option<LowPass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_filters: Option<Map<String, Value>>,
}
