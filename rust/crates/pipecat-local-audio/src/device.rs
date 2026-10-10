use crate::{native_error, LocalAudioError};
use cpal::traits::{DeviceTrait, HostTrait};
use pipecat_audio::PcmFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DeviceSelector {
    #[default]
    Default,
    /// Index from `list_devices`, shared across input and output directions.
    Index(usize),
    /// Native CPAL device ID from `list_devices`.
    Id(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioDevice {
    pub index: usize,
    pub id: String,
    pub name: String,
    pub input: bool,
    pub output: bool,
    pub default_input: bool,
    pub default_output: bool,
}

/// Enumerate native devices without opening a capture or playback stream.
pub fn list_devices() -> Result<Vec<AudioDevice>, LocalAudioError> {
    let host = cpal::default_host();
    let input = host.default_input_device();
    let output = host.default_output_device();
    host.devices()
        .map_err(native_error)?
        .enumerate()
        .map(|(index, device)| {
            Ok(AudioDevice {
                index,
                id: device.id().map_err(native_error)?.to_string(),
                name: device.to_string(),
                input: device.supports_input(),
                output: device.supports_output(),
                default_input: input.as_ref() == Some(&device),
                default_output: output.as_ref() == Some(&device),
            })
        })
        .collect()
}

pub(crate) fn select(
    selector: &DeviceSelector,
    direction: DeviceDirection,
) -> Result<cpal::Device, LocalAudioError> {
    let host = cpal::default_host();
    let device = match selector {
        DeviceSelector::Default => match direction {
            DeviceDirection::Input => host.default_input_device(),
            DeviceDirection::Output => host.default_output_device(),
        },
        DeviceSelector::Index(index) => host.devices().map_err(native_error)?.nth(*index),
        DeviceSelector::Id(id) => {
            let mut found = None;
            for device in host.devices().map_err(native_error)? {
                if device.id().map_err(native_error)?.to_string() == *id {
                    found = Some(device);
                    break;
                }
            }
            found
        }
    }
    .ok_or(LocalAudioError::DeviceNotFound(direction))?;
    Ok(device)
}

pub(crate) fn config(
    device: &cpal::Device,
    direction: DeviceDirection,
    format: PcmFormat,
) -> Result<(cpal::StreamConfig, cpal::SampleFormat), LocalAudioError> {
    let ranges: Vec<_> = match direction {
        DeviceDirection::Input => device
            .supported_input_configs()
            .map_err(native_error)?
            .collect(),
        DeviceDirection::Output => device
            .supported_output_configs()
            .map_err(native_error)?
            .collect(),
    };
    choose_config(ranges, format)
}

pub(crate) fn choose_config(
    ranges: impl IntoIterator<Item = cpal::SupportedStreamConfigRange>,
    format: PcmFormat,
) -> Result<(cpal::StreamConfig, cpal::SampleFormat), LocalAudioError> {
    let range = ranges
        .into_iter()
        .filter(|range| {
            range.channels() == format.channels()
                && range.min_sample_rate() <= format.sample_rate()
                && range.max_sample_rate() >= format.sample_rate()
                && supported(range.sample_format())
        })
        .min_by_key(|range| match range.sample_format() {
            cpal::SampleFormat::I16 => 0,
            cpal::SampleFormat::F32 => 1,
            _ => 2,
        })
        .ok_or(LocalAudioError::UnsupportedFormat {
            rate: format.sample_rate(),
            channels: format.channels(),
        })?;
    let selected = range.with_sample_rate(format.sample_rate());
    Ok((selected.config(), selected.sample_format()))
}

fn supported(format: cpal::SampleFormat) -> bool {
    matches!(
        format,
        cpal::SampleFormat::I8
            | cpal::SampleFormat::I16
            | cpal::SampleFormat::I24
            | cpal::SampleFormat::I32
            | cpal::SampleFormat::I64
            | cpal::SampleFormat::U8
            | cpal::SampleFormat::U16
            | cpal::SampleFormat::U24
            | cpal::SampleFormat::U32
            | cpal::SampleFormat::U64
            | cpal::SampleFormat::F32
            | cpal::SampleFormat::F64
    )
}
