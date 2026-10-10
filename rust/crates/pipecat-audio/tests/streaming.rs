use pipecat_audio::*;

#[test]
fn pcm_decoder_preserves_arbitrary_byte_boundaries_and_channels() {
    let samples = [i16::MIN, 1, 0, i16::MAX, 123, -456];
    let pcm = encode_pcm16_le(&samples);
    for size in 1..=pcm.len() {
        let mut decoder = PcmStreamDecoder::new(PcmFormat::new(48000, 2).unwrap());
        let actual: Vec<i16> = pcm
            .chunks(size)
            .flat_map(|chunk| decoder.push(chunk))
            .collect();
        assert_eq!(actual, samples);
        decoder.finish().unwrap();
        assert_eq!(decoder.pending_bytes(), 0);
    }
}

#[test]
fn decoder_incomplete_final_frame_is_recoverable() {
    let mut decoder = PcmStreamDecoder::new(PcmFormat::new(8000, 2).unwrap());
    assert!(decoder.push(&[1, 0, 2]).is_empty());
    assert!(decoder.finish().is_err());
    assert_eq!(decoder.push(&[0]), [1, 2]);
    decoder.finish().unwrap();
    decoder.push(&[1]);
    decoder.reset();
    decoder.finish().unwrap();
}

#[test]
fn chunker_returns_fixed_frames_and_aligned_tail() {
    let pcm = encode_pcm16_le(&(0..22).collect::<Vec<i16>>());
    for size in 1..=pcm.len() {
        let mut chunker = PcmChunker::new(PcmFormat::new(16000, 2).unwrap(), 3).unwrap();
        let mut result = Vec::new();
        for chunk in pcm.chunks(size) {
            for complete in chunker.push(chunk) {
                assert_eq!(complete.len(), 12);
                result.extend(complete);
            }
            assert!(chunker.pending_bytes() < 12);
        }
        let tail = chunker.flush().unwrap();
        assert_eq!(tail.len(), 8);
        result.extend(tail);
        assert_eq!(result, pcm);
        assert!(chunker.flush().unwrap().is_empty());
    }
}

#[test]
fn chunker_errors_do_not_consume_pending_bytes() {
    let format = PcmFormat::new(8000, 2).unwrap();
    assert!(PcmChunker::new(format, 0).is_err());
    assert!(PcmChunker::new(format, usize::MAX).is_err());
    let mut chunker = PcmChunker::new(format, 3).unwrap();
    chunker.push(&[1, 0, 2]);
    assert!(chunker.flush().is_err());
    assert_eq!(chunker.pending_bytes(), 3);
    chunker.push(&[0]);
    assert_eq!(chunker.flush().unwrap(), [1, 0, 2, 0]);
    chunker.push(&[1]);
    chunker.reset();
    assert!(chunker.flush().unwrap().is_empty());
}
