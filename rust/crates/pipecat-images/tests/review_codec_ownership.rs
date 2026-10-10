//! Linux black-box guard for decoder thread ownership. This runs in its own
//! integration-test process, so no concurrent service decoder can confuse it.
#[cfg(target_os = "linux")]
#[test]
fn large_jpeg_uses_only_the_calling_decoder_thread() {
    use std::io::{self, Cursor, Read};
    struct ObservedRead {
        bytes: Cursor<&'static [u8]>,
        saw_component_thread: bool,
    }
    impl Read for ObservedRead {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            for entry in std::fs::read_dir("/proc/self/task")? {
                let path = entry?.path().join("comm");
                // A thread can exit between enumerating its ID and reading its name.
                if let Ok(name) = std::fs::read_to_string(path) {
                    self.saw_component_thread |= name.starts_with("worker thread f");
                }
            }
            let length = buffer.len().min(256);
            self.bytes.read(&mut buffer[..length])
        }
    }
    let mut input = ObservedRead {
        bytes: Cursor::new(include_bytes!("fixtures/source-cat.jpg")),
        saw_component_thread: false,
    };
    {
        let mut decoder = jpeg_decoder::Decoder::new(&mut input);
        decoder.read_info().unwrap();
        let info = decoder.info().unwrap();
        assert!(info.width > 128 && info.height > 128);
        let pixels = decoder.decode().unwrap();
        assert_eq!(pixels.len(), info.width as usize * info.height as usize * 3);
    }
    assert!(
        !input.saw_component_thread,
        "JPEG decoding started detached component workers outside the owned decoder thread"
    );
}
