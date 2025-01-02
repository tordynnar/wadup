use std::fs::File;
use crossbeam::channel::Sender;

pub struct Mmap {
    inner: memmap2::Mmap,
    len: u64,
    free_sender: Sender<u64>,
}

impl Mmap {
    pub fn new(file: &File, len: u64, free_sender: Sender<u64>) -> Mmap {
        Mmap {
            inner: unsafe { memmap2::Mmap::map(file).unwrap() },
            len,
            free_sender,
        }
    }
}

impl AsRef<[u8]> for Mmap {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        let _ = self.free_sender.send(self.len);
    }
}

//let input_blob : Blob = Arc::new(unsafe { Mmap::map(&input_file).unwrap() });