use std::fs::File;
use std::sync::mpmc::Sender;
use anyhow::Result;

pub struct Mmap {
    inner: memmap2::Mmap,
    len: u64,
    free_sender: Sender<u64>,
}

impl Mmap {
    pub fn new(file: &File, len: u64, free_sender: Sender<u64>) -> Result<Mmap> {
        Ok(Mmap {
            inner: unsafe { memmap2::Mmap::map(file)? },
            len,
            free_sender,
        })
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
