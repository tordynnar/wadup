use crate::types::Blob;
use anyhow::{Result, anyhow};

pub struct Carve {
    pub data: Blob,
    pub offset: usize,
    pub length: usize,
}

impl Carve {
    pub fn new(data: Blob, offset: usize, length: usize) -> Result<Carve> {
        if offset + length > data.as_ref().as_ref().len() {
            Err(anyhow!("carve out of bounds"))
        } else {
            Ok(Carve { data, offset, length })
        }
    }
}

const EMPTY_CARVE: &[u8] = &[];

impl AsRef<[u8]> for Carve {
    fn as_ref(&self) -> &[u8] {
        let data = self.data.as_ref().as_ref();
        data.get(self.offset..(self.offset+self.length)).unwrap_or(EMPTY_CARVE)
    }
}