mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buffer = self.data.clone();
        for offset in &self.offsets {
            buffer.put_u16(*offset);
        }
        buffer.put_u16(self.offsets.len() as u16);
        buffer.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        const U16BYTES: usize = std::mem::size_of::<u16>();
        let count = (&data[data.len() - U16BYTES..]).get_u16() as usize;
        let first_offset_pos = data.len() - U16BYTES * (count + 1);
        Self {
            data: data[..first_offset_pos].to_vec(),
            offsets: data[first_offset_pos..data.len() - U16BYTES]
                .chunks_exact(U16BYTES)
                .map(|mut chunk| chunk.get_u16())
                .collect(),
        }
    }
}
