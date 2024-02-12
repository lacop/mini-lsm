#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full,
    /// but first call to add always succeed, even if it would exceed the block size.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        const U16BYTES: usize = std::mem::size_of::<u16>();
        // Currently stored blocks and offsets.
        let mut total_size = self.offsets.len() * U16BYTES + self.data.len();
        total_size += U16BYTES; // Final size value.
                                // The newly added key and value, plus 2*u16 for their lengths.
        total_size += key.len() + value.len() + 2 * U16BYTES;
        if total_size > self.block_size && !self.offsets.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
