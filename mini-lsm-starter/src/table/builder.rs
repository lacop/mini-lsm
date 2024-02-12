#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        } else {
            debug_assert!(self.last_key.as_key_slice() <= key);
        }

        // Try to add to current block.
        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // Didn't fit, we have to finalize the current block and start a new one.
        self.finalize_block();

        // Start new block with this key. Must fit.
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // Underestimating by only including the data, but this should be good enough.
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path.
    /// Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finalize_block();
        let mut buffer = self.data;
        let metadata_offset = buffer.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buffer);
        buffer.put_u32(metadata_offset as u32);
        let file = FileObject::create(path.as_ref(), buffer)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: metadata_offset,
            id,
            block_cache,
            first_key: self.first_key.into_key_bytes(),
            last_key: self.last_key.into_key_bytes(),
            bloom: None,
            max_ts: 0,
        })
    }

    fn finalize_block(&mut self) {
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            // TODO: Avoid clone here?
            first_key: self.first_key.clone().into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
        });
        // Block is never empty, unless the table itself is empty.
        let full_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let full_block = full_builder.build();
        self.data.extend(full_block.encode());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
