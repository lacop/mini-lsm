#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        if heap.is_empty() {
            return Self {
                iters: heap,
                current: None,
            };
        }
        let first = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(first),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|w| w.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Advance all other iterators to the current key (or the next one, if this key is not present).
        while let Some(mut smallest_iter) = self.iters.peek_mut() {
            match smallest_iter.1.key().cmp(&current.1.key()) {
                // This key will come later.
                cmp::Ordering::Greater => break,
                // Heap ordering gives us this key from the earliest iterator first, ie. the `current` one.
                // Other copies of this key are ignored.
                cmp::Ordering::Equal => {
                    // Error when advancing.
                    if let e @ Err(_) = smallest_iter.1.next() {
                        PeekMut::pop(smallest_iter);
                        return e;
                    }
                    // No longer valid after advancing, remove it.
                    if !smallest_iter.1.is_valid() {
                        PeekMut::pop(smallest_iter);
                    }
                }
                cmp::Ordering::Less => {
                    unreachable!("Broken invariant, we should have popped this earlier")
                }
            }
        }

        // Advance the current iterator and remove it if it's no longer valid.
        current.1.next()?;
        if !current.1.is_valid() {
            self.current = self.iters.pop();
        } else if let Some(mut smallest_iter) = self.iters.peek_mut() {
            // Check if current iterator is still has smallest key, and swap it if not.
            // Note: the comparison seems backwards, but the comparator for the HeapWrapper makes
            // the smaller key from earlier iterator larger (needed for max heap).
            if *current < *smallest_iter {
                std::mem::swap(&mut *smallest_iter, current);
            }
        }

        Ok(())
    }
}
