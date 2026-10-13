//! Durable storage: per-partition segmented logs of batch envelopes and legacy records.

#[cfg(test)]
mod batch_tests;
pub mod entry;
pub mod index;
pub mod log;
pub mod record;
pub mod segment;

pub use entry::{
    KeyRoute, SectionInfo, decode_chunk, section_messages, validate_section, validate_section_with,
};
pub use log::{
    CommitMark, DEFAULT_CACHE_BYTES, EnvelopeChunk, LogConfig, PartitionLog, PartitionWriter,
    RawChunk,
};
