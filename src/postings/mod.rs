//! Postings module (also called inverted index)

mod block_search;

pub(crate) use self::block_search::branchless_binary_search;

mod block_segment_postings;
pub(crate) mod compression;
mod indexing_context;
mod json_postings_writer;
mod loaded_postings;
mod per_field_postings_writer;
mod postings;
mod postings_writer;
mod recorder;
mod segment_postings;
mod serializer;
mod skip;
mod term_info;

pub(crate) use loaded_postings::LoadedPostings;
pub(crate) use stacker::compute_table_memory_size;

pub use self::block_segment_postings::BlockSegmentPostings;
pub(crate) use self::indexing_context::IndexingContext;
pub(crate) use self::per_field_postings_writer::PerFieldPostingsWriter;
pub use self::postings::Postings;
pub(crate) use self::postings_writer::{serialize_postings, IndexingPosition, PostingsWriter};
pub use self::segment_postings::SegmentPostings;
pub use self::serializer::{FieldSerializer, InvertedIndexSerializer};
pub(crate) use self::skip::{BlockInfo, SkipReader};
pub use self::term_info::TermInfo;

#[expect(clippy::enum_variant_names)]
#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub(crate) enum FreqReadingOption {
    NoFreq,
    SkipFreq,
    ReadFreq,
}

#[cfg(test)]
mod tests {}
