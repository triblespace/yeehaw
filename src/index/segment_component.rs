use std::slice;

/// Enum describing each component of a tantivy segment.
///
/// Each component is stored in its own file,
/// using the pattern `segment_uuid`.`component_extension`,
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum SegmentComponent {
    /// Postings (or inverted list). Sorted lists of document ids, associated with terms
    Postings,
    /// Positions of terms in each document.
    Positions,
    /// Column-oriented random-access storage of fields.
    FastFields,
    /// Stores the sum  of the length (in terms) of each field for each document.
    /// Field norms are stored as a special u64 fast field.
    FieldNorms,
    /// Dictionary associating `Term`s to `TermInfo`s which is
    /// simply an address into the `postings` file and the `positions` file.
    Terms,
}

impl SegmentComponent {
    /// Iterates through the components.
    pub fn iterator() -> slice::Iter<'static, SegmentComponent> {
        static SEGMENT_COMPONENTS: [SegmentComponent; 5] = [
            SegmentComponent::Postings,
            SegmentComponent::Positions,
            SegmentComponent::FastFields,
            SegmentComponent::FieldNorms,
            SegmentComponent::Terms,
        ];
        SEGMENT_COMPONENTS.iter()
    }
}
