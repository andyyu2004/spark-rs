use std::ops::Range;

indexed_vec::newtype_index!(PartitionIndex);

pub type Partitions = Range<usize>;
