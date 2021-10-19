pub type SparkIterator<T> = Box<dyn Iterator<Item = T> + Send + Sync>;
