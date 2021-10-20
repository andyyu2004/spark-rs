pub trait SparkIterator<T> = Iterator<Item = T> + Send + Sync + 'static;
pub type SparkIteratorRef<T> = Box<dyn SparkIterator<T>>;
