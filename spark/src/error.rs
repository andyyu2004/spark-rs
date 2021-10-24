pub type SparkResult<T> = Result<T, SparkError>;

pub type SparkError = eyre::Report;
