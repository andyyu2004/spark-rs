pub type SparkResult<T> = Result<T, SparkError>;

pub type SparkError = anyhow::Error;
