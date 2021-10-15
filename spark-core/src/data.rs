pub trait Datum: Clone + Send + Sync + 'static {}

impl<T> Datum for T where T: Clone + Send + Sync + 'static
{
}
