pub trait CanAggregateAsNumber {
    fn aggregation_value(&self) -> Result<f64, String>;
}
