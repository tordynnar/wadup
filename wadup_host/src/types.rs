use std::sync::Arc;

pub type Blob = Arc<dyn AsRef<[u8]> + Sync + Send>;

#[allow(dead_code)]
#[derive(Debug)]
pub enum DataValue {
    StringValue(String),
    Int64Value(i64),
    Float64Value(f64),
    NoneValue,
}
