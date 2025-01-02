use std::sync::Arc;

pub type Blob = Arc<dyn AsRef<[u8]> + Sync + Send>;