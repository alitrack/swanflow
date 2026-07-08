use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

/// Generates a stable i64 hash from a string.
/// Used for deterministic entity ID generation.
pub fn hash_string_to_i64(s: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() as i64
}

/// Create a one-shot stream that emits a single RecordBatch then finishes.
pub fn one_shot_stream(
    schema: SchemaRef,
    batch: RecordBatch,
) -> SendableRecordBatchStream {
    struct OneShotStream {
        schema: SchemaRef,
        batch: Option<RecordBatch>,
    }
    impl RecordBatchStream for OneShotStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }
    impl Stream for OneShotStream {
        type Item = DFResult<RecordBatch>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batch.take().map(Ok))
        }
    }
    Box::pin(OneShotStream { schema, batch: Some(batch) })
}
