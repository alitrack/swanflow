//! SwanFlow example plugin — sort a table by its first column, ascending.
//!
//! This is the reference implementation of ABI `swanflow-plugin-1`. Copy it as a
//! starting point: the host only ever needs these three exports plus `memory`.
//!
//! ```text
//! alloc(size: i32) -> ptr
//! dealloc(ptr: i32, size: i32)
//! sort_table(ptr: i32, len: i32) -> ptr
//! ```
//!
//! `sort_table` receives Arrow IPC stream bytes and returns a pointer whose first
//! four bytes hold the output length (little-endian `i32`), followed by the
//! output Arrow IPC stream. Returning 0 signals failure; the host reports it as
//! an error rather than silently producing an empty table.

use std::alloc::{alloc as raw_alloc, dealloc as raw_dealloc, Layout};

use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// wasm32 is 32-bit, so 8-byte alignment keeps `f64`/`i64` columns happy.
const ALIGN: usize = 8;

#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    if size <= 0 {
        return 0;
    }
    let layout = Layout::from_size_align(size as usize, ALIGN).expect("valid layout");
    unsafe { raw_alloc(layout) as i32 }
}

#[no_mangle]
pub extern "C" fn dealloc(ptr: i32, size: i32) {
    if ptr == 0 || size <= 0 {
        return;
    }
    let layout = Layout::from_size_align(size as usize, ALIGN).expect("valid layout");
    unsafe { raw_dealloc(ptr as *mut u8, layout) }
}

/// Hand bytes back to the host: 4-byte LE length prefix, then the payload.
fn respond(payload: &[u8]) -> i32 {
    let total = payload.len() + 4;
    let out = alloc(total as i32);
    if out == 0 {
        return 0;
    }
    let mem = unsafe { std::slice::from_raw_parts_mut(out as *mut u8, total) };
    mem[..4].copy_from_slice(&(payload.len() as i32).to_le_bytes());
    mem[4..].copy_from_slice(payload);
    out
}

fn sort_by_first_column(input: &[u8]) -> Option<Vec<u8>> {
    let reader = StreamReader::try_new(input, None).ok()?;
    let batch: RecordBatch = reader.into_iter().next()?.ok()?;
    if batch.num_columns() == 0 {
        return None;
    }
    let indices = lexsort_to_indices(
        &[SortColumn {
            values: batch.column(0).clone(),
            options: Some(SortOptions::default()),
        }],
        None,
    )
    .ok()?;
    let sorted = arrow::compute::take_record_batch(&batch, &indices).ok()?;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &sorted.schema()).ok()?;
        writer.write(&sorted).ok()?;
        writer.finish().ok()?;
    }
    Some(buf)
}

#[no_mangle]
pub extern "C" fn sort_table(ptr: i32, len: i32) -> i32 {
    if ptr == 0 || len <= 0 {
        return 0;
    }
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    match sort_by_first_column(input) {
        Some(out) => respond(&out),
        None => 0,
    }
}
