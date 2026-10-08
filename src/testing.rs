//! Test-only access to external chunks under `data/`.
//!
//! Data-backed tests are ignored by the portable suite. `make test-data` selects
//! them and sets `SQD_REQUIRE_CHUNKS=1`, making a missing input a failure.

use std::path::{Path, PathBuf};

/// The chunk directory for a dataset, or `None` when it is not checked out.
pub(crate) fn chunk_dir(dataset: &str) -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join(dataset)
        .join("chunk");

    if path.is_dir() {
        return Some(path);
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but {} is not checked out, so this test would \
         report green having read nothing",
        path.display()
    );

    None
}

/// Whether both bundled chunks are checked out. They arrive together, and the
/// tests that read one usually read the other.
pub(crate) fn chunks_present() -> bool {
    chunk_dir("evm").is_some() && chunk_dir("solana").is_some()
}

/// Counts the bytes each thread holds, so that a test can bound the memory a
/// call takes while other tests run beside it.
pub(crate) struct Counting;

thread_local! {
    static LIVE: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
    static PEAK: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
}

fn count(bytes: isize) {
    let now = LIVE.with(|live| {
        let now = live.get() + bytes;
        live.set(now);
        now
    });
    PEAK.with(|peak| peak.set(peak.get().max(now)));
}

// SAFETY: every call is passed through to `System`; the counters are
// const-initialized thread locals, which never allocate.
unsafe impl std::alloc::GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        let grown = unsafe { std::alloc::System.realloc(ptr, layout, size) };
        if !grown.is_null() {
            count(size as isize - layout.size() as isize);
        }
        grown
    }
}

/// `call`'s result, and the most bytes this thread held during it beyond what
/// it held before. Work `call` hands to other threads is not counted.
pub(crate) fn peak_bytes<R>(call: impl FnOnce() -> R) -> (R, usize) {
    let start = LIVE.with(|live| live.get());
    PEAK.with(|peak| peak.set(start));

    let result = call();

    let peak = PEAK.with(|peak| peak.get());
    (result, (peak - start).max(0) as usize)
}

/// One parquet file of `columns`, in row groups of `group_rows`, opened.
pub(crate) fn write_table(
    dir: &Path,
    fields: Vec<arrow::datatypes::Field>,
    columns: Vec<arrow::array::ArrayRef>,
    group_rows: usize,
) -> crate::scan::ParquetTable {
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
    let batch = arrow::array::RecordBatch::try_new(schema.clone(), columns).unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(group_rows)
        .build();

    let path = dir.join("table.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    crate::scan::ParquetTable::open(&path).unwrap()
}

/// One row group of 32 768 rows in 512 blocks from 1000: each row's block,
/// its index in the block, and 512 bytes of payload, 16 MiB in all.
pub(crate) fn wide_table(dir: &Path) -> crate::scan::ParquetTable {
    use arrow::array::{StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let rows = 32_768u64;
    write_table(
        dir,
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("payload", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(
                (0..rows).map(|r| 1000 + r / 64),
            )),
            Arc::new(UInt32Array::from_iter_values(
                (0..rows).map(|r| (r % 64) as u32),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|r| format!("{r:0>512}")),
            )),
        ],
        rows as usize,
    )
}

/// The bytes [`wide_table`]'s payload decodes to.
pub(crate) const WIDE_PAYLOAD: usize = 32_768 * 512;
