use std::io::{self, Write};

/// Initial capacity for the internal buffer (tuned for typical block JSON sizes).
const INITIAL_CAPACITY: usize = 256 * 1024; // 256 KB
/// Flush the buffer to the underlying writer when it exceeds this size.
const FLUSH_THRESHOLD: usize = 16 * 1024; // 16 KB

/// A buffered newline-delimited JSON (NDJSON) writer that streams blocks to an
/// underlying writer. Output format: `block1\nblock2\n…\nblockN\n`.
///
/// This is the engine's sole JSON output format — byte-compatible with the
/// legacy `sqd_query::JsonLinesWriter`: one block object per line, a newline
/// after every block (trailing newline included), and no array brackets. An
/// empty result produces empty output (zero bytes). Unlike a JSON array, NDJSON
/// stays valid under byte-for-byte concatenation of per-chunk blobs.
///
/// Intermediate flushes only happen at item boundaries (after a complete block),
/// so the underlying writer never observes a half-written block.
pub struct JsonLinesWriter<W: Write> {
    write: W,
    buf: Vec<u8>,
    flush_threshold: usize,
    has_items: bool,
}

impl<W: Write> JsonLinesWriter<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            buf: Vec::with_capacity(INITIAL_CAPACITY),
            flush_threshold: FLUSH_THRESHOLD,
            has_items: false,
        }
    }

    /// Begin a new item (block). Call this before writing block JSON bytes.
    /// Emits the `\n` separating this block from the previous one (the trailing
    /// newline after the final block is added by `finish`). Flushes the buffer at
    /// the item boundary if it exceeds the threshold.
    pub fn begin_item(&mut self) -> io::Result<()> {
        // Flush at item boundary — buffer contains complete blocks only.
        if self.buf.len() > self.flush_threshold {
            self.flush_buf()?;
        }
        if self.has_items {
            self.buf.push(b'\n');
        } else {
            self.has_items = true;
        }
        Ok(())
    }

    /// Write raw bytes to the current item.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Write a single byte to the current item.
    pub fn write_byte(&mut self, byte: u8) {
        self.buf.push(byte);
    }

    /// Get a mutable reference to the internal buffer for direct writes.
    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    /// Finish writing. Appends the trailing newline (when any block was written)
    /// and flushes. Empty result → empty output.
    pub fn finish(mut self) -> io::Result<W> {
        if self.has_items {
            self.buf.push(b'\n');
        }
        self.flush_buf()?;
        Ok(self.write)
    }

    /// Get the current buffer size (for testing/debugging).
    pub fn buffer_len(&self) -> usize {
        self.buf.len()
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.write.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_output() {
        let writer = JsonLinesWriter::new(Vec::new());
        let result = writer.finish().unwrap();
        // Empty result → empty output, NOT "[]".
        assert_eq!(String::from_utf8(result).unwrap(), "");
    }

    #[test]
    fn test_single_block() {
        let mut writer = JsonLinesWriter::new(Vec::new());
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":1}}");
        let result = writer.finish().unwrap();
        // Trailing newline after the (only) block.
        assert_eq!(
            String::from_utf8(result).unwrap(),
            "{\"header\":{\"number\":1}}\n"
        );
    }

    #[test]
    fn test_multiple_blocks() {
        let mut writer = JsonLinesWriter::new(Vec::new());
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":1}}");
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":2}}");
        let result = writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(result).unwrap(),
            "{\"header\":{\"number\":1}}\n{\"header\":{\"number\":2}}\n"
        );
    }

    #[test]
    fn test_flush_threshold_keeps_blocks_intact() {
        // A block larger than the flush threshold must still frame correctly:
        // the flush fires at the next begin_item, never mid-block.
        let mut writer = JsonLinesWriter::new(Vec::new());
        let big = vec![b'x'; FLUSH_THRESHOLD + 1];
        writer.begin_item().unwrap();
        writer.write_byte(b'{');
        writer.write_bytes(&big);
        writer.write_byte(b'}');
        writer.begin_item().unwrap(); // flushes the first (complete) block
        writer.write_bytes(b"{}");
        let result = writer.finish().unwrap();
        let s = String::from_utf8(result).unwrap();
        // block1, block2, then "" from the trailing newline.
        let lines: Vec<&str> = s.split('\n').collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].len(), big.len() + 2); // '{' + big + '}'
        assert_eq!(lines[1], "{}");
        assert_eq!(lines[2], "");
    }
}
