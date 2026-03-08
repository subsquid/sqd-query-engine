use std::io::{self, Write};

const INITIAL_CAPACITY: usize = 256 * 1024; // 256 KB
const FLUSH_THRESHOLD: usize = 16 * 1024; // 16 KB

/// A buffered JSON array writer that streams blocks to an underlying writer.
/// Output format: `[block1,block2,...]`
pub struct JsonArrayWriter<W: Write> {
    write: W,
    buf: Vec<u8>,
    flush_threshold: usize,
    has_items: bool,
}

impl<W: Write> JsonArrayWriter<W> {
    pub fn new(write: W) -> Self {
        Self {
            write,
            buf: Vec::with_capacity(INITIAL_CAPACITY),
            flush_threshold: FLUSH_THRESHOLD,
            has_items: false,
        }
    }

    /// Begin a new item (block). Call this before writing block JSON bytes.
    pub fn begin_item(&mut self) -> io::Result<()> {
        if self.buf.len() > self.flush_threshold {
            self.flush_buf()?;
        }
        if self.has_items {
            self.buf.push(b',');
        } else {
            self.has_items = true;
            self.buf.push(b'[');
        }
        Ok(())
    }

    /// Write raw bytes to the current item.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Finish writing. Closes the JSON array and flushes.
    pub fn finish(mut self) -> io::Result<W> {
        if self.has_items {
            self.buf.push(b']');
        } else {
            self.buf.extend_from_slice(b"[]");
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
        let writer = JsonArrayWriter::new(Vec::new());
        let result = writer.finish().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "[]");
    }

    #[test]
    fn test_single_block() {
        let mut writer = JsonArrayWriter::new(Vec::new());
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":1}}");
        let result = writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(result).unwrap(),
            "[{\"header\":{\"number\":1}}]"
        );
    }

    #[test]
    fn test_multiple_blocks() {
        let mut writer = JsonArrayWriter::new(Vec::new());
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":1}}");
        writer.begin_item().unwrap();
        writer.write_bytes(b"{\"header\":{\"number\":2}}");
        let result = writer.finish().unwrap();
        assert_eq!(
            String::from_utf8(result).unwrap(),
            "[{\"header\":{\"number\":1}},{\"header\":{\"number\":2}}]"
        );
    }
}
