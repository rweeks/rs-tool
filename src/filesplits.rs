use std::io::{self, prelude::*, SeekFrom};
use std::ops::Range;

/// Splits the given `src` on newlines roughly in chunks of `split_size` bytes.
pub fn get_splits<R: BufRead + Seek>(mut src: R, split_size: u64) -> io::Result<Vec<Range<u64>>> {
    let mut splits: Vec<Range<u64>> = Vec::new();
    let mut buf: String = String::new();
    let end_pos = src.seek(SeekFrom::End(0))?;
    src.seek(SeekFrom::Start(0))?;
    loop {
        let split_start_pos = src.stream_position()?;
        let split_end_pos = src.seek(SeekFrom::Current(split_size as i64))?;
        let split_end_pos = u64::min(split_end_pos, end_pos);
        if split_end_pos == end_pos {
            splits.push(split_start_pos..end_pos);
            break;
        } else {
            src.read_line(&mut buf)?;
            buf.clear();
            splits.push(split_start_pos..src.stream_position()?);
        }
    }
    Ok(splits)
}