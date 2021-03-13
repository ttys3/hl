//
// indexing module
//
// index build phase pipeline scheme:
// -----------------------------------------------------------------------
//                            % | N                   ->   |
// | dir-scan -> | file-scan -> | N * segment-process -> % | save-index ->
//                            % | N                   ->   |
// -----------------------------------------------------------------------
//

// std imports
use std::cmp::{max, min};
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// third-party imports
use capnp::{message, serialize::read_message};
use closure::closure;
use crossbeam_channel as channel;
use crossbeam_channel::RecvError;
use crossbeam_utils::thread;
use generic_array::{typenum::U32, GenericArray};
use itertools::izip;
use serde::{Deserialize, Serialize};
use serde_json as json;
use sha2::{Digest, Sha256};

// local imports
use crate::error::*;
use crate::index_capnp as schema;
use crate::input::Input;
use crate::model::Record;
use crate::scanning::{BufFactory, ScannedSegment, Scanner, Segment, SegmentFactory};
use crate::types::Level;

// types
pub type Writer = dyn Write + Send + Sync;
pub type Reader = dyn Read + Send + Sync;

// ---

/// Allows log files indexing to enable message sorting.
pub struct Indexer {
    concurrency: usize,
    buffer_size: u32,
    dir: PathBuf,
}

impl Indexer {
    /// Returns a new Indexer with the given parameters.
    pub fn new(concurrency: usize, buffer_size: u32, dir: PathBuf) -> Self {
        Self {
            concurrency,
            buffer_size,
            dir,
        }
    }

    /// Builds index for the given file.
    ///
    /// Builds the index, saves it to disk and returns it.
    pub fn index(&self, source_path: PathBuf) -> Result<Index> {
        let hash = hex::encode(sha256(source_path.to_string_lossy().as_bytes()));
        let index_path = self.dir.join(PathBuf::from(hash));
        if Path::new(&index_path).exists() {
            let mut file = File::open(&index_path).chain_err(|| {
                format!(
                    "failed to open file '{}' for reading",
                    HILITE.paint(index_path.to_string_lossy())
                )
            })?;
            if let Ok(index) = Index::load(&mut file) {
                return Ok(index);
            }
        }

        self.build_index(&source_path, &index_path)
    }

    fn build_index(&self, source_path: &PathBuf, index_path: &PathBuf) -> Result<Index> {
        let mut input = Input::open(&source_path).chain_err(|| {
            format!(
                "failed to open file '{}' for reading",
                HILITE.paint(source_path.to_string_lossy()),
            )
        })?;
        let metadata = std::fs::metadata(&source_path).chain_err(|| {
            format!(
                "failed to get metadata of file '{}'",
                HILITE.paint(source_path.to_string_lossy()),
            )
        })?;
        let mut output = File::create(&index_path).chain_err(|| {
            format!(
                "failed to open file '{}' for writing",
                HILITE.paint(index_path.to_string_lossy())
            )
        })?;
        self.process_file(&source_path, &metadata, &mut input.stream, &mut output)
    }

    fn process_file(
        &self,
        path: &PathBuf,
        metadata: &std::fs::Metadata,
        input: &mut Reader,
        output: &mut Writer,
    ) -> Result<Index> {
        let n = self.concurrency;
        let sfi = Arc::new(SegmentFactory::new(self.buffer_size.try_into()?));
        let bfo = BufFactory::new(self.buffer_size.try_into()?);
        thread::scope(|scope| -> Result<Index> {
            // prepare receive/transmit channels for input data
            let (txi, rxi): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for output data
            let (txo, rxo): (Vec<_>, Vec<_>) = (0..n)
                .into_iter()
                .map(|_| channel::bounded::<(usize, Stat, Chronology)>(1))
                .unzip();
            // spawn reader thread
            let reader = scope.spawn(closure!(clone sfi, |_| -> Result<()> {
                let mut sn: usize = 0;
                let scanner = Scanner::new(sfi, "\n".to_string());
                for item in scanner.items(input) {
                    if let Err(_) = txi[sn % n].send(item?) {
                        break;
                    }
                    sn += 1;
                }
                Ok(())
            }));
            // spawn processing threads
            for (rxi, txo) in izip!(rxi, txo) {
                scope.spawn(closure!(ref bfo, ref sfi, |_| {
                    for segment in rxi.iter() {
                        let ((stat, chronology), segment) = match segment {
                            ScannedSegment::Complete(segment) => {
                                (self.process_segement(&segment), segment)
                            }
                            ScannedSegment::Incomplete(segment) => {
                                let mut stat = Stat::new();
                                stat.add_invalid();
                                ((stat, Chronology::default()), segment)
                            }
                        };
                        let size = segment.data().len();
                        sfi.recycle(segment);
                        if let Err(_) = txo.send((size, stat, chronology)) {
                            break;
                        };
                    }
                }));
            }
            // spawn writer thread
            let writer = scope.spawn(closure!(ref bfo, |_| -> Result<Index> {
                let bs = usize::try_from(self.buffer_size)?;
                let mut index = Index {
                    source: SourceFile {
                        size: metadata.len(),
                        path: path.to_string_lossy().into(),
                        modified: ts(metadata.modified()?),
                        stat: Stat::new(),
                        blocks: Vec::with_capacity(
                            (usize::try_from(metadata.len())? + bs - 1) / bs,
                        ),
                    },
                };

                let mut sn = 0;
                let mut offset: u64 = 0;
                loop {
                    match rxo[sn % n].recv() {
                        Ok((size, stat, chronology)) => {
                            index.source.stat.merge(&stat);
                            index.source.blocks.push(SourceBlock::new(
                                offset,
                                size.try_into()?,
                                stat,
                                chronology,
                            ));
                            offset += u64::try_from(size)?;
                        }
                        Err(RecvError) => {
                            break;
                        }
                    }
                    sn += 1;
                }
                index.save(output)?;
                Ok(index)
            }));
            // collect errors from reader and writer threads
            reader.join().unwrap()?;
            writer.join().unwrap()
        })
        .unwrap()
    }

    fn process_segement(&self, segment: &Segment) -> (Stat, Chronology) {
        let mut stat = Stat::new();
        let mut sorted = true;
        let mut prev_ts = None;
        let mut lines =
            Vec::<(Option<(i64, u32)>, u32, u32)>::with_capacity(segment.data().len() / 512);
        let mut offset = 0;
        for (i, data) in segment.data().split(|c| *c == b'\n').enumerate() {
            let data_len = data.len();
            let data = strip(data, b'\r');
            let mut ts = None;
            if data.len() != 0 {
                match json::from_slice::<Record>(data) {
                    Ok(rec) => {
                        let mut flags = 0;
                        match rec.level {
                            Some(Level::Debug) => {
                                flags |= schema::FLAG_LEVEL_DEBUG;
                            }
                            Some(Level::Info) => {
                                flags |= schema::FLAG_LEVEL_INFO;
                            }
                            Some(Level::Warning) => {
                                flags |= schema::FLAG_LEVEL_WARNING;
                            }
                            Some(Level::Error) => {
                                flags |= schema::FLAG_LEVEL_ERROR;
                            }
                            None => (),
                        }
                        ts = rec
                            .ts()
                            .and_then(|ts| ts.parse())
                            .and_then(|ts| Some((ts.timestamp(), ts.timestamp_subsec_nanos())));
                        if ts < prev_ts {
                            sorted = false;
                        }
                        prev_ts = ts;
                        stat.add_valid(ts, flags);
                    }
                    _ => {
                        stat.add_invalid();
                    }
                }
            }
            lines.push((ts.or(prev_ts), i as u32, offset));
            offset += data_len as u32;
        }
        let chronology = if sorted {
            Chronology::default()
        } else {
            stat.flags |= schema::FLAG_UNSORTED;
            lines.sort();

            let n = (lines.len() + 63) / 64;
            let mut bitmap = Vec::with_capacity(n);
            let mut offsets = Vec::with_capacity(n);
            let mut jumps = Vec::new();
            let mut prev = None;
            for chunk in lines.chunks(64) {
                let mut mask: u64 = 0;
                for (i, line) in chunk.iter().enumerate() {
                    if i == 0 {
                        offsets.push(OffsetPair {
                            bytes: line.2,
                            jumps: jumps.len().try_into().unwrap(),
                        });
                    }
                    if let Some(prev) = prev {
                        if line.1 != prev + 1 {
                            mask |= 1 << i;
                            jumps.push(line.2);
                        }
                    }
                    prev = Some(line.1);
                }
                bitmap.push(mask);
            }
            Chronology {
                bitmap,
                offsets,
                jumps,
            }
        };
        (stat, chronology)
    }
}

// ---

// Contains index information for a single source file.
#[derive(Debug)]
pub struct Index {
    source: SourceFile,
}

impl Index {
    /// Returns index information for the source file.
    pub fn source(&self) -> &SourceFile {
        &self.source
    }

    /// Loads the index.
    pub fn load(input: &mut Reader) -> Result<Index> {
        Header::load(input)?.validate()?;
        let message = read_message(input, message::ReaderOptions::new())?;
        let root: schema::root::Reader = message.get_root()?;
        let source = root.get_source()?;
        let modified = source.get_modified();
        Ok(Index {
            source: SourceFile {
                size: source.get_size(),
                path: source.get_path()?.into(),
                modified: (modified.get_sec(), modified.get_nsec()),
                stat: Self::load_stat(source.get_index()?),
                blocks: Self::load_blocks(source)?,
            },
        })
    }

    /// Saves the index.
    pub fn save(&self, output: &mut Writer) -> Result<()> {
        let header = Header::new();
        header.save(output)?;
        let mut message = capnp::message::Builder::new_default();
        let root: schema::root::Builder = message.init_root();
        let mut source = root.init_source();
        source.set_size(self.source.size);
        source.set_path(&self.source.path);
        let mut modified = source.reborrow().init_modified();
        modified.set_sec(self.source.modified.0);
        modified.set_nsec(self.source.modified.1);
        let mut index = source.reborrow().init_index();
        Self::save_stat(index.reborrow(), &self.source.stat);
        self.save_blocks(source)?;
        capnp::serialize::write_message(output, &message)?;
        Ok(())
    }

    fn load_stat(index: schema::index::Reader) -> Stat {
        let lines = index.get_lines();
        let ts = index.get_timestamps();
        let flags = index.get_flags();
        Stat {
            flags: flags,
            lines_valid: lines.get_valid(),
            lines_invalid: lines.get_invalid(),
            ts_min_max: if flags & schema::FLAG_HAS_TIMESTAMPS != 0 {
                Some((
                    (ts.get_min().get_sec(), ts.get_min().get_nsec()),
                    (ts.get_max().get_sec(), ts.get_max().get_nsec()),
                ))
            } else {
                None
            },
        }
    }

    fn save_stat(mut index: schema::index::Builder, stat: &Stat) {
        index.set_flags(stat.flags);
        let mut lines = index.reborrow().init_lines();
        lines.set_valid(stat.lines_valid);
        lines.set_invalid(stat.lines_invalid);
        if let Some((min, max)) = stat.ts_min_max {
            let mut timestamps = index.init_timestamps();
            let mut ts_min = timestamps.reborrow().init_min();
            ts_min.set_sec(min.0);
            ts_min.set_nsec(min.1);
            let mut ts_max = timestamps.init_max();
            ts_max.set_sec(max.0);
            ts_max.set_nsec(max.1);
        }
    }

    fn load_blocks(source: schema::source_file::Reader) -> Result<Vec<SourceBlock>> {
        let blocks = source.get_blocks()?;
        let mut result = Vec::with_capacity(blocks.len().try_into()?);
        for block in blocks.iter() {
            result.push(SourceBlock {
                offset: block.get_offset(),
                size: block.get_size(),
                stat: Self::load_stat(block.get_index()?),
                chronology: Chronology::default(),
            })
        }
        Ok(result)
    }

    fn save_blocks(&self, source: schema::source_file::Builder) -> Result<()> {
        let mut blocks = source.init_blocks(self.source.blocks.len().try_into()?);
        for (i, source_block) in self.source.blocks.iter().enumerate() {
            let mut block = blocks.reborrow().get(i.try_into()?);
            block.set_offset(source_block.offset);
            block.set_size(source_block.size);
            let mut index = block.init_index();
            Self::save_stat(index.reborrow(), &source_block.stat);
        }
        Ok(())
    }
}

// ---

/// SourceFile contains index data of scanned source log file.
#[derive(Debug)]
pub struct SourceFile {
    pub size: u64,
    pub path: String,
    pub modified: (i64, u32),
    pub stat: Stat,
    pub blocks: Vec<SourceBlock>,
}

// ---

/// SourceBlock contains index data of a block in a scanned source log file.
#[derive(Debug)]
pub struct SourceBlock {
    pub offset: u64,
    pub size: u32,
    pub stat: Stat,
    pub chronology: Chronology,
}

impl SourceBlock {
    /// Returns a new SourceBlock.
    pub fn new(offset: u64, size: u32, stat: Stat, chronology: Chronology) -> Self {
        Self {
            offset,
            size,
            stat,
            chronology,
        }
    }
}

// ---

/// Stat contains statistical information over a file or over a block.
#[derive(Debug)]
pub struct Stat {
    pub flags: u64,
    pub lines_valid: u64,
    pub lines_invalid: u64,
    pub ts_min_max: Option<((i64, u32), (i64, u32))>,
}

impl Stat {
    /// New returns a new Stat.
    pub fn new() -> Self {
        Self {
            flags: 0,
            lines_valid: 0,
            lines_invalid: 0,
            ts_min_max: None,
        }
    }

    /// Adds information about a single valid line.
    pub fn add_valid(&mut self, ts: Option<(i64, u32)>, flags: u64) {
        self.ts_min_max = min_max_opt(self.ts_min_max, ts.and_then(|ts| Some((ts, ts))));
        self.flags |= flags;
        self.lines_valid += 1;
        if self.ts_min_max.is_some() {
            self.flags |= schema::FLAG_HAS_TIMESTAMPS;
        }
    }

    /// Counts a single invalid line.
    pub fn add_invalid(&mut self) {
        self.lines_invalid += 1;
    }

    /// Merges with other Stat.
    pub fn merge(&mut self, other: &Self) {
        self.lines_valid += other.lines_valid;
        self.lines_invalid += other.lines_invalid;
        self.flags |= other.flags;
        self.ts_min_max = min_max_opt(self.ts_min_max, other.ts_min_max);
    }
}

// ---

/// Chronology contains information about ordering of log messages by timestamp in a SourceBlock.
#[derive(Debug)]
pub struct Chronology {
    pub bitmap: Vec<u64>,
    pub offsets: Vec<OffsetPair>,
    pub jumps: Vec<u32>,
}

impl Default for Chronology {
    fn default() -> Self {
        Self {
            bitmap: Vec::new(),
            offsets: Vec::new(),
            jumps: Vec::new(),
        }
    }
}

// ---

/// OffsetPair contains information offsets for a line in bytes in a SourceBlock and in a jump table.
#[derive(Debug)]
pub struct OffsetPair {
    pub bytes: u32,
    pub jumps: u32,
}

// ---

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Header {
    magic: u64,
    version: u64,
    size: u64,
    checksum: u64,
}

impl Header {
    fn new() -> Self {
        Self {
            magic: VALID_MAGIC,
            version: CURRENT_VERSION,
            size: 0,
            checksum: 0,
        }
    }

    fn load(reader: &mut Reader) -> Result<Self> {
        Ok(bincode::deserialize_from(reader)?)
    }

    fn is_valid(&self) -> bool {
        self.magic == VALID_MAGIC && self.version == CURRENT_VERSION
    }

    fn validate(&self) -> Result<()> {
        if self.is_valid() {
            Ok(())
        } else {
            Err("index file header is invalid".into())
        }
    }

    fn save(&self, writer: &mut Writer) -> Result<()> {
        Ok(bincode::serialize_into(writer, &self)?)
    }
}

// ---

fn min_max_opt<T: Ord>(v1: Option<(T, T)>, v2: Option<(T, T)>) -> Option<(T, T)> {
    match (v1, v2) {
        (Some(v1), Some(v2)) => Some((min(v1.0, v2.0), max(v1.1, v2.1))),
        (Some(v1), None) => Some(v1),
        (None, Some(v2)) => Some(v2),
        (None, None) => None,
    }
}

fn ts(ts: SystemTime) -> (i64, u32) {
    match ts.duration_since(UNIX_EPOCH) {
        Ok(ts) => (ts.as_secs() as i64, ts.subsec_nanos()),
        Err(_) => match UNIX_EPOCH.duration_since(ts) {
            Ok(ts) => (-(ts.as_secs() as i64), ts.subsec_nanos()),
            Err(_) => (0, 0),
        },
    }
}

fn sha256(bytes: &[u8]) -> GenericArray<u8, U32> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn strip<'a>(slice: &'a [u8], ch: u8) -> &'a [u8] {
    let n = slice.len();
    if n == 0 {
        slice
    } else if slice[n - 1] == ch {
        &slice[..n - 1]
    } else {
        slice
    }
}

const VALID_MAGIC: u64 = 0x5845444e492d4c48;
const CURRENT_VERSION: u64 = 1;
