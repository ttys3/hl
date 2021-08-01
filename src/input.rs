// std imports
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, stdin, BufReader, Read, Seek, SeekFrom};
use std::mem::size_of_val;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

// third-party imports
use ansi_term::Colour;
use closure::closure;
use flate2::bufread::GzDecoder;

// local imports
use crate::error::Result;
use crate::index::{Index, Indexer, SourceBlock};
use crate::iox::ReadFill;
use crate::pool::SQPool;
use crate::replay::{LruCache, ReplayBufCreator, ReplayBufReader, RewindingReader};
use crate::tee::TeeReader;

// ---

pub type InputStream = Box<dyn Read + Send + Sync>;
pub type InputStreamFactory = Box<dyn FnOnce() -> Box<dyn Read> + Send + Sync>;

pub type InputSeekStream = Box<Mutex<dyn ReadSeek + Send + Sync>>;

pub type BufPool = SQPool<Vec<u8>>;

// ---

pub enum InputReference {
    Stdin,
    File(PathBuf),
}

impl Into<io::Result<Input>> for InputReference {
    fn into(self) -> io::Result<Input> {
        self.open()
    }
}

impl InputReference {
    pub fn open(&self) -> io::Result<Input> {
        match self {
            InputReference::Stdin => Ok(Input::new("<stdin>".into(), Box::new(stdin()))),
            InputReference::File(filename) => Input::open(&filename),
        }
    }

    pub fn index(&self, indexer: &Indexer) -> Result<IndexedInput> {
        match self {
            InputReference::Stdin => {
                let mut tee = TeeReader::new(stdin(), ReplayBufCreator::new());
                let index = indexer.index_from_stream(&mut tee)?;
                let buf = tee.into_writer().result()?;
                Ok(IndexedInput::new(
                    "<stdin>".into(),
                    Box::new(Mutex::new(ReplayBufReader::new(buf))),
                    index,
                ))
            }
            InputReference::File(filename) => IndexedInput::open(&filename, indexer),
        }
    }
}

// ---

pub struct Input {
    pub name: String,
    pub stream: InputStream,
}

impl Input {
    pub fn new(name: String, stream: InputStream) -> Self {
        Self { name, stream }
    }

    pub fn open(path: &PathBuf) -> io::Result<Self> {
        let name = format!("file '{}'", Colour::Yellow.paint(path.to_string_lossy()));
        let f = File::open(path).map_err(|e| io::Error::new(e.kind(), format!("failed to open {}: {}", name, e)))?;
        let stream: InputStream = match path.extension().map(|x| x.to_str()) {
            Some(Some("gz")) => Box::new(GzDecoder::new(BufReader::new(f))),
            _ => Box::new(f),
        };
        Ok(Self::new(name, stream))
    }
}

// ---

pub struct IndexedInput {
    pub name: String,
    pub stream: InputSeekStream,
    pub index: Index,
}

impl IndexedInput {
    pub fn new(name: String, stream: InputSeekStream, index: Index) -> Self {
        Self { name, stream, index }
    }

    pub fn open(path: &PathBuf, indexer: &Indexer) -> Result<Self> {
        let name = format!("file '{}'", Colour::Yellow.paint(path.to_string_lossy()));
        let f = closure!(
            clone path, clone name, || File::open(&path).map_err(|e| io::Error::new(e.kind(), format!("failed to open {}: {}", &name, e)))
        );
        let stream: InputSeekStream = match path.extension().map(|x| x.to_str()) {
            Some(Some("gz")) => Box::new(Mutex::new(
                RewindingReader::build(move || Ok(GzDecoder::new(BufReader::new(f()?))))
                    .cache(LruCache::new(8))
                    .result()?,
            )),
            _ => Box::new(Mutex::new(f()?)),
        };
        let index = indexer.index(&path)?;

        Ok(Self::new(name, stream, index))
    }

    pub fn into_blocks(self) -> Blocks<IndexedInput, impl Iterator<Item = usize>> {
        let n = self.index.source().blocks.len();
        Blocks::new(Arc::new(self), (0..n).into_iter())
    }
}

// ---

pub struct Blocks<I, II> {
    input: Arc<I>,
    indexes: II,
}

impl<II: Iterator<Item = usize>> Blocks<IndexedInput, II> {
    pub fn new(input: Arc<IndexedInput>, indexes: II) -> Self {
        Self { input, indexes }
    }

    pub fn sorted(self) -> Blocks<IndexedInput, impl Iterator<Item = usize>> {
        let (input, indexes) = (self.input, self.indexes);
        let mut indexes: Vec<_> = indexes.collect();
        indexes.sort_by_key(|&i| input.index.source().blocks[i].stat.ts_min_max);
        Blocks::new(input, indexes.into_iter())
    }
}

impl<II: Iterator<Item = usize>> Iterator for Blocks<IndexedInput, II> {
    type Item = Block<IndexedInput>;

    fn next(&mut self) -> Option<Self::Item> {
        self.indexes.next().map(|i| Block::new(self.input.clone(), i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indexes.size_hint()
    }

    fn count(self) -> usize {
        self.indexes.count()
    }

    // fn last(self) -> Option<Self::Item> {
    //     self.indexes
    //         .last()
    //         .map(|i| Block::new(self.input.clone(), i))
    // }

    #[cfg(feature = "iter_advance_by")]
    fn advance_by(&mut self, n: usize) -> Result<(), usize> {
        self.indexes.advance_by(n)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.indexes.nth(n).map(|i| Block::new(self.input.clone(), i))
    }
}

// ---

pub struct Block<I> {
    input: Arc<I>,
    index: usize,
    buf_pool: Option<Arc<BufPool>>,
}

impl Block<IndexedInput> {
    pub fn new(input: Arc<IndexedInput>, index: usize) -> Self {
        Self {
            input,
            index,
            buf_pool: None,
        }
    }

    pub fn with_buf_pool(self, buf_pool: Arc<BufPool>) -> Self {
        Self {
            input: self.input,
            index: self.index,
            buf_pool: Some(buf_pool),
        }
    }

    pub fn into_lines(self) -> Result<BlockLines<IndexedInput>> {
        BlockLines::new(self)
    }

    pub fn offset(&self) -> u64 {
        self.source_block().offset
    }

    pub fn size(&self) -> u32 {
        self.source_block().size
    }

    pub fn source_block(&self) -> &SourceBlock {
        &self.input.index.source().blocks[self.index]
    }

    pub fn lines_valid(&self) -> u64 {
        self.source_block().stat.lines_valid
    }
}

// ---

pub struct BlockLines<I> {
    block: Block<I>,
    buf: Arc<Vec<u8>>,
    total: usize,
    current: usize,
    byte: usize,
    jump: usize,
}

impl BlockLines<IndexedInput> {
    pub fn new(mut block: Block<IndexedInput>) -> Result<Self> {
        let (buf, total) = {
            let block = &mut block;
            let mut buf = if let Some(pool) = &block.buf_pool {
                pool.checkout() // TODO: implement checkin
            } else {
                Vec::new()
            };
            let source_block = block.source_block();
            buf.resize(source_block.size.try_into()?, 0);
            let mut stream = block.input.stream.lock().unwrap();
            stream.seek(SeekFrom::Start(source_block.offset))?;
            stream.read_fill(&mut buf)?;
            let total = (source_block.stat.lines_valid + source_block.stat.lines_invalid).try_into()?;
            (buf, total)
        };
        Ok(Self {
            block,
            buf: Arc::new(buf), // TODO: optimize allocations
            total,
            current: 0,
            byte: 0,
            jump: 0,
        })
    }
}

impl Iterator for BlockLines<IndexedInput> {
    type Item = BlockLine;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.total {
            return None;
        }
        let block = self.block.source_block();
        let bitmap = &block.chronology.bitmap;

        if bitmap.len() != 0 {
            let k = 8 * size_of_val(&bitmap[0]);
            let n = self.current / k;
            let m = self.current % k;
            if m == 0 {
                let offsets = block.chronology.offsets[n];
                self.byte = offsets.bytes as usize;
                self.jump = offsets.jumps as usize;
            }
            if bitmap[n] & (1 << m) != 0 {
                self.byte = block.chronology.jumps[self.jump] as usize;
                self.jump += 1;
            }
        }
        let s = &self.buf[self.byte..];
        let l = s.iter().position(|&x| x == b'\n').map_or(s.len(), |i| i + 1);
        let offset = self.byte;
        self.byte += l;
        self.current += 1;

        Some(BlockLine::new(self.buf.clone(), offset..offset + l))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.total - self.current;
        (count, Some(count))
    }

    fn count(self) -> usize {
        self.size_hint().0
    }
}

// ---

pub struct BlockLine {
    buf: Arc<Vec<u8>>,
    range: Range<usize>,
}

impl BlockLine {
    pub fn new(buf: Arc<Vec<u8>>, range: Range<usize>) -> Self {
        Self { buf, range }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.buf[self.range.clone()]
    }

    pub fn offset(&self) -> usize {
        self.range.start
    }

    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }
}

// ---

pub struct ConcatReader<I> {
    iter: I,
    item: Option<Input>,
}

impl<I> ConcatReader<I> {
    pub fn new(iter: I) -> Self {
        Self { iter, item: None }
    }
}

impl<I> Read for ConcatReader<I>
where
    I: Iterator<Item = io::Result<Input>>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.item.is_none() {
                match self.iter.next() {
                    None => {
                        return Ok(0);
                    }
                    Some(result) => {
                        self.item = Some(result?);
                    }
                };
            }

            let input = self.item.as_mut().unwrap();
            let n = input
                .stream
                .read(buf)
                .map_err(|e| io::Error::new(e.kind(), format!("failed to read {}: {}", input.name, e)))?;
            if n != 0 {
                return Ok(n);
            }
            self.item = None;
        }
    }
}

// ---

pub trait ReadSeek: Read + Seek {}

impl<T: Read + Seek> ReadSeek for T {}
