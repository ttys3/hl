// std imports
use std::fs::File;
use std::io::{stdin, BufReader, Error, Read, Result, Seek};
use std::mem::size_of_val;
use std::path::PathBuf;
use std::sync::Arc;

// third-party imports
use ansi_term::Colour;
use flate2::bufread::GzDecoder;

// local imports
use crate::index::{Chronology, Index, Indexer, SourceBlock};
use crate::pool::SQPool;

// ---

pub type InputStream = Box<dyn Read + Send + Sync>;

pub type InputSeekStream = Box<dyn ReadSeek + Send + Sync>;

pub type BufPool = SQPool<Arc<Vec<u8>>>;

// ---

pub enum InputReference {
    Stdin,
    File(PathBuf),
}

impl Into<Result<Input>> for InputReference {
    fn into(self) -> Result<Input> {
        self.open()
    }
}

impl InputReference {
    pub fn open(&self) -> Result<Input> {
        match self {
            InputReference::Stdin => Ok(Input::new("<stdin>".into(), Box::new(stdin()))),
            InputReference::File(filename) => Input::open(&filename),
        }
    }

    pub fn index(&self, indexer: &Indexer) -> crate::error::Result<IndexedInput> {
        match self {
            InputReference::Stdin => panic!("indexing stdin is not implemented yet"),
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

    pub fn open(path: &PathBuf) -> Result<Self> {
        let name = format!("file '{}'", Colour::Yellow.paint(path.to_string_lossy()));
        let f = File::open(path)
            .map_err(|e| Error::new(e.kind(), format!("failed to open {}: {}", name, e)))?;
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
        Self {
            name,
            stream,
            index,
        }
    }

    pub fn open(path: &PathBuf, indexer: &Indexer) -> crate::error::Result<Self> {
        let name = format!("file '{}'", Colour::Yellow.paint(path.to_string_lossy()));
        let f = File::open(path)
            .map_err(|e| Error::new(e.kind(), format!("failed to open {}: {}", name, e)))?;
        let stream: InputSeekStream = match path.extension().map(|x| x.to_str()) {
            Some(Some("gz")) => panic!("sorting messages from gz files is not yet implemented"),
            _ => Box::new(f),
        };
        let index = indexer.index(path)?;

        Ok(Self::new(name, stream, index))
    }

    pub fn into_blocks(self) -> Blocks<IndexedInput, impl Iterator<Item = usize>> {
        Blocks::new(
            Arc::new(self),
            (0..self.index.source().blocks.len()).into_iter(),
        )
    }

    pub fn into_lines(&self) -> Lines<IndexedInput> {
        Lines::new(self.into_blocks())
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
        let mut indexes: Vec<_> = self.indexes.collect();
        indexes.sort_by_key(|&i| self.input.index.source().blocks[i].stat.ts_min_max);
        Blocks::new(self.input, indexes.into_iter())
    }
}

impl<II: Iterator<Item = usize>> Iterator for Blocks<IndexedInput, II> {
    type Item = Block<IndexedInput>;

    fn next(&mut self) -> Option<Self::Item> {
        self.indexes
            .next()
            .map(|i| Block::new(self.input.clone(), i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indexes.size_hint()
    }

    fn count(self) -> usize {
        self.indexes.count()
    }

    fn last(self) -> Option<Self::Item> {
        self.indexes
            .last()
            .map(|i| Block::new(self.input.clone(), i))
    }

    #[cfg(feature = "iter_advance_by")]
    fn advance_by(&mut self, n: usize) -> Result<(), usize> {
        self.indexes.advance_by(n)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.indexes
            .nth(n)
            .map(|i| Block::new(self.input.clone(), i))
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

    fn source_block(&self) -> &SourceBlock {
        self.input.index.source().blocks[self.index]
    }
}

// ---

pub struct BlockLines<I> {
    block: Block<I>,
    buf: Arc<Vec<u8>>,
    counter: usize,
}

impl BlockLines<IndexedInput> {
    pub fn new(block: Block<IndexedInput>) -> Result<Self> {
        let mut buf = if let Some(pool) = block.buf_pool {
            pool.checkout()
        } else {
            Arc::new(Vec::new())
        };
        let source_block = block.source_block();
        buf.resize(source_block.size, 0);
        let stream = block.input.stream;
        stream.seek(source_block.offset)?;
        stream.read(&mut buf)?;
        Ok(Self {
            block,
            buf,
            counter: 0,
        })
    }

    fn source_block(&self) -> SourceBlock {
        self.block.source_block()
    }
}

impl Iterator for BlockLines<IndexedInput> {
    type Item = BlockLine<IndexedInput>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.source_block();
        if self.counter >= block.lines_valid {
            return None;
        }
        let bitmap = &block.chronology.bitmap;
        if bitmap[self.counter / size_of_val(&bitmap[0])]
            & (1 << self.counter % size_of_val(&bitmap[0]))
            == 0
        {
            // read
        } else {
            // jump
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indexes.size_hint()
    }

    fn count(self) -> usize {
        self.indexes.count()
    }

    fn last(self) -> Option<Self::Item> {
        self.indexes
            .last()
            .map(|i| Block::new(self.input.clone(), i))
    }

    fn advance_by(&mut self, n: usize) -> Result<(), usize> {
        self.indexes.advance_by(n)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.indexes
            .nth(n)
            .map(|i| Block::new(self.input.clone(), i))
    }
}

// ---

pub struct BlockLine<I> {
    buf: Arc<Vec<u8>>,
    begin: usize,
    end: usize,
}

impl BlockLine<IndexedInput> {
    pub fn new(buf: Arc<Vec<u8>>, begin: usize, end: usize) -> Self {
        Self { buf, begin, end }
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
    I: Iterator<Item = Result<Input>>,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
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
            let n = input.stream.read(buf).map_err(|e| {
                Error::new(e.kind(), format!("failed to read {}: {}", input.name, e))
            })?;
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
