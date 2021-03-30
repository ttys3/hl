// std imports
use std::fs::File;
use std::io::{stdin, BufReader, Error, Read, Result, Seek};
use std::path::PathBuf;

// third-party imports
use ansi_term::Colour;
use flate2::bufread::GzDecoder;

// local imports
use crate::index::{Index, Indexer};
use crate::scanning::{SegmentBufFactory, SegmentBuf};

// ---

pub type InputStream = Box<dyn Read + Send + Sync>;

pub type InputSeekStream = Box<dyn ReadSeek + Send + Sync>;

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

    pub fn into_blocks(self) -> Blocks<IndexedInput, impl Iterator<Item=usize>> {
        Blocks::new(self, (0..self.index.source().blocks.len()).into_iter())
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

impl<II> Blocks<IndexedInput, II> {
    pub fn new(input: Arc<IndexedInput>, indexes: II) -> Self {
        Self { input, indexes }
    }

    pub fn sorted(self) -> Blocks<IndexedInput, impl Iterator<Item=usize>> {
        let mut indexes = self.indexes.collect();
        indexes.sort_by_key(|i| self.input.source().blocks[i].ts_min_max);
        Self{ self.input, indexes.into_iter())
    }
}

impl<II> Iterator for Blocks<IndexedInput, II> {
    type Item = Block<IndexedInput>;

    fn next(&mut self) -> Option<Self::Item>{ 
        self.indexes.next().map(|i|Block::new(self.input.clone(), i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indexes.size_hint()
    }

    fn count(self) -> usize {
        self.indexes.count()
    }

    fn last(self) -> Option<Self::Item> { 
        self.indexes.last().map(|i|Block::new(self.input.clone(), i))
    }

    fn advance_by(&mut self, n: usize) -> Result<(), usize> { 
        self.indexes.advance_by(n)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> { 
        self.indexes.nth(n).map(|i|Block::new(self.input.clone(), i))
    }
}

// ---

pub struct Block<I> {
    input: Arc<I>,
    index: usize,
}

impl Block<IndexedInput> {
    pub fn new(input: IndexedInput, index: usize) -> Self {
        Self{input, index}
    }

    pub fn into_lines(self, sf: Arc<SegmentBufFactory>) -> Lines<Block<IndexedInput>> {
        Lines::new(self, sf)
    }
}

// ---

pub struct Lines<B> {
    block: B,
    sf: Arc<SegmentBufFactory>,
    buf: Arc<SegmentBuf>,
}

impl Lines<Block<IndexedInput>> {
    pub fn new(block: Block<IndexedInput>, sf: Arc<SegmentBufFactory>) -> Self {
        let buf = sf.new_segment();
        if buf.data.capacity().len() < block.size {
            sf.recycle(buf);
        }
        Self{block, sf}
    }
}

impl<II> Iterator for Lines<Block<IndexedInput>, II> {
    type Item = Line<Block<IndexedInput>>;

    fn next(&mut self) -> Option<Self::Item>{ 
        self.indexes.next().map(|i|Block::new(self.input.clone(), i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.indexes.size_hint()
    }

    fn count(self) -> usize {
        self.indexes.count()
    }

    fn last(self) -> Option<Self::Item> { 
        self.indexes.last().map(|i|Block::new(self.input.clone(), i))
    }

    fn advance_by(&mut self, n: usize) -> Result<(), usize> { 
        self.indexes.advance_by(n)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> { 
        self.indexes.nth(n).map(|i|Block::new(self.input.clone(), i))
    }
}

// ---

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
