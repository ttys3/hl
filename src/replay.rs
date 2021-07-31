// std imports
use std::{
    cmp::min,
    collections::{btree_map::Entry as BTreeEntry, hash_map::Entry, BTreeMap, HashMap},
    convert::{TryFrom, TryInto},
    io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
    mem::replace,
    num::NonZeroUsize,
    time::Instant,
};

// third-party imports
use snap::{read::FrameDecoder, write::FrameEncoder};

// ---

const DEFAULT_SEGMENT_SIZE: Option<NonZeroUsize> = NonZeroUsize::new(256 * 1024);

// ---

type Buf = Vec<u8>;

// ---

pub trait Cache {
    fn cache<F: FnOnce() -> Result<Buf>>(&mut self, index: usize, f: F) -> Result<&[u8]>;
}

// ---

pub struct ReplayBuf {
    segment_size: NonZeroUsize,
    segments: Vec<CompressedBuf>,
    size: usize,
}

impl ReplayBuf {
    fn new(segment_size: NonZeroUsize) -> Self {
        Self {
            segment_size,
            segments: Vec::new(),
            size: 0,
        }
    }
}

impl TryFrom<ReplayBufCreator> for ReplayBuf {
    type Error = Error;

    fn try_from(builder: ReplayBufCreator) -> Result<Self> {
        builder.result()
    }
}

// ---

pub struct ReplayBufCreator {
    buf: ReplayBuf,
    scratch: ReusableBuf,
}

impl ReplayBufCreator {
    pub fn new() -> Self {
        Self::build().result()
    }

    pub fn build() -> ReplayBufCreatorBuilder {
        ReplayBufCreatorBuilder {
            segment_size: DEFAULT_SEGMENT_SIZE.unwrap(),
        }
    }

    pub fn result(mut self) -> Result<ReplayBuf> {
        self.flush()?;
        Ok(self.buf)
    }

    fn prepare(&mut self) -> Result<()> {
        if self.buf.size % self.buf.segment_size != 0 {
            assert_eq!(self.scratch.len(), 0);
            self.buf.segments.pop().unwrap().decode(self.scratch.backstage())?;
        }
        Ok(())
    }
}

impl Write for ReplayBufCreator {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut k: usize = 0;
        if buf.len() != 0 {
            self.prepare()?;
        }
        while k < buf.len() {
            let buf = &buf[k..];
            let target = self.scratch.backstage();
            let n = min(buf.len(), target.len());
            target[..n].copy_from_slice(&buf[..n]);
            self.scratch.extend(n);
            k += n;
            if self.scratch.full() {
                self.flush()?;
            }
        }
        Ok(k)
    }

    fn flush(&mut self) -> Result<()> {
        if self.scratch.len() != 0 {
            let buf = self.scratch.clear();
            self.buf.segments.push(CompressedBuf::try_from(buf)?);
            self.buf.size += buf.len();
        }
        Ok(())
    }
}

impl From<ReplayBufCreatorBuilder> for ReplayBufCreator {
    fn from(builder: ReplayBufCreatorBuilder) -> Self {
        builder.result()
    }
}

// ---

pub struct ReplayBufCreatorBuilder {
    segment_size: NonZeroUsize,
}

impl ReplayBufCreatorBuilder {
    #[allow(dead_code)]
    pub fn with_segment_size(mut self, segment_size: NonZeroUsize) -> Self {
        self.segment_size = segment_size;
        self
    }

    pub fn result(self) -> ReplayBufCreator {
        ReplayBufCreator {
            buf: ReplayBuf::new(self.segment_size),
            scratch: ReusableBuf::new(self.segment_size.get()),
        }
    }
}

// ---

pub struct ReplayBufReader<C> {
    buf: ReplayBuf,
    cache: C,
    position: usize,
}

impl ReplayBufReader<MinimalCache> {
    pub fn new(buf: ReplayBuf) -> Self {
        Self::build(buf).result()
    }

    pub fn build(buf: ReplayBuf) -> ReplayBufReaderBuilder<MinimalCache> {
        ReplayBufReaderBuilder {
            buf,
            cache: MinimalCache::default(),
            position: 0,
        }
    }
}

impl<C: Cache> ReplayBufReader<C> {
    #[inline(always)]
    fn segment_size(&self) -> NonZeroUsize {
        self.buf.segment_size
    }

    fn segment(&mut self, index: usize) -> Result<&[u8]> {
        if index >= self.buf.segments.len() {
            panic!("logic error")
        }
        let ss = usize::from(self.segment_size());
        let data = &mut self.buf.segments;
        self.cache.cache(index, || {
            let mut buf = vec![0; ss];
            data[index].decode(&mut buf)?;
            Ok(buf)
        })
    }

    fn from_start(&self, offset: u64) -> Option<usize> {
        usize::try_from(offset).ok().filter(|&v| v <= self.buf.size)
    }

    fn from_current(&self, offset: i64) -> Option<usize> {
        usize::try_from(i64::try_from(self.position).ok()?.checked_add(offset)?).ok()
    }

    fn from_end(&mut self, offset: i64) -> Option<usize> {
        usize::try_from(i64::try_from(self.buf.size).ok()?.checked_sub(offset)?).ok()
    }
}

impl<C: Cache> Read for ReplayBufReader<C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut i = 0;
        let ss = usize::from(self.segment_size());
        loop {
            let segment = self.position / self.segment_size();
            let offset = self.position % self.segment_size();
            let data = self.segment(segment)?;
            let k = data.len();
            let n = min(buf.len() - i, data.len() - offset);
            buf[i..i + n].copy_from_slice(&data[offset..offset + n]);
            i += n;
            self.position += n;
            if k != ss || i == buf.len() {
                return Ok(i);
            }
        }
    }
}

impl<C: Cache> Seek for ReplayBufReader<C> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let pos = match pos {
            SeekFrom::Start(pos) => self.from_start(pos),
            SeekFrom::Current(pos) => self.from_current(pos),
            SeekFrom::End(pos) => self.from_end(pos),
        };
        let pos = pos.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "position out of range"))?;
        let pos = min(pos, self.buf.size);
        self.position = pos;
        u64::try_from(pos).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
    }
}

impl<C: Cache> From<ReplayBufReaderBuilder<C>> for ReplayBufReader<C> {
    fn from(builder: ReplayBufReaderBuilder<C>) -> Self {
        builder.result()
    }
}

// ---

pub struct ReplayBufReaderBuilder<C> {
    buf: ReplayBuf,
    cache: C,
    position: usize,
}

impl<C: Cache> ReplayBufReaderBuilder<C> {
    pub fn with_cache<C2: Cache>(self, cache: C2) -> ReplayBufReaderBuilder<C2> {
        ReplayBufReaderBuilder {
            buf: self.buf,
            cache,
            position: self.position,
        }
    }

    pub fn with_position(mut self, position: usize) -> Self {
        self.position = position;
        self
    }

    pub fn result(self) -> ReplayBufReader<C> {
        ReplayBufReader {
            buf: self.buf,
            cache: self.cache,
            position: self.position,
        }
    }
}

// ---

#[derive(Default)]
pub struct CompressedBuf(Vec<u8>);

impl CompressedBuf {
    pub fn new(data: &[u8]) -> Result<Self> {
        let mut encoded = Vec::new();
        FrameEncoder::new(&mut encoded).write_all(data)?;
        Ok(Self(encoded))
    }

    pub fn decode(&self, buf: &mut [u8]) -> Result<()> {
        FrameDecoder::new(&self.0[..]).read_exact(buf)
    }
}

impl TryFrom<&[u8]> for CompressedBuf {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self> {
        Self::new(data)
    }
}

impl TryInto<Buf> for &CompressedBuf {
    type Error = Error;

    fn try_into(self) -> Result<Buf> {
        let mut decoded = Buf::new();
        self.decode(&mut decoded)?;
        Ok(decoded)
    }
}

// ---

#[derive(Default)]
struct ReusableBuf {
    buf: Buf,
    len: usize,
}

impl ReusableBuf {
    fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0; capacity],
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    fn backstage(&mut self) -> &mut [u8] {
        &mut self.buf[self.len..]
    }

    fn extend(&mut self, n: usize) {
        self.len += n
    }

    fn full(&self) -> bool {
        self.len == self.buf.len()
    }

    fn clear(&mut self) -> &[u8] {
        self.len = 0;
        self.backstage()
    }

    fn replace(&mut self, buf: Buf) -> Buf {
        self.len = 0;
        replace(&mut self.buf, buf)
    }
}

// ---

#[derive(Default)]
pub struct MinimalCache {
    data: Option<(usize, Buf)>,
}

impl MinimalCache {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Cache for MinimalCache {
    fn cache<F: FnOnce() -> Result<Buf>>(&mut self, index: usize, f: F) -> Result<&[u8]> {
        if self.data.as_ref().map(|v| v.0) != Some(index) {
            self.data = Some((index, f()?));
        }
        Ok(&self.data.as_ref().unwrap().1)
    }
}

// ---

#[derive(Default)]
pub struct LruCache {
    limit: usize,
    data: BTreeMap<(Instant, usize), Buf>,
    timestamps: HashMap<usize, Instant>,
}

impl LruCache {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            data: BTreeMap::new(),
            timestamps: HashMap::new(),
        }
    }
}

impl Cache for LruCache {
    fn cache<F: FnOnce() -> Result<Buf>>(&mut self, index: usize, f: F) -> Result<&[u8]> {
        let now = Instant::now();
        if self.timestamps.len() == self.limit && !self.timestamps.contains_key(&index) {
            if let Some((&(timestamp, i), &_)) = self.data.iter().next() {
                self.timestamps.remove(&i);
                self.data.remove(&(timestamp, i));
            }
        }

        Ok(match self.timestamps.entry(index) {
            Entry::Vacant(e) => {
                e.insert(now);
                match self.data.entry((now, index)) {
                    BTreeEntry::Vacant(e) => e.insert(f()?),
                    BTreeEntry::Occupied(_) => unreachable!(),
                }
            }
            Entry::Occupied(mut e) => {
                let buf = self.data.remove(&(*e.get(), index)).unwrap();
                e.insert(now);
                match self.data.entry((now, index)) {
                    BTreeEntry::Vacant(e) => e.insert(buf),
                    BTreeEntry::Occupied(_) => unreachable!(),
                }
            }
        })
    }
}
