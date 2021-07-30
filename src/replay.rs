// std imports
use std::{
    cmp::min,
    convert::{TryFrom, TryInto},
    io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
    mem::replace,
    num::NonZeroUsize,
};

// third-party imports
use clru::CLruCache;
use snap::{read::FrameDecoder, write::FrameEncoder};

// ---

const DEFAULT_SEGMENT_SIZE: Option<NonZeroUsize> = NonZeroUsize::new(256 * 1024);

// ---

type Buf = Vec<u8>;

// ---

pub struct ReplayBuf {
    segment_size: NonZeroUsize,
    segments: Vec<CompressedBuf>,
    size: usize,
}

impl ReplayBuf {
    pub fn new() -> Self {
        Self::with_segment_size(DEFAULT_SEGMENT_SIZE.unwrap())
    }

    pub fn with_segment_size(segment_size: NonZeroUsize) -> Self {
        Self {
            segment_size,
            segments: Vec::new(),
            size: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.size
    }

    pub fn segments(&self) -> &Vec<CompressedBuf> {
        &self.segments
    }
}

impl TryFrom<ReplayBufBuilder> for ReplayBuf {
    type Error = Error;

    fn try_from(builder: ReplayBufBuilder) -> Result<Self> {
        builder.result()
    }
}

// ---

pub struct ReplayBufBuilder {
    buf: ReplayBuf,
    scratch: ReusableBuf,
}

impl ReplayBufBuilder {
    pub fn new() -> Self {
        Self::with_segment_size(DEFAULT_SEGMENT_SIZE.unwrap())
    }

    pub fn with_segment_size(segment_size: NonZeroUsize) -> Self {
        Self {
            buf: ReplayBuf::with_segment_size(segment_size),
            scratch: ReusableBuf::new(usize::from(segment_size)),
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

impl Write for ReplayBufBuilder {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut k: usize = 0;
        if buf.len() != 0 {
            self.prepare()?;
        }
        while k < buf.len() {
            let buf = &buf[k..];
            let target = self.scratch.backstage();
            let n = min(buf.len(), target.len());
            target[..n].copy_from_slice(buf);
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

// ---

pub trait Cache: Default {
    fn cache<F: FnOnce() -> Result<Buf>>(&mut self, index: usize, f: F) -> Result<&[u8]>;
}

// ---

#[derive(Default)]
pub struct MinimalCache {
    data: Option<(usize, Buf)>,
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

pub struct ReplayBufReader<C: Cache = MinimalCache> {
    buf: ReplayBuf,
    cache: C,
    position: usize,
}

impl<C: Cache> ReplayBufReader<C> {
    pub fn new(buf: ReplayBuf) -> Self {
        Self {
            buf,
            cache: C::default(),
            position: 0,
        }
    }

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

// ---

pub struct ReplayingReader<I> {
    inner: I,
    segment_size: NonZeroUsize,
    data: Vec<CompressedBuf>,
    scratch: ReusableBuf,
    cache: CLruCache<usize, Buf>,
    position: usize,
    end_position: Option<usize>,
}

impl<I: Read> ReplayingReader<I> {
    pub fn builder(inner: I) -> ReplayingReaderBuilder<I> {
        ReplayingReaderBuilder::new(inner)
    }

    pub fn new(inner: I) -> Self {
        Self::builder(inner).build()
    }

    fn segment(&mut self, index: usize) -> Result<&[u8]> {
        if index >= self.data.len() {
            panic!("logic error")
        }
        if index == self.data.len() {
            if let Some(buf) = self.fetch()? {
                Ok(self.cache(index, buf))
            } else {
                Ok(self.scratch.bytes())
            }
        } else {
            Ok(self.reload(index)?)
        }
    }

    fn fetch(&mut self) -> Result<Option<Buf>> {
        let n = self.inner.read(self.scratch.backstage())?;
        self.scratch.extend(n);
        if self.scratch.full() {
            self.data.push(self.scratch.bytes().try_into()?);
            Ok(Some(self.scratch.replace(self.new_buf())))
        } else {
            Ok(None)
        }
    }

    fn reload(&mut self, index: usize) -> Result<&Buf> {
        let ss = usize::from(self.segment_size);
        let data = &mut self.data;
        let put = |index: &usize, _| -> Result<Buf> {
            let mut buf = vec![0; ss];
            data[*index].decode(&mut buf)?;
            Ok(buf)
        };
        let modify = |_: &usize, _: &mut Buf, _| Ok(());
        Ok(self.cache.try_put_or_modify(index, put, modify, ())?)
    }

    fn cache(&mut self, index: usize, buf: Buf) -> &Buf {
        self.cache.put(index, buf);
        &self.cache.get(&index).unwrap()
    }

    fn new_buf(&self) -> Buf {
        vec![0; usize::from(self.segment_size)]
    }

    fn from_current(&self, offset: i64) -> Option<usize> {
        usize::try_from(i64::try_from(self.position).ok()?.checked_add(offset)?).ok()
    }

    fn from_end(&mut self, offset: i64) -> Result<usize> {
        let total_size = self.detect_total_size()?;
        self.before_pos(total_size, offset)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "position out of range"))
    }

    fn before_pos(&self, position: usize, offset: i64) -> Option<usize> {
        usize::try_from(i64::try_from(position).ok()?.checked_sub(offset)?).ok()
    }

    fn detect_total_size(&mut self) -> Result<usize> {
        panic!("not implemented")
    }

    fn seek_forward(&mut self, position: usize) -> Result<usize> {
        while self.fetch_position() < position {
            if self.fetch()?.is_none() {
                return Ok(self.fetch_position());
            }
        }
        self.position = position;
        Ok(position)
    }

    fn fetch_position(&self) -> usize {
        self.fetch_position_aligned() + self.scratch.len()
    }

    fn fetch_position_aligned(&self) -> usize {
        self.data.len() * usize::from(self.segment_size)
    }
}

impl<I: Read> Read for ReplayingReader<I> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut i = 0;
        let ss = usize::from(self.segment_size);
        loop {
            let segment = self.position / self.segment_size;
            let offset = self.position % self.segment_size;
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

impl<I: Read> Seek for ReplayingReader<I> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let mut pos = match pos {
            SeekFrom::Start(pos) => usize::try_from(pos).map_err(|e| Error::new(ErrorKind::InvalidInput, e)),
            SeekFrom::Current(pos) => self
                .from_current(pos)
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "position out of range")),
            SeekFrom::End(pos) => self.from_end(pos),
        }?;
        let n = self.data.len() * usize::from(self.segment_size);
        if pos > n {
            pos = self.seek_forward(pos)?;
        }
        self.position = pos;
        u64::try_from(pos).map_err(|e| Error::new(ErrorKind::InvalidInput, e))
    }
}

// ---

pub struct ReplayingReaderBuilder<I> {
    inner: I,
    segment_size: NonZeroUsize,
    cache_size: NonZeroUsize,
}

impl<I> ReplayingReaderBuilder<I> {
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            segment_size: NonZeroUsize::new(256 * 1024).unwrap(),
            cache_size: NonZeroUsize::new(4).unwrap(),
        }
    }

    pub fn segment_size(mut self, segment_size: usize) -> Self {
        self.segment_size = NonZeroUsize::new(segment_size).unwrap();
        self
    }

    pub fn cache_size(mut self, segment_count: usize) -> Self {
        self.cache_size = NonZeroUsize::new(segment_count).unwrap();
        self
    }

    pub fn build(self) -> ReplayingReader<I> {
        ReplayingReader {
            inner: self.inner,
            segment_size: self.segment_size,
            data: Vec::new(),
            scratch: ReusableBuf::new(usize::from(self.segment_size)),
            cache: CLruCache::new(self.cache_size),
            position: 0,
            end_position: None,
        }
    }
}

// ---

enum Segment<T> {
    Complete(T),
    Incomplete(T),
}

impl<T> Segment<T> {
    fn data(&self) -> &T {
        match self {
            Self::Complete(data) => data,
            Self::Incomplete(data) => data,
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Complete(_) => true,
            Self::Incomplete(_) => false,
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
