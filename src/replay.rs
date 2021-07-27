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

type Buf = Vec<u8>;

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

    fn segment(&mut self, index: usize) -> Result<Segment<&[u8]>> {
        if index >= self.data.len() {
            panic!("logic error")
        }
        if index == self.data.len() {
            if let Some(buf) = self.fetch()? {
                Ok(Segment::Complete(self.cache(index, buf)))
            } else {
                Ok(Segment::Incomplete(self.scratch.bytes()))
            }
        } else {
            Ok(Segment::Complete(self.reload(index)?))
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
        let result = self.cache.try_put_or_modify(index, put, modify, ())?;
        Ok(result)
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
            let segment = self.segment(segment)?;
            let data = segment.data();
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
struct CompressedBuf(Vec<u8>);

impl CompressedBuf {
    fn new(data: &[u8]) -> Result<Self> {
        let mut encoded = Vec::new();
        FrameEncoder::new(&mut encoded).write_all(data)?;
        Ok(Self(encoded))
    }

    fn decode(&self, buf: &mut Buf) -> Result<()> {
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
