// std imports
use std::{
    cmp::min,
    io::{Read, Result, Write},
    mem::replace,
    num::NonZeroUsize,
};

// third-party imports
use clru::CLruCache;
use snap::{read::FrameDecoder, write::FrameEncoder};

// ---

pub struct ReplayingReader<I> {
    inner: I,
    segment_size: NonZeroUsize,
    data: Vec<Vec<u8>>,
    scratch: Vec<u8>,
    scratch_size: usize,
    cache: CLruCache<usize, Vec<u8>>,
    position: usize,
}

impl<I: Read> ReplayingReader<I> {
    pub fn builder(inner: I) -> ReplayingReaderBuilder<I> {
        ReplayingReaderBuilder::new(inner)
    }

    pub fn new(inner: I) -> Self {
        Self::builder(inner).build()
    }

    fn segment(&mut self, index: usize) -> Result<&Vec<u8>> {
        let n = self.data.len();
        if index >= n {
            for i in n..index {
                let buf = self.load_next()?;
                self.data.push(buf);
            }
            let data = self.load_next()?;
            self.cache.put(index, data);
        }
        if let Some(data) = &self.cache.get(&index) {
            return Ok(data);
        }
        let data = self.reload(index)?;
        self.cache.put(index, data);
        Ok(&self.cache.get(&index).unwrap())
    }

    fn next(&mut self) -> Result<&[u8]> {
        if let Some(buf) = self.try_load_next()? {
            let index = self.position / self.segment_size;
            self.cache.put(index, buf);
            Ok(self.cache.get(&index).unwrap())
        } else {
            Ok(&self.scratch[..self.scratch_size])
        }
    }

    fn try_load_next(&mut self) -> Result<Option<Vec<u8>>> {
        let n = self.inner.read(&mut self.scratch[self.scratch_size..])?;
        self.scratch_size += n;
        if self.scratch_size != usize::from(self.segment_size) {
            return Ok(None);
        }
        let mut encoded = Vec::new();
        let mut writer = FrameEncoder::new(&mut encoded);
        writer.write_all(&self.scratch)?;
        self.data.push(encoded);
        self.scratch_size = 0;
        Ok(Some(replace(&mut self.scratch, self.buf())))
    }

    fn reload(&mut self, index: usize) -> Result<Vec<u8>> {
        let mut buf = self.buf();
        let mut reader = FrameDecoder::new(&self.data[index][..]);
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn buf(&self) -> Vec<u8> {
        vec![0; usize::from(self.segment_size)]
    }
}

impl<I: Read> Read for ReplayingReader<I> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut i = 0;
        let mut n = buf.len();
        loop {
            let segment = self.position / self.segment_size;
            let offset = self.position % self.segment_size;
            let data = self.segment(segment)?;
            if offset == 0 && segment == self.data.len() {}
            let data = self.load(segment);
            if let Some(data) = self.cache.get(&segment) {
                let k = min(n, usize::from(self.segment_size) - offset);
                buf[i..i + k].copy_from_slice(&data[offset..offset + k]);
                i += k;
                n -= k;
            }
        }
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

    pub fn segment_size(self, segment_size: usize) -> Self {
        self.segment_size = NonZeroUsize::new(segment_size).unwrap();
        self
    }

    pub fn cache_size(self, segment_count: usize) -> Self {
        self.cache_size = NonZeroUsize::new(segment_count).unwrap();
        self
    }

    pub fn build(self) -> ReplayingReader<I> {
        ReplayingReader {
            inner: self.inner,
            segment_size: self.segment_size,
            data: Vec::new(),
            scratch: vec![0; usize::from(self.segment_size)],
            scratch_size: 0,
            cache: CLruCache::new(self.cache_size),
            position: 0,
        }
    }
}
