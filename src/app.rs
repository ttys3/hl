// std imports
// use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

// third-party imports
use chrono::FixedOffset;
use closure::closure;
use crossbeam_channel as channel;
use crossbeam_channel::RecvError;
use crossbeam_utils::thread;
use generic_array::{typenum::U32, GenericArray};
use itertools::izip;
use serde_json as json;
use sha2::{Digest, Sha256};

// local imports
use crate::datefmt::{DateTimeFormat, DateTimeFormatter};
use crate::error::*;
use crate::formatting::RecordFormatter;
use crate::index::Indexer;
use crate::input::{ConcatReader, InputReference};
use crate::model::{Filter, Record};
use crate::scanning::{BufFactory, Scanner, Segment, SegmentBuf, SegmentBufFactory};
use crate::theme::Theme;
use crate::IncludeExcludeKeyFilter;

// ---

pub struct Options {
    pub theme: Arc<Theme>,
    pub time_format: DateTimeFormat,
    pub raw_fields: bool,
    pub buffer_size: usize,
    pub max_message_size: usize,
    pub concurrency: usize,
    pub filter: Filter,
    pub fields: Arc<IncludeExcludeKeyFilter>,
    pub time_zone: FixedOffset,
    pub hide_empty_fields: bool,
    pub sort: bool,
}

pub struct App {
    options: Options,
}

impl App {
    pub fn new(options: Options) -> Self {
        Self { options }
    }

    pub fn run(
        &self,
        inputs: Vec<InputReference>,
        output: &mut (dyn Write + Send + Sync),
    ) -> Result<()> {
        if self.options.sort {
            self.sort(inputs, output)
        } else {
            self.cat(inputs, output)
        }
    }

    fn cat(
        &self,
        inputs: Vec<InputReference>,
        output: &mut (dyn Write + Send + Sync),
    ) -> Result<()> {
        let inputs = inputs
            .into_iter()
            .map(|x| x.into())
            .collect::<std::io::Result<Vec<_>>>()?;

        let mut input = ConcatReader::new(inputs.into_iter().map(|x| Ok(x)));
        let n = self.options.concurrency;
        let sfi = Arc::new(SegmentBufFactory::new(self.options.buffer_size.try_into()?));
        let bfo = BufFactory::new(self.options.buffer_size.try_into()?);
        thread::scope(|scope| -> Result<()> {
            // prepare receive/transmit channels for input data
            let (txi, rxi): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for output data
            let (txo, rxo): (Vec<_>, Vec<_>) = (0..n)
                .into_iter()
                .map(|_| channel::bounded::<Vec<u8>>(1))
                .unzip();
            // spawn reader thread
            let reader = scope.spawn(closure!(clone sfi, |_| -> Result<()> {
                let mut sn: usize = 0;
                let scanner = Scanner::new(sfi, "\n".to_string());
                for item in scanner.items(&mut input).with_max_segment_size(self.options.max_message_size) {
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
                    let mut formatter = RecordFormatter::new(
                        self.options.theme.clone(),
                        DateTimeFormatter::new(
                            self.options.time_format.clone(),
                            self.options.time_zone,
                        ),
                        self.options.hide_empty_fields,
                        self.options.fields.clone(),
                    )
                    .with_field_unescaping(!self.options.raw_fields);
                    for segment in rxi.iter() {
                        match segment {
                            Segment::Complete(segment) => {
                                let mut buf = bfo.new_buf();
                                self.process_segement(&segment, &mut formatter, &mut buf);
                                sfi.recycle(segment);
                                if let Err(_) = txo.send(buf) {
                                    break;
                                };
                            }
                            Segment::Incomplete(segment, _) => {
                                if let Err(_) = txo.send(segment.to_vec()) {
                                    break;
                                }
                            }
                        }
                    }
                }));
            }
            // spawn writer thread
            let writer = scope.spawn(closure!(ref bfo, |_| -> Result<()> {
                let mut sn = 0;
                loop {
                    match rxo[sn % n].recv() {
                        Ok(buf) => {
                            output.write_all(&buf[..])?;
                            bfo.recycle(buf);
                        }
                        Err(RecvError) => {
                            break;
                        }
                    }
                    sn += 1;
                }
                Ok(())
            }));
            // collect errors from reader and writer threads
            reader.join().unwrap()?;
            writer.join().unwrap()?;
            Ok(())
        })
        .unwrap()?;

        Ok(())
    }

    fn process_segement(
        &self,
        segment: &SegmentBuf,
        formatter: &mut RecordFormatter,
        buf: &mut Vec<u8>,
    ) {
        for data in segment.data().split(|c| *c == b'\n') {
            let data = trim_right(data, |ch| ch == b'\r');
            if data.len() == 0 {
                continue;
            }
            let mut stream = json::Deserializer::from_slice(data).into_iter::<Record>();
            while let Some(Ok(record)) = stream.next() {
                if record.matches(&self.options.filter) {
                    formatter.format_record(buf, &record);
                }
            }
            let remainder = trim_right(&data[stream.byte_offset()..], |ch| match ch {
                b'\r' | b'\n' | b' ' | b'\t' => true,
                _ => false,
            });
            if remainder.len() > 0 {
                buf.extend_from_slice(remainder);
                buf.push(b'\n');
            }
        }
    }

    fn sort(
        &self,
        inputs: Vec<InputReference>,
        output: &mut (dyn Write + Send + Sync),
    ) -> Result<()> {
        let param_hash = hex::encode(self.parameters_hash()?);
        let cache_dir = directories::BaseDirs::new()
            .and_then(|d| Some(d.cache_dir().into()))
            .unwrap_or(PathBuf::from(".cache"))
            .join("github.com/pamburus/hl")
            .join(param_hash);
        fs::create_dir_all(&cache_dir)?;
        let indexer = Indexer::new(
            self.options.concurrency,
            self.options.buffer_size.try_into()?,
            self.options.max_message_size.try_into()?,
            cache_dir,
        );

        let inputs = inputs
            .into_iter()
            .map(|x| x.index(&indexer))
            .collect::<Result<Vec<_>>>()?;

        /*
        for input in inputs {
            // writeln!(output, "{:#?}", input.index)?;
            for block in input.into_blocks().sorted() {
                // writeln!(
                //     output,
                //     "block at {} with size {}",
                //     block.offset(),
                //     block.size()
                // )?;
                // writeln!(output, "{:#?}", block.source_block())?;
                for line in block.into_lines()? {
                    // writeln!(output, "{} bytes at {}", line.len(), line.offset())?;
                    output.write_all(line.bytes())?;
                }
            }
        }
        */

        let mut blocks: Vec<_> = inputs
            .into_iter()
            .enumerate()
            .map(|(i, input)| input.into_blocks().map(move |block| (block, i)))
            .flatten()
            .filter_map(|(block, i)| {
                let src = block.source_block();
                if src.stat.lines_valid == 0 {
                    return None;
                }
                if let Some(level) = self.options.filter.level {
                    if !src.match_level(level) {
                        return None;
                    }
                }
                src.stat
                    .ts_min_max
                    .map(|(ts_min, ts_max)| (block, ts_min, ts_max, i))
            })
            .collect();

        blocks.sort_by(|a, b| (a.1, a.2, a.3).partial_cmp(&(b.1, b.2, b.3)).unwrap());

        for (block, ts_min, ts_max, i) in blocks {
            writeln!(
                output,
                "|{:10}.{:09}|{:10}.{:09} {:7} @[{}]{:9}",
                ts_min.0,
                ts_min.1,
                ts_max.0,
                ts_max.1,
                block.size(),
                i,
                block.offset(),
            )?;
        }

        /*
        let n = self.options.concurrency;
        let sfi = Arc::new(SegmentBufFactory::new(self.options.buffer_size.try_into()?));
        let bfo = BufFactory::new(self.options.buffer_size.try_into()?);
        thread::scope(|scope| -> Result<()> {
            // prepare receive/transmit channels for sorter stage
            let (stx, srx): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for parser stage
            let (ptx, prx): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for formatter stage
            let (ftx, frx): (Vec<_>, Vec<_>) = (0..n)
                .into_iter()
                .map(|_| channel::bounded::<Vec<u8>>(1))
                .unzip();
            // spawn processing threads
            for (rxi, txo) in izip!(rxi, txo) {
                scope.spawn(closure!(ref bfo, ref sfi, |_| {
                    let mut formatter = RecordFormatter::new(
                        self.options.theme.clone(),
                        DateTimeFormatter::new(
                            self.options.time_format.clone(),
                            self.options.time_zone,
                        ),
                        self.options.hide_empty_fields,
                        self.options.fields.clone(),
                    )
                    .with_field_unescaping(!self.options.raw_fields);
                    for segment in rxi.iter() {
                        match segment {
                            Segment::Complete(segment) => {
                                let mut buf = bfo.new_buf();
                                self.process_segement(&segment, &mut formatter, &mut buf);
                                sfi.recycle(segment);
                                if let Err(_) = txo.send(buf) {
                                    break;
                                };
                            }
                            Segment::Incomplete(segment) => {
                                if let Err(_) = txo.send(segment.to_vec()) {
                                    break;
                                }
                            }
                        }
                    }
                }));
            }
            // spawn writer thread
            let writer = scope.spawn(closure!(ref bfo, |_| -> Result<()> {
                let mut sn = 0;
                loop {
                    match rxo[sn % n].recv() {
                        Ok(buf) => {
                            output.write_all(&buf[..])?;
                            bfo.recycle(buf);
                        }
                        Err(RecvError) => {
                            break;
                        }
                    }
                    sn += 1;
                }
                Ok(())
            }));
            */

        /*
            // spawn reader threads
            let reader = scope.spawn(closure!(clone sfi, |_| -> Result<()> {
                let mut workspace = VecDeque::new();
                let mut last_ts = None;
                let mut blocks = blocks.iter();
                loop {
                    if let Some(last_ts) = last_ts {
                        if workspace.front().map(|x|x.2 < last_ts).unwrap_or_default() {
                            workspace.pop_front();
                        }
                        if workspace.back().map(|x|x.2 >= last_ts)
                    }
                    if workspace.len() != 0 {
                            if workspace.front().
                        }
                    }
                    if workspace.len() == 0 || workspace.back().
                }
                let input = &inputs[i];
                let blocks = input.index.source().blocks.clone();
                blocks.sort_by(|a, b|a.stat.ts_min_max.partial_cmp(&b.stat.ts_min_max).unwrap());
                let scanner = Scanner::new(sfi, "\n".to_string());
                for item in scanner.items(&mut input.stream) {
                    if let Err(_) = stx[i].send(item?) {
                        break;
                    }
                }
                Ok(())
            }));
            // collect errors from reader and writer threads
            for reader in readers {
                reader.join().unwrap()?;
            }
            writer.join().unwrap()?;
            Ok(())
        })
        .unwrap()?;
            */

        Ok(())
    }

    fn parameters_hash(&self) -> Result<GenericArray<u8, U32>> {
        let mut hasher = Sha256::new();
        bincode::serialize_into(
            &mut hasher,
            &(self.options.buffer_size, self.options.max_message_size),
        )?;
        Ok(hasher.finalize())
    }
}

fn trim_right<'a, F: Fn(u8) -> bool>(slice: &'a [u8], predicate: F) -> &'a [u8] {
    if let Some(pos) = slice.iter().rposition(|&ch| !predicate(ch)) {
        &slice[..pos + 1]
    } else {
        &slice[0..0]
    }
}

// [1..4] [2..5] [4..10] [12..14] [14..18]
// [[1..4], [2..5]], [[2..5], [4..10]]
