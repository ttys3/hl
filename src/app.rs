// std imports
use std::cmp::{max, Reverse};
use std::collections::VecDeque;
use std::convert::{TryFrom, TryInto};
use std::fs;
use std::io::{BufWriter, Write};
use std::iter::once;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

// third-party imports
use chrono::FixedOffset;
use closure::closure;
use crossbeam_channel::{self as channel, Receiver, RecvError, Sender};
use crossbeam_utils::thread;
use itertools::izip;
use serde_json as json;
use sha2::{Digest, Sha256};

// local imports
use crate::datefmt::{DateTimeFormat, DateTimeFormatter};
use crate::error::*;
use crate::formatting::RecordFormatter;
use crate::index::{Indexer, Timestamp};
use crate::input::{BlockLine, BlockLines, ConcatReader, IndexedInput, InputReference};
use crate::model::{Filter, Parser, ParserSettings, RawRecord};
use crate::pool::SQPool;
use crate::scanning::{BufFactory, Scanner, Segment, SegmentBufFactory};
use crate::settings::Fields;
use crate::theme::Theme;
use crate::IncludeExcludeKeyFilter;

// TODO: merge Options to Settings and replace Options with Settings.

// ---

pub struct Options {
    pub theme: Arc<Theme>,
    pub time_format: DateTimeFormat,
    pub raw_fields: bool,
    pub buffer_size: usize,
    pub max_message_size: usize,
    pub concurrency: usize,
    pub filter: Filter,
    pub fields: FieldOptions,
    pub time_zone: FixedOffset,
    pub hide_empty_fields: bool,
    pub sort: bool,
}

pub struct FieldOptions {
    pub filter: Arc<IncludeExcludeKeyFilter>,
    pub settings: Fields,
}

pub struct App {
    options: Options,
}

impl App {
    pub fn new(options: Options) -> Self {
        Self { options }
    }

    pub fn run(&self, inputs: Vec<InputReference>, output: &mut (dyn Write + Send + Sync)) -> Result<()> {
        if self.options.sort {
            self.sort(inputs, output)
        } else {
            self.cat(inputs, output)
        }
    }

    fn cat(&self, inputs: Vec<InputReference>, output: &mut (dyn Write + Send + Sync)) -> Result<()> {
        let inputs = inputs
            .into_iter()
            .map(|x| x.into())
            .collect::<std::io::Result<Vec<_>>>()?;

        let mut input = ConcatReader::new(inputs.into_iter().map(|x| Ok(x)));
        let n = self.options.concurrency;
        let sfi = Arc::new(SegmentBufFactory::new(self.options.buffer_size.try_into()?));
        let bfo = BufFactory::new(self.options.buffer_size.try_into()?);
        let parser = self.parser();
        thread::scope(|scope| -> Result<()> {
            // prepare receive/transmit channels for input data
            let (txi, rxi): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for output data
            let (txo, rxo): (Vec<_>, Vec<_>) = (0..n).into_iter().map(|_| channel::bounded::<Vec<u8>>(1)).unzip();
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
                scope.spawn(closure!(ref bfo, ref parser, ref sfi, |_| {
                    let mut formatter = self.formatter();
                    let mut processor = SegmentProcesor::new(&parser, &mut formatter, &self.options.filter);
                    for segment in rxi.iter() {
                        match segment {
                            Segment::Complete(segment) => {
                                let mut buf = bfo.new_buf();
                                processor.run(segment.data(), &mut buf);
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

    fn sort(&self, inputs: Vec<InputReference>, output: &mut (dyn Write + Send + Sync)) -> Result<()> {
        let mut output = BufWriter::new(output);
        let param_hash = hex::encode(self.parameters_hash()?);
        let cache_dir = directories::BaseDirs::new()
            .map(|d| d.cache_dir().into())
            .unwrap_or_else(|| PathBuf::from(".cache"))
            .join("github.com/pamburus/hl")
            .join(param_hash);
        fs::create_dir_all(&cache_dir)?;
        let indexer = Indexer::new(
            self.options.concurrency,
            self.options.buffer_size.try_into()?,
            self.options.max_message_size.try_into()?,
            cache_dir,
            &self.options.fields.settings.predefined,
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

        // if blocks.len() == 0 {
        //     return Ok(());
        // }

        let n = self.options.concurrency;
        let parser = self.parser();
        thread::scope(|scope| -> Result<()> {
            // prepare transmit/receive channels for data produced by pusher thread
            let (txp, rxp): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded(1)).unzip();
            // prepare transmit/receive channels for data produced by worker threads
            let (txw, rxw): (Vec<_>, Vec<_>) = (0..n).map(|_| channel::bounded::<OutputBlock>(1)).unzip();
            // spawn pusher thread
            let pusher = scope.spawn(closure!(|_| -> Result<()> {
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
                        src.stat.ts_min_max.map(|(ts_min, ts_max)| (block, ts_min, ts_max, i))
                    })
                    .collect();

                blocks.sort_by(|a, b| (a.1, a.2, a.3).partial_cmp(&(b.1, b.2, b.3)).unwrap());

                // for (block, ts_min, ts_max, i) in &blocks {
                //     writeln!(
                //         output,
                //         "|{:10}.{:09}|{:10}.{:09} {:7} @[{}]{:9}",
                //         ts_min.0,
                //         ts_min.1,
                //         ts_max.0,
                //         ts_max.1,
                //         block.size(),
                //         i,
                //         block.offset(),
                //     )?;
                // }
                let mut output = StripedSender::new(txp);
                for (block, ts_min, ts_max, i) in blocks {
                    if output.send((block, i)).is_none() {
                        break;
                    }
                }
                Ok(())
            }));
            // spawn worker threads
            let mut workers = Vec::with_capacity(n);
            for (rxp, txw) in izip!(rxp, txw) {
                workers.push(scope.spawn(closure!(ref parser, |_| -> Result<()> {
                    let mut formatter = self.formatter();
                    for (block, i) in rxp.iter() {
                        let mut buf = Vec::with_capacity(2 * usize::try_from(block.size())?);
                        let mut items = Vec::with_capacity(2 * usize::try_from(block.lines_valid())?);
                        for line in block.into_lines()? {
                            if line.len() == 0 {
                                continue;
                            }
                            let record = parser.parse(json::from_slice(line.bytes())?);
                            if record.matches(&self.options.filter) {
                                let offset = buf.len();
                                formatter.format_record(&mut buf, &record);
                                items.push((record.ts.unwrap().unix_utc().unwrap().into(), offset..buf.len()));
                            }
                        }

                        let buf = Arc::new(buf);
                        if txw.send(OutputBlock { buf, items }).is_err() {
                            break;
                        }
                    }
                    Ok(())
                })));
            }
            // spawn merger thread
            /*
            let merger = scope.spawn(|_| -> Result<()> {
                let mut input = StripedReceiver::new(rxw);
                let (mut tsi, mut tso) = (None, None);
                let mut workspace = Vec::new();

                loop {
                    while tso >= tsi {
                        if let Some(block) = input.next() {
                            let mut tail = block.into_lines();
                            let head = tail.next();
                            if let Some(head) = head {
                                tsi = Some(head.0.clone());
                                tso = tso.or(tsi);
                                workspace.push((head, tail));
                            }
                        }
                    }

                    if workspace.len() == 0 {
                        break;
                    }

                    workspace.sort_by_key(|v| Reverse((v.0).0));
                    let k = workspace.len() - 1;
                    let item = &mut workspace[k];
                    let ts = (item.0).0;
                    tso = Some(ts);
                    if tso >= tsi {
                        continue;
                    }
                    output.write_all((item.0).1.bytes())?;
                    match item.1.next() {
                        Some(head) => item.0 = head,
                        None => drop(workspace.swap_remove(k)),
                    }
                }

                Ok(())
            });
            */
            // spawn catter thread
            let catter = scope.spawn(|_| -> Result<()> {
                for block in StripedReceiver::new(rxw) {
                    for (_, line) in block.into_lines() {
                        output.write_all(line.bytes())?;
                    }
                }

                Ok(())
            });

            pusher.join().unwrap()?;
            for worker in workers {
                worker.join().unwrap()?;
            }
            catter.join().unwrap()?;

            Ok(())
        })
        .unwrap()?;
        /*
                let batch = |ts_min| {
                    let mut ts_next = ts_min;
                    let mut next = None;
                    let mut first = true;
                    |it| if first {
                            first = false;
                            (ts_next, ||{
                                let result = it.next();
                                next = it.next();
                                result
                            })
                        } else {
                            ()
                        }
                        None =>                     match it.next() {
                            None => None,
                            Some(line) => {
                                next = Some(line),
                            }
                        }


                        Some()
                    }
                };

                let (block, ts_min, ts_max, i) = blocks[0];
                let mut ts_next = ts_min;
                let lazy_lines = block
                    .into_lines()
                    .into_iter()
                    .batching(batch);
        */

        /*
        let mut workspace = VecDeque::new();
        let mut last_ts = None;
        let mut blocks = blocks.into_iter();
        let mut next = blocks.next();
        loop {
            if let Some(last_ts) = last_ts {
                if workspace.back().map(|x|x.2 >= last_ts) {
                    if let Some((block, ts_min, ts_max, i)) = blocks.next() {
                        workspace.push_back((block.into_lines(), ts_min, ts_max, i));
                    }
                }
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
        */

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

    fn parameters_hash(&self) -> Result<[u8; 32]> {
        let mut hasher = Sha256::new();
        bincode::serialize_into(
            &mut hasher,
            &(
                &self.options.buffer_size,
                &self.options.max_message_size,
                &self.options.fields.settings.predefined,
            ),
        )?;
        Ok(hasher.finalize().into())
    }

    fn parser(&self) -> Parser {
        Parser::new(ParserSettings::new(
            &self.options.fields.settings.predefined,
            &self.options.fields.settings.ignore,
            self.options.filter.since.is_some() || self.options.filter.until.is_some(),
        ))
    }

    fn formatter(&self) -> RecordFormatter {
        RecordFormatter::new(
            self.options.theme.clone(),
            DateTimeFormatter::new(self.options.time_format.clone(), self.options.time_zone),
            self.options.hide_empty_fields,
            self.options.fields.filter.clone(),
        )
        .with_field_unescaping(!self.options.raw_fields)
    }
}

// ---

pub struct SegmentProcesor<'a> {
    parser: &'a Parser,
    formatter: &'a mut RecordFormatter,
    filter: &'a Filter,
}

impl<'a> SegmentProcesor<'a> {
    pub fn new(parser: &'a Parser, formatter: &'a mut RecordFormatter, filter: &'a Filter) -> Self {
        Self {
            parser,
            formatter,
            filter,
        }
    }

    pub fn run(&mut self, data: &[u8], buf: &mut Vec<u8>) {
        for data in rtrim(data, b'\n').split(|c| *c == b'\n') {
            if data.len() == 0 {
                buf.push(b'\n');
                continue;
            }
            let mut stream = json::Deserializer::from_slice(data).into_iter::<RawRecord>();
            let mut some = false;
            while let Some(Ok(record)) = stream.next() {
                some = true;
                let record = self.parser.parse(record);
                if record.matches(self.filter) {
                    self.formatter.format_record(buf, &record);
                }
            }
            let remainder = if some { &data[stream.byte_offset()..] } else { data };
            if remainder.len() != 0 && self.filter.is_empty() {
                buf.extend_from_slice(remainder);
                buf.push(b'\n');
            }
        }
    }
}

// ---

struct OutputBlock {
    buf: Arc<Vec<u8>>,
    items: Vec<(Timestamp, Range<usize>)>,
}

impl OutputBlock {
    pub fn lines<'a>(&'a self) -> impl Iterator<Item = (Timestamp, BlockLine)> + 'a {
        let buf = self.buf.clone();
        self.items
            .iter()
            .map(move |(ts, range)| (ts.clone(), BlockLine::new(buf.clone(), range.clone())))
    }

    pub fn into_lines(self) -> impl Iterator<Item = (Timestamp, BlockLine)> {
        let buf = self.buf;
        self.items
            .into_iter()
            .map(move |(ts, range)| (ts, BlockLine::new(buf.clone(), range.clone())))
    }
}

// ---

struct StripedReceiver<T> {
    input: Vec<Receiver<T>>,
    sn: usize,
}

impl<T> StripedReceiver<T> {
    fn new(input: Vec<Receiver<T>>) -> Self {
        Self { input, sn: 0 }
    }
}

impl<T> Iterator for StripedReceiver<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.input[self.sn].recv().ok();
        self.sn = (self.sn + 1) % self.input.len();
        result
    }
}

// ---

struct StripedSender<T> {
    output: Vec<Sender<T>>,
    sn: usize,
}

impl<T> StripedSender<T> {
    fn new(output: Vec<Sender<T>>) -> Self {
        Self { output, sn: 0 }
    }

    fn send(&mut self, value: T) -> Option<()> {
        self.output[self.sn].send(value).ok()?;
        self.sn = (self.sn + 1) % self.output.len();
        Some(())
    }
}

// ---

fn rtrim<'a>(s: &'a [u8], c: u8) -> &'a [u8] {
    if s.len() > 0 && s[s.len() - 1] == c {
        &s[..s.len() - 1]
    } else {
        s
    }
}

// [1..4] [2..5] [4..10] [12..14] [14..18]
// [[1..4], [2..5]], [[2..5], [4..10]]
