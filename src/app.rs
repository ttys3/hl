// std imports
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
use itertools::{izip, Itertools};
use serde_json as json;

// local imports
use crate::datefmt::{DateTimeFormat, DateTimeFormatter};
use crate::error::*;
use crate::formatting::RecordFormatter;
use crate::index::Indexer;
use crate::input::{ConcatReader, InputReference};
use crate::model::{Filter, Parser, ParserSettings, RawRecord};
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
        let parser = Parser::new(ParserSettings::new(
            &self.options.fields.settings,
            self.options.filter.since.is_some() || self.options.filter.until.is_some(),
        ));
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
                scope.spawn(closure!(ref bfo, ref parser, ref sfi, |_| {
                    let mut formatter = RecordFormatter::new(
                        self.options.theme.clone(),
                        DateTimeFormatter::new(
                            self.options.time_format.clone(),
                            self.options.time_zone,
                        ),
                        self.options.hide_empty_fields,
                        self.options.fields.filter.clone(),
                    )
                    .with_field_unescaping(!self.options.raw_fields);
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

        return Ok(());
    }

    fn sort(
        &self,
        inputs: Vec<InputReference>,
        output: &mut (dyn Write + Send + Sync),
    ) -> Result<()> {
        let cache_dir = directories::BaseDirs::new()
            .and_then(|d| Some(d.cache_dir().into()))
            .unwrap_or(PathBuf::from(".cache"))
            .join("github.com/pamburus/hl");
        fs::create_dir_all(&cache_dir)?;
        let indexer = Indexer::new(
            self.options.concurrency,
            self.options.buffer_size.try_into()?,
            self.options.max_message_size.try_into()?,
            cache_dir,
            &self.options.fields.settings,
        );

        let inputs = inputs
            .into_iter()
            .map(|x| x.index(&indexer))
            .collect::<Result<Vec<_>>>()?;

        let mut blocks: Vec<_> = inputs
            .iter()
            .enumerate()
            .map(|(i, input)| input.index.source().blocks.iter().map(|block| (block, i)))
            .flatten()
            .filter_map(|(block, i)| {
                if block.stat.lines_valid == 0 {
                    return None;
                }
                if let Some(level) = self.options.filter.level {
                    if !block.match_level(level) {
                        return None;
                    }
                }
                block
                    .stat
                    .ts_min_max
                    .map(|(ts_min, ts_max)| (block, ts_min, ts_max, i))
            })
            .collect();

        blocks.sort_by(|a, b| (a.1, a.2, a.3).partial_cmp(&(b.1, b.2, b.3)).unwrap());

        for input in inputs {
            writeln!(output, "{:#?}", input.index);
        }

        let n = self.options.concurrency;
        let m = inputs.len();
        let sfi = Arc::new(SegmentBufFactory::new(self.options.buffer_size.try_into()?));
        let bfo = BufFactory::new(self.options.buffer_size.try_into()?);
        thread::scope(|scope| -> Result<()> {
            // prepare receive/transmit channels for sorter stage
            let (stx, srx): (Vec<_>, Vec<_>) = (0..m).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for parser stage
            let (ptx, prx): (Vec<_>, Vec<_>) = (0..m).map(|_| channel::bounded(1)).unzip();
            // prepare receive/transmit channels for formatter stage
            let (ftx, frx): (Vec<_>, Vec<_>) = (0..n)
                .into_iter()
                .map(|_| channel::bounded::<Vec<u8>>(1))
                .unzip();
            // spawn reader thread
            let reader = scope.spawn(closure!(clone sfi, |_| -> Result<()> {
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
            // collect errors from reader and writer threads
            for reader in readers {
                reader.join().unwrap()?;
            }
            writer.join().unwrap()?;
            Ok(())
        })
        .unwrap()?;

        return Ok(());
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
            let remainder = if some {
                &data[stream.byte_offset()..]
            } else {
                data
            };
            if remainder.len() != 0 && self.filter.is_empty() {
                buf.extend_from_slice(remainder);
                buf.push(b'\n');
            }
        }
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
