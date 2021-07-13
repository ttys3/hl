use std::io::{Read, Write};
use std::sync::Arc;

use chrono::FixedOffset;
use closure::closure;
use crossbeam_channel as channel;
use crossbeam_channel::RecvError;
use crossbeam_utils::thread;
use itertools::izip;
use serde_json as json;

use crate::datefmt::{DateTimeFormat, DateTimeFormatter};
use crate::error::*;
use crate::formatting::{RecordFormatter, RecordFormatterState};
use crate::model::{Filter, Parser, ParserSettings, RawRecord};
use crate::scanning::{BufFactory, Scanner, Segment, SegmentBuf, SegmentBufFactory};
use crate::settings::Fields;
use crate::theme::Theme;
use crate::IncludeExcludeKeyFilter;

// TODO: merge Options to Settings and replace Options with Settings.
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
        input: &mut (dyn Read + Send + Sync),
        output: &mut (dyn Write + Send + Sync),
    ) -> Result<()> {
        let n = self.options.concurrency;
        let sfi = Arc::new(SegmentBufFactory::new(self.options.buffer_size));
        let bfo = BufFactory::new(self.options.buffer_size);
        let settings = ParserSettings::new(
            &self.options.fields.settings,
            self.options.filter.since.is_some() || self.options.filter.until.is_some(),
        );
        let parser = Parser::new(&settings);
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
                for item in scanner.items(input).with_max_segment_size(self.options.max_message_size) {
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
                    let mut state = RecordFormatterState::default();
                    for segment in rxi.iter() {
                        match segment {
                            Segment::Complete(segment) => {
                                let mut buf = bfo.new_buf();
                                self.process_segement(&parser, &segment, &mut formatter, &mut state, &mut buf, self.options.filter.is_empty());
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

    fn process_segement(
        &self,
        parser: &Parser,
        segment: &SegmentBuf,
        formatter: &mut RecordFormatter,
        state: &mut RecordFormatterState,
        buf: &mut Vec<u8>,
        include_unparsed: bool,
    ) {
        for data in rtrim(segment.data(), b'\n').split(|c| *c == b'\n') {
            if data.len() == 0 {
                buf.push(b'\n');
                continue;
            }
            let mut stream = json::Deserializer::from_slice(data).into_iter::<RawRecord>();
            let mut some = false;
            while let Some(Ok(record)) = stream.next() {
                some = true;
                let record = parser.parse(record);
                if record.matches(&self.options.filter) {
                    formatter.format_record(state, buf, &record);
                }
            }
            let remainder = if some {
                &data[stream.byte_offset()..]
            } else {
                data
            };
            if remainder.len() != 0 && include_unparsed {
                buf.extend_from_slice(remainder);
                buf.push(b'\n');
            }
        }
    }
}

fn rtrim<'a>(s: &'a [u8], c: u8) -> &'a [u8] {
    if s.len() > 0 && s[s.len() - 1] == c {
        &s[..s.len() - 1]
    } else {
        s
    }
}
