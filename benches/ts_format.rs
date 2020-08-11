use std::io::Write;

use chrono::{format::strftime::StrftimeItems, Datelike, Timelike};
use criterion::{criterion_group, criterion_main, Criterion};

use hl::datefmt::{format_date, LinuxDateFormat};
use hl::timestamp::Timestamp;

fn criterion_benchmark(c: &mut Criterion) {
    let ts = Timestamp::new("2020-06-27T00:48:30.466249792+03:00")
        .parse()
        .unwrap();
    let tsn = ts.naive_local();
    c.bench_function("datefmt format [%y-%m-%d %T.%N]", |b| {
        let mut buf = Vec::<u8>::with_capacity(4096);
        let format = LinuxDateFormat::new("%y-%m-%d %T.%N").compile();
        b.iter(|| {
            format_date(&mut buf, ts, &format);
            buf.clear();
        });
    });
    c.bench_function("datefmt format [%b %d %T.%N]", |b| {
        let mut buf = Vec::<u8>::with_capacity(4096);
        let format = LinuxDateFormat::new("%b %d %T.%N").compile();
        b.iter(|| {
            format_date(&mut buf, ts, &format);
            buf.clear();
        });
    });
    c.bench_function("calling chrono date-time methods", |b| {
        b.iter(|| {
            assert!(
                tsn.year() as i64
                    + tsn.month() as i64
                    + tsn.day() as i64
                    + tsn.hour() as i64
                    + tsn.minute() as i64
                    + tsn.second() as i64
                    + tsn.nanosecond() as i64
                    != 0
            );
        });
    });
    let items = StrftimeItems::new("%y-%m-%d %T.%3f");
    c.bench_function("chrono format", |b| {
        let mut buf = Vec::<u8>::with_capacity(4096);
        b.iter(|| {
            assert!(write!(&mut buf, "{}", ts.format_with_items(items.clone())).is_ok());
            buf.clear();
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);