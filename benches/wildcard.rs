// third-party imports
use criterion::{criterion_group, criterion_main, Criterion};
use wildmatch::WildMatch;

fn criterion_benchmark(c: &mut Criterion) {
    let pattern = WildMatch::new(r"_*");
    let prefix = String::from("_");

    c.bench_function("wild-short-match", |b| {
        b.iter(|| {
            assert_eq!(pattern.matches("_TEST"), true);
        });
    });
    c.bench_function("wild-long-match", |b| {
        b.iter(|| {
            assert_eq!(pattern.matches("_TEST_SOME_VERY_VERY_LONG_NAME"), true);
        });
    });
    c.bench_function("wild-short-non-match", |b| {
        b.iter(|| {
            assert_eq!(pattern.matches("TEST"), false);
        });
    });
    c.bench_function("wild-long-non-match", |b| {
        b.iter(|| {
            assert_eq!(pattern.matches("TEST_SOME_VERY_VERY_LONG_NAME"), false);
        });
    });
    c.bench_function("compare-short-match", |b| {
        let what = String::from("_TEST");
        b.iter(|| {
            assert_eq!(what.starts_with(&prefix), true);
        });
    });
    c.bench_function("compare-long-match", |b| {
        let what = String::from("_TEST_SOME_VERY_VERY_LONG_NAME");
        b.iter(|| {
            assert_eq!(what.starts_with(&prefix), true);
        });
    });
    c.bench_function("compare-short-non-match", |b| {
        let what = String::from("TEST");
        b.iter(|| {
            assert_eq!(what.starts_with(&prefix), false);
        });
    });
    c.bench_function("compare-long-non-match", |b| {
        let what = String::from("TEST_SOME_VERY_VERY_LONG_NAME");
        b.iter(|| {
            assert_eq!(what.starts_with(&prefix), false);
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
