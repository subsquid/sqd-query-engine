//! Native baseline: values in, wire bytes out.
//!
//! FUSE_SETS points at the directory `prepare.sh` fills (default `sets`).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fuse_build::{build_fuse8, pack};

fn sets() -> Vec<(String, Vec<String>)> {
    let dir = std::env::var("FUSE_SETS").unwrap_or_else(|_| "sets".into());
    let read = |name: &str| -> Vec<String> {
        std::fs::read_to_string(format!("{dir}/{name}"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    };

    let random = read("random-2m.txt");
    let mut sets: Vec<_> = [10_000, 40_000, 100_000, 1_000_000, 2_000_000]
        .into_iter()
        .map(|n| (format!("random/{n}"), random[..n].to_vec()))
        .collect();
    sets.push(("evm/400000".into(), read("evm-pairs.txt")));
    sets.push(("solana/400000".into(), read("solana-owners.txt")));
    sets
}

fn build(c: &mut Criterion) {
    let mut group = c.benchmark_group("fuse8");
    group.sample_size(10);

    for (name, values) in sets() {
        let (bytes, ends) = pack(values.iter().map(String::as_str));
        group.throughput(Throughput::Elements(values.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(&name),
            &(bytes, ends),
            |b, (bytes, ends)| b.iter(|| build_fuse8(bytes, ends).unwrap()),
        );
    }
    group.finish();
}

criterion_group!(benches, build);
criterion_main!(benches);
