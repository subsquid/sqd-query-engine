#!/bin/sh
# Builds the WASM module, and writes the sets and the native reference outputs
# the TypeScript side checks itself against. SQD_MEMBERSHIP_DIR holds
# evm-pairs.txt and solana-owners.txt, which membership_wall derives on its
# first run.
set -eu

members=$(cd "${SQD_MEMBERSHIP_DIR:?point it at the membership_wall sets}" && pwd)
cd "$(dirname "$0")"
mkdir -p sets ref
cp "$members/evm-pairs.txt" "$members/solana-owners.txt" sets/

cargo build --release --bin fuse8
fuse8=target/release/fuse8
$fuse8 random 2000000 > sets/random-2m.txt
$fuse8 vectors > sets/xxh3-vectors.tsv
$fuse8 sweep sets/random-2m.txt 3000 > sets/sweep-3000.tsv
for n in 10000 40000 100000 1000000; do
  head -n $n sets/random-2m.txt > sets/random-$n.txt
done
for set in evm-pairs solana-owners random-10000 random-40000 random-100000 random-1000000 random-2m; do
  $fuse8 build sets/$set.txt ref/$set.fuse8
done

rustup target add wasm32-unknown-unknown
wasm-pack build --release --target nodejs --out-dir ts/wasm --no-pack
echo '{"type":"commonjs"}' > ts/wasm/package.json
(cd ts && npm ci)
