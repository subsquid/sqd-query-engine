// Warm build time: a set of values in, the base64 filter value out.
// SETS points at the value sets (default ../sets); BENCH_SETS narrows the run
// to set names that contain one of its comma-separated parts.

import { readFileSync } from 'node:fs'
import { Bench } from 'tinybench'
import { buildTs, buildWasm, hashTs, pack, toBase64, wasm } from './build.ts'
import { buildFuse8 as fuse8FromKeys } from './fuse8.ts'

const sets = process.env.SETS ?? new URL('../sets', import.meta.url).pathname
// Through JSON, so the strings are flat like the ones a client parses from a
// response; slices of one file hash about 13% slower.
const read = (name: string): string[] =>
  JSON.parse(JSON.stringify(readFileSync(`${sets}/${name}.txt`, 'utf8').split('\n').filter(Boolean)))

const random = read('random-2m')
const all: [string, string[]][] = [
  ...[10_000, 40_000, 100_000].map((n) => [`random/${n}`, random.slice(0, n)] as [string, string[]]),
  ['evm/400000', read('evm-pairs')],
  ['solana/400000', read('solana-owners')],
  ['random/1000000', random.slice(0, 1_000_000)],
  ['random/2000000', random],
]
const only = process.env.BENCH_SETS?.split(',')
const chosen = all.filter(([name]) => !only || only.some((part) => name.includes(part)))

const bench = new Bench({ time: 1000, iterations: 5, warmupTime: 300, warmupIterations: 2 })
for (const [name, values] of chosen) {
  bench.add(`wasm ${name}`, () => toBase64(buildWasm(values)))
  bench.add(`ts ${name}`, () => toBase64(buildTs(values)))
}

// Where the time goes, on the 400k Ethereum set.
const evm = all.find(([name]) => name === 'evm/400000')![1]
if (chosen.some(([name]) => name === 'evm/400000')) {
  const packed = pack(evm)
  const keys = hashTs(evm)
  const filter = buildWasm(evm)
  bench.add('part pack evm/400000', () => pack(evm))
  bench.add('part wasm-call evm/400000', () => wasm.buildFuse8(packed.bytes, packed.ends))
  bench.add('part ts-hash evm/400000', () => hashTs(evm))
  bench.add('part ts-construct evm/400000', () => fuse8FromKeys(keys.hi, keys.lo))
  bench.add('part base64 evm/400000', () => toBase64(filter))
}

await bench.run()

const sizes = new Map(all.map(([name, values]) => [name, values.length]))
console.log(`node ${process.version}, ${process.arch}`)
console.log('| task | p50 ms | mean ms | ±% | ns per value | samples |')
console.log('|---|---:|---:|---:|---:|---:|')
for (const task of bench.tasks) {
  const result = task.result as any
  const n = sizes.get(task.name.split(' ').at(-1)!)!
  const latency = result.latency
  console.log(
    `| ${task.name} | ${latency.p50.toFixed(2)} | ${latency.mean.toFixed(2)} | ${latency.rme.toFixed(1)} | ${((latency.p50 * 1e6) / n).toFixed(0)} | ${latency.samplesCount} |`,
  )
}
