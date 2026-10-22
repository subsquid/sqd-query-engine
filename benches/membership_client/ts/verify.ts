// Both builders against the native Rust output: every xxh3 vector, and the
// filter bytes for every set. SETS and REF default to ../sets and ../ref.

import { readFileSync } from 'node:fs'
import { buildTs, buildWasm } from './build.ts'
import { R, hex64 } from './u64.ts'
import { xxh3 } from './xxh3.ts'

const sets = process.env.SETS ?? new URL('../sets', import.meta.url).pathname
const ref = process.env.REF ?? new URL('../ref', import.meta.url).pathname

let failures = 0

const vectors = readFileSync(`${sets}/xxh3-vectors.tsv`, 'utf8').split('\n').filter(Boolean)
for (const line of vectors) {
  const [text, expected] = line.split('\t')
  xxh3(text)
  const actual = hex64(R[0], R[1])
  if (actual !== expected) {
    failures++
    if (failures < 10) console.log(`xxh3 len ${text.length}: ${actual} != ${expected}`)
  }
}
console.log(`xxh3: ${vectors.length} vectors, ${failures} wrong`)

const read = (name: string) => readFileSync(`${sets}/${name}.txt`, 'utf8').split('\n').filter(Boolean)
const cases: [string, string[]][] = [
  ['evm-pairs', read('evm-pairs')],
  ['solana-owners', read('solana-owners')],
]
const random = read('random-2m')
for (const n of [10_000, 40_000, 100_000, 1_000_000]) cases.push([`random-${n}`, random.slice(0, n)])
cases.push(['random-2m', random])

// Every size up to 3000: retries and the halved segment layout happen here.
const fnv = (bytes: Uint8Array) => {
  let h = 0x811c9dc5
  for (const b of bytes) h = Math.imul(h ^ b, 0x01000193) >>> 0
  return h.toString(16).padStart(8, '0')
}
const sweep = readFileSync(`${sets}/sweep-3000.tsv`, 'utf8').split('\n').filter(Boolean)
const seeds = new Set<string>()
for (const builder of [buildWasm, buildTs]) {
  let wrong = 0
  for (const line of sweep) {
    const [n, descriptor, digest] = line.split('\t')
    const filter = builder(random.slice(0, Number(n)))
    const actual = `${Buffer.from(filter.subarray(0, 20)).toString('hex')}\t${fnv(filter)}`
    if (actual !== `${descriptor}\t${digest}`) wrong++
    seeds.add(descriptor.slice(0, 16))
  }
  failures += wrong
  console.log(`sweep 0..3000 ${builder === buildWasm ? 'wasm' : 'ts'}: ${wrong} sizes differ`)
}
console.log(`sweep: ${seeds.size} distinct seeds, so ${seeds.size - 1} retry attempts covered`)

for (const [name, values] of cases) {
  const expected = readFileSync(`${ref}/${name}.fuse8`)
  for (const [builder, build] of [['wasm', buildWasm], ['ts', buildTs]] as const) {
    const same = Buffer.from(build(values)).equals(expected)
    if (!same) failures++
    console.log(`${name} ${builder}: ${same ? 'same bytes' : 'DIFFERENT'} (${expected.length} B)`)
  }
}

process.exit(failures === 0 ? 0 : 1)
