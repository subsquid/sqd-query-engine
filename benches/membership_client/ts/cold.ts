// The first build in a fresh process, as a client meets it: module load, the
// first call (unoptimised code), the second, and for WASM the linear memory the
// process keeps afterwards.
// Usage: node cold.ts <wasm|ts> <set file name, without .txt> [first n values]

import { readFileSync } from 'node:fs'

const [builder, setName, limit] = process.argv.slice(2)
const sets = process.env.SETS ?? new URL('../sets', import.meta.url).pathname
let values: string[] = JSON.parse(
  JSON.stringify(readFileSync(`${sets}/${setName}.txt`, 'utf8').split('\n').filter(Boolean)),
)
if (limit) values = values.slice(0, Number(limit))

let start = performance.now()
const { buildTs, buildWasm, toBase64, wasm } = await import('./build.ts')
const load = performance.now() - start
const build = builder === 'wasm' ? buildWasm : buildTs

start = performance.now()
const base64Bytes = toBase64(build(values)).length
const firstMs = performance.now() - start

start = performance.now()
toBase64(build(values))
const secondMs = performance.now() - start

console.log(
  JSON.stringify({
    builder,
    set: setName,
    values: values.length,
    loadMs: +load.toFixed(1),
    firstMs: +firstMs.toFixed(1),
    secondMs: +secondMs.toFixed(1),
    base64Bytes,
    wasmMemoryMb: builder === 'wasm' ? +(wasm.memoryBytes() / 2 ** 20).toFixed(1) : undefined,
  }),
)
