// Two ways for a TypeScript client to turn its set into the `fuse8` filter
// value: the Rust builder compiled to WASM, or a TypeScript port. Both take
// values already in the form the column stores (lowercase `0x` hex, base58),
// which is how a client keeps its set anyway.

import { createRequire } from 'node:module'
import { buildFuse8 as fuse8FromKeys } from './fuse8.ts'
import { R } from './u64.ts'
import { xxh3 } from './xxh3.ts'

const require = createRequire(import.meta.url)
export const wasm: {
  buildFuse8(values: Uint8Array, ends: Uint32Array): Uint8Array
  memoryBytes(): number
} = require('./wasm/fuse_build.js')

/** Every value's UTF-8 back to back, and where each one ends. */
export function pack(values: string[]): { bytes: Uint8Array; ends: Uint32Array } {
  const ends = new Uint32Array(values.length)
  let end = 0
  for (let i = 0; i < values.length; i++) {
    end += values[i].length
    ends[i] = end
  }

  // The lengths above are byte lengths, and latin1 is UTF-8, only when every
  // value is ASCII. Latin1 encodes about twice as fast as TextEncoder.
  const joined = values.join('')
  if (Buffer.byteLength(joined, 'utf8') !== end) throw new Error('pack: a value is not ASCII')
  return { bytes: Buffer.from(joined, 'latin1'), ends }
}

export function buildWasm(values: string[]): Uint8Array {
  const { bytes, ends } = pack(values)
  return wasm.buildFuse8(bytes, ends)
}

export function hashTs(values: string[]): { hi: Uint32Array; lo: Uint32Array } {
  const hi = new Uint32Array(values.length)
  const lo = new Uint32Array(values.length)
  for (let i = 0; i < values.length; i++) {
    xxh3(values[i])
    hi[i] = R[0]
    lo[i] = R[1]
  }
  return { hi, lo }
}

export function buildTs(values: string[]): Uint8Array {
  const { hi, lo } = hashTs(values)
  return fuse8FromKeys(hi, lo)
}

export function toBase64(filter: Uint8Array): string {
  return Buffer.from(filter.buffer, filter.byteOffset, filter.length).toString('base64')
}
