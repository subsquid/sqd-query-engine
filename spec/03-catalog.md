# 3. The catalog

The catalog declares every dataset the engine serves. It is data. Nothing in
this document requires code specific to any chain
([INV-X1](07-invariants.md#inv-x1)).

This chapter is the normative record of what the catalog must contain. An engine
whose catalog omits an entry here does not serve that dataset, however well it
serves the others.

### Conventions

- Filter and relation names are given in the camelCase spelling clients send.
- `→ col` names the column a filter reads, when it differs from the filter name.
- Relation notation `name : kind → table [leftKey ↔ rightKey]`. Where the keys
  are equal on both sides, one list is shown.
- `bn` abbreviates the table's block number column, always the first key column.
- **Selectable fields** are listed per table, in camelCase. The list is the
  normative output surface (§2.3): a name absent from it is `UnknownField`, even
  when a column of that name exists. A catalog usually derives the list from its
  non-`system` columns plus virtual fields plus field-group request keys, but
  where that derivation and the list disagree, the list wins
  ([INV-Q14](07-invariants.md#inv-q14)). A table addressable in `fields` — one
  that declares a `fieldName` — declares the list, and there is no default:
  deriving it is what makes the physical column layout part of the wire contract.
  Columns with non-default encodings are called out separately.

### Compatibility markers

| Marker | Meaning |
|---|---|
| *(none)* | Required. Present in the reference implementation this spec was derived from. |
| **[X]** | Extension. Not in the reference implementation. Permitted; MUST NOT change the meaning of any non-extension construct. |

---

## 3.1 `evm`

Tables, in output order: `blocks`, `transactions`, `logs`, `traces`,
`statediffs`.

| Table | queryName | fieldName | itemOrderKeys | addressColumn |
|---|---|---|---|---|
| `blocks` | `blocks` | `block` | — | — |
| `transactions` | `transactions` | `transaction` | `transactionIndex` | — |
| `logs` | `logs` | `log` | `transactionIndex, logIndex` | — |
| `traces` | `traces` | `trace` | `transactionIndex` | `traceAddress` |
| `statediffs` | `stateDiffs` | `stateDiff` | `transactionIndex, address, key` | — |

Note `statediffs` — the table name, its `queryName` and its `fieldName` all
differ.

### Filters

| Table | Filter | Kind | Column |
|---|---|---|---|
| `transactions` | `from`, `to`, `sighash` | inList (hex) | same |
| | `firstNonce` | rangeGte | `nonce` |
| | `lastNonce` | rangeLte | `nonce` |
| `logs` | `address`, `topic0`, `topic1`, `topic2`, `topic3` | inList (hex) | same |
| `traces` | `type` | inList (exact) | `type` |
| | `createFrom`, `createResultAddress` | inList (hex) | same |
| | `callFrom`, `callTo`, `callSighash` | inList (hex) | same |
| | `callCallType` | inList (exact), columnAlias | `callType` |
| | `suicideAddress`, `suicideRefundAddress` | inList (hex) | same |
| | `rewardAuthor` | inList (hex) | same |
| | `createValueNonZero` | gteConst `"0x1"` | `createValue` |
| | `callValueNonZero` | gteConst `"0x1"` | `callValue` |
| | `suicideBalanceNonZero` | gteConst `"0x1"` | `suicideBalance` |
| | `rewardValueNonZero` | gteConst `"0x1"` | `rewardValue` |
| `statediffs` | `address` | inList (hex) | same |
| | `key`, `kind` | inList (**exact**, not case-folded) | same |

`traces.type` and `traces.callCallType` are compared exactly. They hold
identifiers (`call`, `create`, `delegatecall`), not hex.

### Relations

| Table | Relation | Definition |
|---|---|---|
| `transactions` | `logs` | join → `logs` [bn, transactionIndex] |
| | `traces` | join → `traces` [bn, transactionIndex] |
| | `stateDiffs` | join → `statediffs` [bn, transactionIndex] |
| `logs` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionLogs` | join → `logs` [bn, transactionIndex] |
| | `transactionTraces` | join → `traces` [bn, transactionIndex] |
| | `transactionStateDiffs` | join → `statediffs` [bn, transactionIndex] |
| `traces` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionLogs` | join → `logs` [bn, transactionIndex] |
| | `subtraces` | children → `traces`, group [bn, transactionIndex], address `traceAddress` |
| | `parents` | parents → `traces`, group [bn, transactionIndex], address `traceAddress` |
| `statediffs` | `transaction` | join → `transactions` [bn, transactionIndex] |

### Virtual fields

`logs.topics` — roll of `topic0, topic1, topic2, topic3`.

### Field groups — `traces`

Tag column `type`. Base fields: `transactionIndex`, `traceAddress`, `type`,
`subtraces`, `error`, `revertReason`.

| Tag | Group | Request key → output path |
|---|---|---|
| `create` | `action` | `createFrom`→`from`, `createValue`→`value`, `createGas`→`gas`, `createInit`→`init` |
| | `result` | `createResultGasUsed`→`gasUsed`, `createResultCode`→`code`, `createResultAddress`→`address` |
| `call` | `action` | `callFrom`→`from`, `callTo`→`to`, `callValue`→`value`, `callGas`→`gas`, `callInput`→`input`, `callSighash`→`sighash`, `callType`→`type`, `callCallType`→`callType` |
| | `result` | `callResultGasUsed`→`gasUsed`, `callResultOutput`→`output` |
| `suicide` | `action` | `suicideAddress`→`address`, `suicideRefundAddress`→`refundAddress`, `suicideBalance`→`balance` |
| `reward` | `action` | `rewardAuthor`→`author`, `rewardValue`→`value`, `rewardType`→`type` |

Both `callType` and `callCallType` read the column `callType`. They emit to
different output keys: `action.type` and `action.callType` respectively. This is
not a mistake; both keys exist in the wire format.

### Notable encodings

`blocks.timestamp`: `timestampSecond`. `blocks.logsBloom`: weight 512.
`transactions.authorizationList`: `list<struct>` whose `nonce` member renders as
`decimalString`. All address/hash/data columns: `hexBytes`.

### Selectable fields

`blocks`: `number, hash, parentHash, timestamp, transactionsRoot, receiptsRoot,
stateRoot, logsBloom, sha3Uncles, extraData, miner, nonce, mixHash, size,
gasLimit, gasUsed, difficulty, totalDifficulty, baseFeePerGas, uncles,
withdrawals, withdrawalsRoot, blobGasUsed, excessBlobGas, parentBeaconBlockRoot,
requestsHash, l1BlockNumber, mainBlockGeneralGasLimit, sharedGasLimit,
timestampMillisPart`

`transactions`: `transactionIndex, hash, nonce, from, to, input, value, gas,
gasPrice, maxFeePerGas, maxPriorityFeePerGas, v, r, s, yParity, accessList,
chainId, sighash, contractAddress, gasUsed, logsBloom, cumulativeGasUsed,
effectiveGasPrice, type, status, blobGasUsed, blobGasPrice, maxFeePerBlobGas,
blobVersionedHashes, authorizationList, calls, nonceKey, signature, feeToken,
feePayerV, feePayerR, feePayerS, validBefore, validAfter, aaAuthorizationList,
keyAuthorization, l1Fee, l1FeeScalar, l1GasPrice, l1GasUsed, l1BlobBaseFee,
l1BlobBaseFeeScalar, l1BaseFeeScalar`

`logs`: `logIndex, transactionIndex, transactionHash, address, data, topics`

`traces`: the field-group request keys above.

`statediffs`: `transactionIndex, address, key, kind, prev, next`

> The `evm` dataset also serves chains whose block or transaction shape extends
> Ethereum's (Optimism's L1 fee fields, Tempo's fee-payer and authorization
> fields). These are nullable columns of the same tables, not new datasets. A
> chain is not a dataset; a *shape* is.

---

## 3.2 `solana`

Tables: `blocks`, `transactions`, `instructions`, `logs`, `balances`,
`token_balances`, `rewards`.

| Table | queryName | fieldName | itemOrderKeys | addressColumn |
|---|---|---|---|---|
| `blocks` | `blocks` | `block` | — | — |
| `transactions` | `transactions` | `transaction` | `transactionIndex` | — |
| `instructions` | `instructions` | `instruction` | `transactionIndex` | `instructionAddress` |
| `logs` | `logs` | `log` | `transactionIndex, logIndex` | — |
| `balances` | `balances` | `balance` | `transactionIndex, account` | — |
| `token_balances` | `tokenBalances` | `tokenBalance` | `transactionIndex, account` | — |
| `rewards` | `rewards` | `reward` | `pubkey, rewardType` | — |

### Filters

| Table | Filter | Kind | Column |
|---|---|---|---|
| `transactions` | `feePayer` | inList (exact) | `feePayer` |
| | `mentionsAccount` | bloom (64 bytes, 7 hashes), ≤ `P-MAX-BLOOM-VALUES` | `accountsBloom` |
| `instructions` | `programId` | inList (exact) | same |
| | `discriminator` | discriminator → `d1`…`d16` | — |
| | `d1`, `d2`, `d4`, `d8` | inList (hex, fixed byte length 1/2/4/8) | same |
| | `a0` … `a15` | inList (exact) | same |
| | `isCommitted` | equals (boolean) | same |
| | `mentionsAccount` | bloom, ≤ `P-MAX-BLOOM-VALUES` | `accountsBloom` |
| `logs` | `programId`, `kind` | inList (exact) | same |
| `balances` | `account` | inList (exact) | same |
| `token_balances` | `account`, `preMint`, `postMint`, `preProgramId`, `postProgramId`, `preOwner`, `postOwner` | inList (exact) | same |
| `rewards` | `pubkey` | inList (exact) | same |

At most one of `discriminator`, `d1`, `d2`, `d4`, `d8` per item request
([INV-Q11](07-invariants.md#inv-q11)).

Solana values are base58 or raw hex. **Nothing is case-folded.**

### Relations

| Table | Relation | Definition |
|---|---|---|
| `transactions` | `instructions` | join → `instructions` [bn, transactionIndex] |
| | `logs` | join → `logs` [bn, transactionIndex] |
| | `balances` | join → `balances` [bn, transactionIndex] |
| | `tokenBalances` | join → `token_balances` [bn, transactionIndex] |
| `instructions` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionInstructions` | join → `instructions` [bn, transactionIndex] |
| | `transactionBalances` | join → `balances` [bn, transactionIndex] |
| | `transactionTokenBalances` | join → `token_balances` [bn, transactionIndex] |
| | `logs` | join → `logs` [bn, transactionIndex, instructionAddress] |
| | `innerInstructions` | children → `instructions`, group [bn, transactionIndex] |
| | `parentInstructions` | parents → `instructions`, group [bn, transactionIndex] |
| `logs` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `instruction` | join → `instructions` [bn, transactionIndex, instructionAddress] |
| `balances` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionInstructions` | join → `instructions` [bn, transactionIndex] |
| `token_balances` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionInstructions` | join → `instructions` [bn, transactionIndex] |
| | `transactionBalances` | join → `balances` [bn, transactionIndex] |
| | `transactionTokenBalances` | join → `token_balances` [bn, transactionIndex] |
| `rewards` | *(none)* | |

### Virtual fields

`instructions.accounts` — roll of `a0 … a15, restAccounts`. The trailing list is
spread.

### Notable encodings

`transactions.version`: `-1` renders as `"legacy"`, otherwise the number.
`transactions.err`, `instructions.error`: `jsonVerbatim`.
`transactions.fee`, `computeUnitsConsumed`, `balances.pre`/`post`,
`token_balances.preAmount`/`postAmount`, `rewards.lamports`/`postBalance`:
`decimalString`. `blocks.timestamp`: `timestampSecond`.

**`instructions.d1`, `d2`, `d4`, `d8` are selectable output fields** encoded as
`hexNumber` — zero-padded to the column's width, so a `uint16` `d2` of 1600
renders as `"0x0640"`. They are simultaneously filter columns. A column being
used by a filter does not make it a `system` column.

### Selectable fields

`blocks`: `number, hash, parentNumber, parentHash, height, timestamp`

`transactions`: `transactionIndex, version, accountKeys, addressTableLookups,
numReadonlySignedAccounts, numReadonlyUnsignedAccounts, numRequiredSignatures,
recentBlockhash, signatures, err, fee, computeUnitsConsumed, loadedAddresses,
feePayer, hasDroppedLogMessages`

`instructions`: `transactionIndex, instructionAddress, programId, accounts, data,
d1, d2, d4, d8, error, computeUnitsConsumed, isCommitted, hasDroppedLogMessages`

`logs`: `transactionIndex, logIndex, instructionAddress, programId, kind, message`

`balances`: `transactionIndex, account, pre, post`

`token_balances`: `transactionIndex, account, preMint, postMint, preDecimals,
postDecimals, preProgramId, postProgramId, preOwner, postOwner, preAmount,
postAmount`

`rewards`: `pubkey, lamports, postBalance, rewardType, commission`

---

## 3.3 `substrate`

Tables: `blocks`, `extrinsics`, `calls`, `events`.

| Table | queryName | fieldName | itemOrderKeys | addressColumn |
|---|---|---|---|---|
| `blocks` | `blocks` | `block` | — | — |
| `extrinsics` | `extrinsics` **[X]** | `extrinsic` | `index` | — |
| `calls` | `calls` | `call` | `extrinsicIndex` | `address` |
| `events` | `events` | `event` | `index` | `callAddress` |

The reference implementation exposes no item request array for `extrinsics`;
extrinsics reach the response only through relations. Exposing one is a
compatible extension.

### Filters

| Table | Filter | Kind |
|---|---|---|
| `calls` | `name` | inList (exact) |
| `events` | `name` | inList (exact) |

### Relations

| Table | Relation | Definition |
|---|---|---|
| `extrinsics` | `events` **[X]** | join → `events` [bn, index ↔ bn, extrinsicIndex] |
| `calls` | `extrinsic` | join → `extrinsics` [bn, extrinsicIndex ↔ bn, index] |
| | `subcalls` | children → `calls`, group [bn, extrinsicIndex], address `address` |
| | `stack` | parents → `calls`, group [bn, extrinsicIndex], address `address` |
| | `events` | children → `events`, group [bn, extrinsicIndex], source address `address`, target address `callAddress` |
| `events` | `extrinsic` | join → `extrinsics` [bn, extrinsicIndex ↔ bn, index] |
| | `call` | join → `calls` [bn, extrinsicIndex, callAddress ↔ bn, extrinsicIndex, address] |
| | `stack` | parents → `calls`, group [bn, extrinsicIndex], source address `callAddress`, target address `address` |

`calls.events` and `events.stack` are **cross-table** hierarchies: source and
target address columns differ, so equal-depth addresses match as well as strict
prefixes ([INV-R8](07-invariants.md#inv-r8)). An event whose `callAddress` equals
a call's `address` belongs to that call.

### Aliases

Each alias targets `calls` or `events`, adds an implicit `name` predicate, and
remaps its filters onto extraction columns.

| Alias | Table | Implicit predicate | Filters → column | Relations |
|---|---|---|---|---|
| `evmLogs` | `events` | `name = "EVM.Log"` | `address`→`_evmLogAddress`, `topic0…3`→`_evmLogTopic0…3` (hex, case-folded) | `extrinsic`, `call`, `stack` |
| `ethereumTransactions` | `calls` | `name = "Ethereum.transact"` | `to`→`_ethereumTransactTo`, `sighash`→`_ethereumTransactSighash` (hex, case-folded) | `extrinsic`, `stack`, `events` |
| `contractsEvents` | `events` | `name = "Contracts.ContractEmitted"` | `contractAddress`→`_contractAddress` (exact) | `extrinsic`, `call`, `stack` |
| `gearMessagesEnqueued` | `events` | `name = "Gear.UserMessageEnqueued"` | `programId`→`_gearProgramId` (exact) | `extrinsic`, `call`, `stack` |
| `gearUserMessagesSent` | `events` | `name = "Gear.UserMessageSent"` | `programId`→`_gearProgramId` (exact) | `extrinsic`, `call`, `stack` |
| `reviveContractEmitted` | `events` | `name = "Revive.ContractEmitted"` | `contract`→`_reviveContract`, `topic0…3`→`_reviveTopic0…3` (exact) | `extrinsic`, `call`, `stack` |

The `_`-prefixed columns are `system`: they exist to serve these aliases and are
never emitted.

### Notable encodings

`blocks.timestamp`: `timestampMillisecond`. `blocks.digest`, `extrinsics.signature`,
`extrinsics.error`, `calls.args`, `calls.origin`, `calls.error`, `events.args`:
`jsonVerbatim`. `extrinsics.fee`, `extrinsics.tip`: `decimalString`.

### Selectable fields

`blocks`: `number, hash, parentHash, stateRoot, extrinsicsRoot, digest, specName,
specVersion, implName, implVersion, validator, timestamp`

`extrinsics`: `index, version, success, hash, fee, tip, signature, error`

`calls`: `extrinsicIndex, address, name, success, args, origin, error`

`events`: `index, extrinsicIndex, name, phase, callAddress, topics, args`

Each selected field appears **exactly once** in an item object
([INV-O7](07-invariants.md#inv-o7)).

---

## 3.4 `tron`

Tables: `blocks`, `transactions`, `logs`, `internal_transactions`.

| Table | queryName | fieldName | itemOrderKeys |
|---|---|---|---|
| `blocks` | `blocks` | `block` | — |
| `transactions` | `transactions` | `transaction` | `transactionIndex` |
| `logs` | `logs` | `log` | `transactionIndex, logIndex` |
| `internal_transactions` | `internalTransactions` | `internalTransaction` | `transactionIndex, internalTransactionIndex` |

### Filters

| Table | Filter | Kind |
|---|---|---|
| `transactions` | `type` | inList (exact) |
| `logs` | `address`, `topic0…3` | inList (hex, case-folded) |
| `internal_transactions` | `caller` → `callerAddress`, `transferTo` → `transferToAddress` | inList (hex, case-folded) |

### Relations

| Table | Relation | Definition |
|---|---|---|
| `transactions` | `logs` | join → `logs` [bn, transactionIndex] |
| | `internalTransactions` | join → `internal_transactions` [bn, transactionIndex] |
| `logs` | `transaction` | join → `transactions` [bn, transactionIndex] |
| `internal_transactions` | `transaction` | join → `transactions` [bn, transactionIndex] |

### Aliases

| Alias | Table | Implicit predicate | Filters → column | Relations |
|---|---|---|---|---|
| `transferTransactions` | `transactions` | `type = "TransferContract"` | `owner`→`_transferContractOwner`, `to`→`_transferContractTo` (hex, case-folded) | `logs`, `internalTransactions` |
| `transferAssetTransactions` | `transactions` | `type = "TransferAssetContract"` | `owner`→`_transferAssetContractOwner`, `to`→`_transferAssetContractTo` (hex, case-folded), `asset`→`_transferAssetContractAsset` (exact) | `logs`, `internalTransactions` |
| `triggerSmartContractTransactions` | `transactions` | `type = "TriggerSmartContract"` | `owner`→`_triggerSmartContractOwner`, `contract`→`_triggerSmartContractContract`, `sighash`→`_triggerSmartContractSighash` (hex, case-folded) | `logs`, `internalTransactions` |

### Virtual fields

`logs.topics` — roll of `topic0…topic3`.

### Notable encodings

`blocks.timestamp`, `transactions.expiration`, `transactions.timestamp`:
`timestampMillisecond`. `transactions.ret`, `parameter`,
`cancelUnfreezeV2Amount`, `internal_transactions.callValueInfo`:
`jsonVerbatim`. `transactions.feeLimit`, `fee`, `withdrawAmount`,
`unfreezeAmount`, `withdrawExpireAmount`, `energyFee`, `energyUsage`,
`energyUsageTotal`, `netUsage`, `netFee`, `originEnergyUsage`,
`energyPenaltyTotal`: `decimalString`.

Tron writes hex with **no** `0x` prefix — sighashes are 8 bare digits, addresses
40 or 42. Those columns are `utf8` by §1.5 and render verbatim; the "hex,
case-folded" filters above fold on a rule the catalog declares per column
([INV-P8](07-invariants.md#inv-p8)).

`internal_transactions.extra` is one of them: `utf8`, not `jsonVerbatim`. The
archive writes it through the same builder as `callValueInfo`, which makes it
look like JSON, but it stores hex bytes verbatim. Declaring it `jsonVerbatim`
splices a bare `a1b2c3d4` into the response and the whole line stops parsing.

### Selectable fields

`blocks`: `number, hash, parentHash, txTrieRoot, version, timestamp,
witnessAddress, witnessSignature`

`transactions`: `transactionIndex, hash, ret, signature, type, parameter,
permissionId, refBlockBytes, refBlockHash, feeLimit, expiration, timestamp,
rawDataHex, fee, contractResult, contractAddress, resMessage, withdrawAmount,
unfreezeAmount, withdrawExpireAmount, cancelUnfreezeV2Amount, result, energyFee,
energyUsage, energyUsageTotal, netUsage, netFee, originEnergyUsage,
energyPenaltyTotal`

`logs`: `transactionIndex, logIndex, address, data, topics`

`internal_transactions`: `transactionIndex, internalTransactionIndex, hash,
callerAddress, transferToAddress, callValueInfo, note, rejected, extra`

The `_`-prefixed columns backing the three aliases are `system` and never
emitted.

---

## 3.5 `bitcoin`

Tables: `blocks`, `transactions`, `inputs`, `outputs`.

| Table | queryName | fieldName | itemOrderKeys |
|---|---|---|---|
| `blocks` | `blocks` | `block` | — |
| `transactions` | `transactions` | `transaction` | `transactionIndex` |
| `inputs` | `inputs` | `input` | `transactionIndex, inputIndex` |
| `outputs` | `outputs` | `output` | `transactionIndex, outputIndex` |

### Filters

| Table | Filter | Kind |
|---|---|---|
| `transactions` | *(none)* | |
| `inputs` | `type`, `prevoutScriptPubKeyAddress`, `prevoutScriptPubKeyType` | inList (exact) |
| | `prevoutGenerated` | equals (boolean) |
| `outputs` | `scriptPubKeyAddress`, `scriptPubKeyType` | inList (exact) |

`transactions` declares no filters. The only well-formed item request on it is
`{}` — possibly carrying relation flags.

### Relations

| Table | Relation | Definition |
|---|---|---|
| `transactions` | `inputs`, `outputs` | join → resp. [bn, transactionIndex] |
| `inputs` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionInputs`, `transactionOutputs` | join → resp. [bn, transactionIndex] |
| `outputs` | `transaction` | join → `transactions` [bn, transactionIndex] |
| | `transactionInputs`, `transactionOutputs` | join → resp. [bn, transactionIndex] |

### Notable encodings

`blocks.timestamp`, `blocks.medianTime`: `timestampSecond`.

### Selectable fields

`blocks`: `number, hash, parentHash, timestamp, medianTime, version, merkleRoot,
nonce, target, bits, difficulty, chainWork, strippedSize, size, weight`

`transactions`: `transactionIndex, hex, txid, hash, size, vsize, weight, version,
locktime`

`inputs`: `transactionIndex, inputIndex, type, txid, vout, scriptSigHex,
scriptSigAsm, sequence, coinbase, txInWitness, prevoutGenerated, prevoutHeight,
prevoutValue, prevoutScriptPubKeyHex, prevoutScriptPubKeyAsm,
prevoutScriptPubKeyDesc, prevoutScriptPubKeyType, prevoutScriptPubKeyAddress`

`outputs`: `transactionIndex, outputIndex, value, scriptPubKeyHex,
scriptPubKeyAsm, scriptPubKeyDesc, scriptPubKeyType, scriptPubKeyAddress`

---

## 3.6 `fuel` — not served

The engine does not serve `fuel`. The dataset is out of scope
([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)): it is not being ported from
the reference implementation, and no conforming engine is required to answer a
`{"type": "fuel"}` query.

The section number is kept so that §3.7 and §3.8 do not move.

An engine that ships a `fuel` catalog anyway is serving a dataset this document
does not describe. Nothing above constrains it, and nothing below may depend on
it.

---

## 3.7 `hyperliquidFills`

Tables: `blocks`, `fills`.

| Table | queryName | fieldName | itemOrderKeys |
|---|---|---|---|
| `blocks` | `blocks` | `block` | — |
| `fills` | `fills` | `fill` | `fillIndex` |

### Filters

`fills`: `user`, `coin`, `dir`, `cloid`, `feeToken`, `builder` — all inList,
exact.

No relations.

### Notable encodings

`blocks.timestamp`, `fills.time`: `timestampMillisecond`.

### Selectable fields

`blocks`: `number, hash, parentHash, timestamp`

`fills`: `fillIndex, user, coin, px, sz, side, time, startPosition, dir,
closedPnl, hash, oid, crossed, fee, builderFee, tid, cloid, feeToken, builder,
twapId`

---

## 3.8 `hyperliquidReplicaCmds`

Tables: `blocks`, `actions`.

| Table | queryName | fieldName | itemOrderKeys |
|---|---|---|---|
| `blocks` | `blocks` | `block` | — |
| `actions` | `actions` | `action` | `actionIndex` |

### Filters — `actions`

| Filter | Kind |
|---|---|
| `actionType` | inList (exact) |
| `user`, `vaultAddress` | inList (exact) |
| `status` | equals; one of `"ok"`, `"err"` |

`status` is a scalar, not a list.

No relations.

### Aliases

Each targets `actions`, adds an implicit `actionType` predicate, and exposes
`containsAsset` / `containsCloid` over the action-shape-specific list columns.

| Alias | Implicit predicate | `containsAsset` → | `containsCloid` → |
|---|---|---|---|
| `orderActions` | `actionType = "order"` | `orderAsset` | `orderCloid` |
| `cancelActions` | `actionType = "cancel"` | `cancelAsset` | — |
| `cancelByCloidActions` | `actionType = "cancelByCloid"` | `cancelByCloidAsset` | `cancelByCloidCloid` |
| `batchModifyActions` | `actionType = "batchModify"` | `batchModifyAsset` | `batchModifyCloid` |

Each also accepts `user`, `vaultAddress`, `status`.

`containsAsset` and `containsCloid` are `listContainsAny` filters. The target
columns are `system` and never emitted.

Item requests through aliases count toward `P-MAX-ITEM-REQUESTS` exactly like any
other ([INV-Q5](07-invariants.md#inv-q5)).

### Notable encodings

`blocks.timestamp`: `timestampMillisecond`. `blocks.hardfork`, `actions.action`,
`actions.signature`, `actions.response`: `jsonVerbatim`.

### Selectable fields

`blocks`: `number, hash, parentHash, round, parentRound, proposer, timestamp,
hardfork`

`actions`: `actionIndex, user, action, signature, nonce, vaultAddress, status,
response`
