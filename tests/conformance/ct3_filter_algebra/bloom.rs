//! The bloom the engine builds is the bloom the archive writer wrote.
//!
//! Every other test in this suite watches the engine's *answers*. This one
//! cannot: a bloom over-approximates, so a wrong construction shows up as rows
//! that quietly stop being returned, and there is no response a client can
//! compare against to notice. A filter one bit narrower than the writer's
//! answers 200 with fewer rows, forever.
//!
//! So the assertion has to be about the bits, and the only artifact of the
//! writer's construction is a chunk it wrote. Three of those rows are frozen
//! below with the accounts they were built from, which is what makes the
//! comparison something the per-PR gate can run; the sweeps repeat it over every
//! row of a chunk where one is checked out.

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray, StructArray};
use arrow::record_batch::RecordBatch;
use sqd_query_engine::metadata::SpecialFilter;
use sqd_query_engine::scan::predicate::bloom_bit;
use std::collections::BTreeSet;

use crate::harness::chunk::read_columns;
use crate::harness::fixtures::{fixture_chunk, fixture_tree_has, meta, run};
use crate::harness::json::items_of;

/// What the catalog declares for `mentionsAccount`, asserted against it below so
/// the constants here cannot drift into being a second, private truth.
const NUM_BYTES: usize = 64;
const NUM_HASHES: usize = 7;

/// A filter built the way the archive writer builds one: `NUM_HASHES` bits per
/// value, through the engine's own hash.
struct Bloom {
    bytes: Vec<u8>,
}

impl Bloom {
    fn new() -> Self {
        Self {
            bytes: vec![0; NUM_BYTES],
        }
    }

    fn insert(&mut self, value: &str) {
        self.insert_upto(value, NUM_HASHES);
    }

    /// Set the bits of a value's first `hashes` hashes and no more. Only one
    /// test wants a partial insertion, and it is the one that tells the counts
    /// apart.
    fn insert_upto(&mut self, value: &str, hashes: usize) {
        for n in 0..hashes {
            let bit = self.bit_of(value, n);
            self.bytes[bit / 8] |= 1 << (bit % 8);
        }
    }

    fn holds(&self, value: &str, n: usize) -> bool {
        let bit = self.bit_of(value, n);

        self.bytes[bit / 8] & (1 << (bit % 8)) != 0
    }

    fn bit_of(&self, value: &str, n: usize) -> usize {
        bloom_bit(value.as_bytes(), n, self.bytes.len() * 8)
    }

    fn of<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
        let mut bloom = Bloom::new();
        for value in values {
            bloom.insert(value);
        }
        bloom.bytes
    }
}

/// A row an archiver wrote: the accounts it was given, and the filter it
/// produced from them.
struct Vector {
    what: &'static str,
    accounts: &'static [&'static str],
    bloom: &'static str,
}

/// Read off an archiver's own chunk, one per shape the two writers assemble
/// their account set from. They are data, not expectations: nothing here
/// recomputes them, which is the point — an engine that changed its hash would
/// change what it recomputes them to.
const VECTORS: &[Vector] = &[
    // Four accounts and no collisions: 28 of the 512 bits are set, so every one
    // of the 28 is a separate statement about where the hash puts a value.
    Vector {
        what: "a transaction whose accounts are all in the message",
        accounts: &[
            "12fHY8e7o9ssuQi4jeqE5NjYbYFyBBj2JZpeZpHQmwxa",
            "69SZpswdovkijfXAvwqB9UrzKRcUHPRgepCRjZtnBTBW",
            "Gu4nCCfSRL17RXpAf5noQGzGu1e2HXPEFjsA5AYgKhyy",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        ],
        bloom: "00000a00044c00200000000000401012000000000000000020000000000080120200000000000204800000000000002100320000000004000001404000000000",
    },
    // The same, for a transaction that loaded addresses from a lookup table: the
    // writer's set is the message's accounts and both loaded lists, and an
    // engine that stopped at the message would answer this row's `mentions`
    // queries with silence.
    Vector {
        what: "a transaction that loaded addresses from a lookup table",
        accounts: &[
            "5DGbogKUo8haEYC9Tt4FTHE5ue55bjLC4DpSgE1g6yT1",
            "4hXPGTmR6dKNNqjLYdfDRSrTaa1Wt2GZoZnQ9hAJEeev",
            "ComputeBudget111111111111111111111111111111",
            "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg",
            "78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX",
            "Fgh9JSZ2qfSjCw9RPJ85W2xbihsp2muLvfRztzoVR7f1",
            "5tqSN3xtgCFNXvLhTeZXafv7qTCwKfMNBuVkWHuJ3EW3",
            "ACNWkvJ1obziiWBsDr4wkMeBeDtPJyosDrFkBrYLgX3F",
        ],
        bloom: "000400900c0200000002010002008020082812000000a000208028c00001000010000200040000001000100848080a5804040010601006000000800880090404",
    },
    // An instruction, whose account set is assembled from sixteen columns and an
    // overflow list rather than from one. It mentions the USDC mint twice, which
    // is the case that says the filter is over a *set*: an engine counting
    // insertions rather than bits would still agree here, and one salting a hash
    // with a position would not.
    Vector {
        what: "an instruction with more than sixteen accounts, one of them twice",
        accounts: &[
            "AjmhV4FUFxvCbAdhRL7SNC4VWV6mHr3Js4N1qe7Y8U8C",
            "EjGqFNYvxbocTpijJnfoowEcmYpf43qxhsfEL8y8BsQi",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "EhDVDRBGNC71aYqeWvEQTB9YFvK4chrjUpqvSYpcEjFj",
            "5BS4EbdaXfJAzuVNdPkBdcHXzavY4x3fnjmzbWptjRPd",
            "Bu47xyxCvFeQEWNHkzPJakXLaS5h2CYdm3aMJsy9JVT7",
            "pJjzLvBrMhmyU9mu3ERn7DbMuwwizAch6HXK9wnHqk8",
            "GHTBuM2wLKvw7ZTsJfZiHoWfdbPsUadgM1dtVhQiQWG8",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "11111111111111111111111111111111",
            "Sysvar1nstructions1111111111111111111111111",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
            "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG",
            "F2awnA7GM36eYcvU9QeAPErXASefvoek2RMNCWBHAUwN",
            "GqLr9yhddGrnMhYxxJUniBzAxvTxFCxAecSSMbhBdxEG",
            "BBchf7NgTxEQyVCNyxNeCtNCGHBvXCeZJUKXs1deVtm7",
            "8GD7Hb2BnwGfJqEP2pg5aVzn1NgKJiE9KC26JdWzHzdG",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        ],
        bloom: "02804c300008444324288242000a041a141980900404058f01001020285a097222c3240380204290401810002000054504d940000011000a00044129a0000021",
    },
];

fn unhex(text: &str) -> Vec<u8> {
    assert_eq!(text.len() % 2, 0, "a byte is two digits");

    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap())
        .collect()
}

fn popcount(bytes: &[u8]) -> u32 {
    bytes.iter().map(|b| b.count_ones()).sum()
}

/// The engine's construction must reproduce the writer's, bit for bit.
///
/// Equality is the assertion rather than membership because membership cannot
/// fail in the direction that matters: a filter built with too few hashes, or
/// too narrow, still contains everything the writer put in it. It contains a
/// great deal more, and the rows it wrongly admits are indistinguishable from
/// the false positives the invariant permits.
///
/// Covers CT-3 · INV-P9
#[test]
fn the_engine_builds_the_bloom_the_archiver_wrote() {
    for vector in VECTORS {
        let expected = unhex(vector.bloom);
        assert_eq!(expected.len(), NUM_BYTES, "{}: vector width", vector.what);

        // A saturated filter matches every value, and would satisfy the equality
        // below however the hash was wrong.
        let set = popcount(&expected);
        assert!(
            set > 0 && set < (NUM_BYTES * 8) as u32,
            "{}: {set} of {} bits set, which asserts nothing",
            vector.what,
            NUM_BYTES * 8
        );

        assert_eq!(
            Bloom::of(vector.accounts.iter().copied()),
            expected,
            "{}: the engine's bloom is not the one the archiver stored",
            vector.what
        );
    }
}

/// The vectors are only the writer's construction while the catalog asks for
/// that construction. The count is the half the reader takes from the catalog,
/// so a catalog declaring some other one would leave the engine reading real
/// chunks with a filter these vectors do not describe. The width it takes from
/// the stored array instead, and `num_bytes` has to agree only for the vectors
/// frozen above to be describing the filters a real chunk holds — which is what
/// the sweeps check directly.
///
/// Covers CT-3 · INV-P9
#[test]
fn the_catalog_declares_the_construction_the_vectors_were_built_with() {
    let solana = meta("solana");

    for table in ["transactions", "instructions"] {
        let filter = solana
            .table(table)
            .unwrap()
            .special_filters
            .get("mentions_account")
            .unwrap_or_else(|| panic!("{table} must carry a mentionsAccount filter"));

        let SpecialFilter::BloomFilter {
            num_bytes,
            num_hashes,
            ..
        } = filter
        else {
            panic!("{table}'s mentionsAccount must be a bloom filter");
        };

        assert_eq!(
            (*num_bytes, *num_hashes),
            (NUM_BYTES, NUM_HASHES),
            "{table}"
        );
    }
}

/// The value offsets of one list row, or an empty range where the row is null.
fn list_range(list: &ListArray, row: usize) -> std::ops::Range<usize> {
    if list.is_null(row) {
        return 0..0;
    }

    let offsets = list.value_offsets();

    offsets[row] as usize..offsets[row + 1] as usize
}

fn list_of(batch: &RecordBatch, name: &str) -> ListArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("the chunk must carry '{name}'"))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap_or_else(|| panic!("'{name}' must be a list"))
        .clone()
}

fn strings_of(list: &ListArray) -> StringArray {
    list.values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("account lists hold strings")
        .clone()
}

fn blooms_of(batch: &RecordBatch) -> FixedSizeBinaryArray {
    batch
        .column_by_name("accounts_bloom")
        .expect("the chunk must carry 'accounts_bloom'")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("a bloom is fixed-size binary")
        .clone()
}

/// Every transaction the archiver wrote, rebuilt from the three lists its writer
/// draws on.
///
/// Covers CT-3 · INV-P9
#[test]
#[ignore = "requires external fixture data"]
fn every_transaction_rebuilds_the_bloom_the_archiver_wrote() {
    if !fixture_tree_has("solana") {
        return;
    }

    let chunk = fixture_chunk("solana");
    let batches = read_columns(
        &chunk,
        "transactions",
        &["account_keys", "loaded_addresses", "accounts_bloom"],
    )
    .expect("the fixture chunk carries transactions");

    let mut rows = 0usize;
    let mut loaded_addresses = 0usize;
    for batch in &batches {
        let keys = list_of(batch, "account_keys");
        let key_values = strings_of(&keys);

        let loaded = batch
            .column_by_name("loaded_addresses")
            .expect("the chunk must carry 'loaded_addresses'")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("loaded addresses are a struct of two lists")
            .clone();

        let readonly = loaded
            .column_by_name("readonly")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();
        let writable = loaded
            .column_by_name("writable")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();

        let readonly_values = strings_of(&readonly);
        let writable_values = strings_of(&writable);
        let blooms = blooms_of(batch);

        for row in 0..batch.num_rows() {
            let mut bloom = Bloom::new();

            for i in list_range(&keys, row) {
                bloom.insert(key_values.value(i));
            }

            let readonly_range = list_range(&readonly, row);
            let writable_range = list_range(&writable, row);
            if !readonly_range.is_empty() || !writable_range.is_empty() {
                loaded_addresses += 1;
            }

            for i in readonly_range {
                bloom.insert(readonly_values.value(i));
            }
            for i in writable_range {
                bloom.insert(writable_values.value(i));
            }

            assert_eq!(
                bloom.bytes,
                blooms.value(row),
                "transaction row {rows} was written with a bloom the engine does not build"
            );
            rows += 1;
        }
    }

    assert!(rows > 0, "the sweep read no rows, so it compared nothing");
    assert!(
        loaded_addresses > 0,
        "no transaction loaded addresses from a lookup table, so the half of the \
         account set an engine reading only the message would miss was never read"
    );
}

/// Every instruction the archiver wrote, whose account set its writer assembles
/// from sixteen columns and an overflow list rather than from one.
///
/// The whole table rather than a prefix: `instructions` is sorted
/// `program_id -> d1 -> b9 -> block_number`, so a prefix is a biased sample of
/// programs, not a shuffle, and an archiver that assembled the set differently
/// for a program sorting late would be invisible to one.
///
/// Covers CT-3 · INV-P9
#[test]
#[ignore = "requires external fixture data"]
fn every_instruction_rebuilds_the_bloom_the_archiver_wrote() {
    if !fixture_tree_has("solana") {
        return;
    }

    let names: Vec<String> = (0..16).map(|i| format!("a{i}")).collect();
    let mut columns: Vec<&str> = names.iter().map(String::as_str).collect();
    columns.push("rest_accounts");
    columns.push("accounts_bloom");

    let chunk = fixture_chunk("solana");
    let batches = read_columns(&chunk, "instructions", &columns)
        .expect("the fixture chunk carries instructions");

    let mut rows = 0usize;
    let mut overflowed = 0usize;
    for batch in &batches {
        let accounts: Vec<StringArray> = names
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("the chunk must carry '{name}'"))
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or_else(|| panic!("'{name}' must be a string"))
                    .clone()
            })
            .collect();

        let rest = list_of(batch, "rest_accounts");
        let rest_values = strings_of(&rest);
        let blooms = blooms_of(batch);

        for row in 0..batch.num_rows() {
            let mut bloom = Bloom::new();

            for column in &accounts {
                if !column.is_null(row) {
                    bloom.insert(column.value(row));
                }
            }

            let overflow = list_range(&rest, row);
            if !overflow.is_empty() {
                overflowed += 1;
            }
            for i in overflow {
                bloom.insert(rest_values.value(i));
            }

            assert_eq!(
                bloom.bytes,
                blooms.value(row),
                "instruction row {rows} was written with a bloom the engine does not build"
            );
            rows += 1;
        }
    }

    assert!(rows > 0, "the sweep read no rows, so it compared nothing");
    assert!(
        overflowed > 0,
        "no row overflowed its sixteen account columns, so the list half was never read"
    );
}

/// The client-visible half: a row that truly mentions an account is returned.
///
/// The sweeps above pin the bits; this pins that the bits are what the query
/// reaches. Over-approximation means the response may hold rows that do not
/// mention the account, so the assertion is one-sided — but not vacuous: a
/// filter that matched everything would satisfy "no row is missing" too, and the
/// row count is asserted against the table's.
///
/// Covers CT-3 · INV-P9
#[test]
#[ignore = "requires external fixture data"]
fn a_transaction_that_mentions_an_account_is_returned() {
    if !fixture_tree_has("solana") {
        return;
    }

    // The mint every SPL token transfer names, so the truth set is neither empty
    // nor the whole table.
    const ACCOUNT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    const FROM: u64 = 217710049;
    const TO: u64 = 217710148;

    let chunk = fixture_chunk("solana");
    let batches = read_columns(
        &chunk,
        "transactions",
        &["block_number", "transaction_index", "account_keys"],
    )
    .expect("the fixture chunk carries transactions");

    let mut mentions: BTreeSet<(u64, u64)> = BTreeSet::new();
    let mut in_range = 0usize;
    for batch in &batches {
        let blocks = arrow::compute::cast(
            batch.column_by_name("block_number").unwrap(),
            &arrow::datatypes::DataType::UInt64,
        )
        .unwrap();
        let blocks = blocks
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .clone();

        let indexes = arrow::compute::cast(
            batch.column_by_name("transaction_index").unwrap(),
            &arrow::datatypes::DataType::UInt64,
        )
        .unwrap();
        let indexes = indexes
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .clone();

        let keys = list_of(batch, "account_keys");
        let key_values = strings_of(&keys);

        for row in 0..batch.num_rows() {
            let block = blocks.value(row);
            if !(FROM..=TO).contains(&block) {
                continue;
            }
            in_range += 1;

            if list_range(&keys, row).any(|i| key_values.value(i) == ACCOUNT) {
                mentions.insert((block, indexes.value(row)));
            }
        }
    }

    assert!(
        !mentions.is_empty() && mentions.len() < in_range,
        "the truth set is {} of {in_range} rows, which asserts nothing",
        mentions.len()
    );

    let body = run(
        "solana",
        &meta("solana"),
        format!(
            r#"{{"type":"solana","fromBlock":{FROM},"toBlock":{TO},
                "fields":{{"block":{{"number":true}},"transaction":{{"transactionIndex":true}}}},
                "transactions":[{{"mentionsAccount":["{ACCOUNT}"]}}]}}"#
        )
        .as_bytes(),
    )
    .expect("the query must be answerable");

    let returned: BTreeSet<(u64, u64)> = items_of(&body, "transactions")
        .iter()
        .map(|(block, item)| (*block, item["transactionIndex"].as_u64().unwrap()))
        .collect();

    let missing: Vec<_> = mentions.difference(&returned).collect();
    assert!(
        missing.is_empty(),
        "{} transactions mentioning the account were not returned, starting at {:?}",
        missing.len(),
        missing.first()
    );

    // False positives are permitted and must not be filtered away, but a filter
    // that returned the table would pass the assertion above.
    assert!(
        returned.len() < in_range,
        "the filter returned every transaction in range, so it narrowed nothing"
    );
}

/// A row the filter admits but that does not mention the account is returned.
///
/// [INV-P9] has two clauses, and every test above serves the first: the
/// construction must be the writer's. The second is what the over-approximation
/// costs the client — "an engine MUST NOT post-filter them away" — and it is the
/// clause an engine breaks by being helpful. Re-checking the bloom's answer
/// against the row's real accounts looks like an improvement, returns a strictly
/// more accurate response, and is wrong: the account set the writer hashed is
/// not always a column the reader has, so the re-check drops rows that do
/// mention the account.
///
/// The false positive here is a real one rather than a mismatch written by hand.
/// A filter carrying enough accounts admits values nobody inserted, and one of
/// those is what the query asks for, so the row the response must carry is a row
/// whose every account is something else.
///
/// [INV-P9]: ../../../spec/07-invariants.md#inv-p9
///
/// Covers CT-3 · INV-P9
#[test]
fn a_false_positive_is_not_filtered_away() {
    use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use sqd_query_engine::metadata::parse_dataset_description;
    use std::sync::Arc;

    use crate::harness::chunk::{blocks_parquet, write_table};
    use crate::harness::fixtures::run_against;

    const CATALOG: &str = r#"
name: overapprox
tables:
  blocks:
    field_name: block
    block_number_column: number
    sort_key: [number]
    filters: []
    fields: [number]
    columns:
      number: { type: uint64 }
  items:
    query_name: items
    field_name: item
    block_number_column: block_number
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    filters: [mentions_account]
    fields: [seq, account]
    special_filters:
      mentions_account:
        type: bloom_filter
        column: accounts_bloom
        num_bytes: 64
        num_hashes: 7
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      account: { type: string }
      accounts_bloom: { type: fixed_binary_64, system: true }
"#;

    const BLOCK: u64 = 10;
    const MENTIONED: &str = "mentioned-0";

    // Enough accounts that the filter admits values nobody inserted, which is
    // the condition the invariant is about. Too few and there is no false
    // positive to find; a saturated filter would admit everything and assert
    // nothing, so the popcount is checked below.
    let inserted: Vec<String> = (0..64).map(|i| format!("mentioned-{i}")).collect();

    let mut bloom = Bloom::new();
    for account in &inserted {
        bloom.insert(account);
    }

    let set = popcount(&bloom.bytes);
    assert!(
        set < (NUM_BYTES * 8) as u32,
        "the filter is saturated, so admitting a stranger asserts nothing"
    );

    // A marginal false positive: every one of its seven bits is set, and its
    // eighth is not. The seven are what the filter admits it on; the eighth is
    // what makes an engine that narrows the filter at all — one hash more, a
    // re-check against the row — fail this test rather than pass it by luck.
    let stranger = (0..1 << 16)
        .map(|i| format!("stranger-{i}"))
        .find(|candidate| {
            (0..NUM_HASHES).all(|n| bloom.holds(candidate, n))
                && !bloom.holds(candidate, NUM_HASHES)
        })
        .expect("some stranger is admitted on seven bits and not on eight");
    assert!(
        !inserted.contains(&stranger),
        "the stranger was inserted, so it is not a false positive"
    );

    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), &[BLOCK]);
    write_table(
        dir.path(),
        "items",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
            Field::new("account", DataType::Utf8, false),
            Field::new("accounts_bloom", DataType::FixedSizeBinary(64), false),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![BLOCK])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32])) as ArrayRef,
            Arc::new(StringArray::from(vec![MENTIONED])) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(bloom.bytes.clone())).unwrap(),
            ) as ArrayRef,
        ],
    );

    let catalog = parse_dataset_description(CATALOG).unwrap();
    let query = format!(
        r#"{{"type":"overapprox","fromBlock":{BLOCK},"toBlock":{BLOCK},
            "fields":{{"block":{{"number":true}},"item":{{"seq":true,"account":true}}}},
            "items":[{{"mentionsAccount":["{stranger}"]}}]}}"#
    );
    let body = run_against(&catalog, dir.path(), &query).expect("the query must be answerable");

    let items = items_of(&body, "items");
    assert_eq!(
        items.len(),
        1,
        "the filter admits this row, so post-filtering it away is the only way to lose it"
    );
    assert_eq!(
        items[0].1["account"].as_str().unwrap(),
        MENTIONED,
        "the row returned is the one whose accounts are not what the query asked for"
    );
}

/// The hash count is the catalog's, and it is the count the engine uses.
///
/// This is the half the vectors above cannot reach. They pin where the hash puts
/// a value, and the sweeps pin that the writer put the right values in — but
/// both compute the bits themselves, and an engine testing membership with
/// *fewer* hashes than the writer used agrees with every one of them. It agrees
/// with the client too: fewer hashes is more false positives, which the
/// invariant permits, so nothing downstream can see it either.
///
/// What separates the counts is a filter that holds a value's first six bits and
/// not its seventh. A row carrying one is a row a six-hash engine returns and a
/// seven-hash engine does not, so the same chunk read under two catalogs answers
/// two different ways — which is the assertion that the count comes from the
/// catalog rather than from somewhere in the code.
///
/// Covers CT-3 · INV-P9
#[test]
fn the_hash_count_is_the_one_the_catalog_declares() {
    use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use sqd_query_engine::metadata::parse_dataset_description;
    use std::sync::Arc;

    use crate::harness::chunk::{blocks_parquet, write_table};
    use crate::harness::fixtures::run_against;
    use crate::harness::json::count_items;

    const CATALOG: &str = r#"
name: bloomed
tables:
  blocks:
    field_name: block
    block_number_column: number
    sort_key: [number]
    filters: []
    fields: [number]
    columns:
      number: { type: uint64 }
  items:
    query_name: items
    field_name: item
    block_number_column: block_number
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    filters: [mentions_account]
    fields: [seq]
    special_filters:
      mentions_account:
        type: bloom_filter
        column: accounts_bloom
        num_bytes: 64
        num_hashes: HASHES
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      accounts_bloom: { type: fixed_binary_64, system: true }
"#;

    const MENTIONED: &str = "an account the row really mentions";
    const BLOCK: u64 = 10;

    let mut bloom = Bloom::new();
    bloom.insert(MENTIONED);

    // A value whose first six bits the filter holds and whose seventh it does
    // not. Sought rather than asserted, because a candidate's seventh bit may
    // already be set by one of its own six.
    let witness = (0..64)
        .map(|i| format!("witness-{i}"))
        .find(|candidate| {
            let mut trial = Bloom {
                bytes: bloom.bytes.clone(),
            };
            trial.insert_upto(candidate, NUM_HASHES - 1);

            !trial.holds(candidate, NUM_HASHES - 1)
        })
        .expect("some candidate's last bit is not set by its first six");

    bloom.insert_upto(&witness, NUM_HASHES - 1);

    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), &[BLOCK]);
    write_table(
        dir.path(),
        "items",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
            Field::new("accounts_bloom", DataType::FixedSizeBinary(64), false),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![BLOCK])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32])) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(bloom.bytes.clone())).unwrap(),
            ) as ArrayRef,
        ],
    );

    let matches = |hashes: usize, account: &str| {
        let catalog =
            parse_dataset_description(&CATALOG.replace("HASHES", &hashes.to_string())).unwrap();
        let query = format!(
            r#"{{"type":"bloomed","fromBlock":{BLOCK},"toBlock":{BLOCK},
                "fields":{{"item":{{"seq":true}}}},
                "items":[{{"mentionsAccount":["{account}"]}}]}}"#
        );
        let body = run_against(&catalog, dir.path(), &query).expect("the query must be answerable");

        count_items(&body, "items") == 1
    };

    assert!(
        matches(NUM_HASHES, MENTIONED),
        "the row's own account must not be filtered away"
    );
    assert!(
        !matches(NUM_HASHES, &witness),
        "a value missing its seventh bit was matched, so fewer than seven hashes were tested"
    );
    assert!(
        matches(NUM_HASHES - 1, &witness),
        "the same value stayed unmatched under a catalog asking for six hashes, \
         so the count is not being read from the catalog"
    );
}
