//! End-to-end fixture tests: run queries against parquet chunks and compare
//! output with expected results from the legacy engine.
//!
//! Fixture data is expected at `tests/fixtures/` (symlink to legacy repo's
//! `crates/query/fixtures/`). Tests are skipped if fixtures are not present.

use sqd_query_engine::metadata::{
    load_dataset_description, ColumnDescription, DatasetDescription, JsonEncoding, SpecialFilter,
};
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{camel_to_snake, compile, parse_query};
use std::path::{Path, PathBuf};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn run_fixture_query(
    meta_path: &str,
    chunk_dir: &Path,
    query_json: &[u8],
) -> anyhow::Result<serde_json::Value> {
    run_fixture_query_raw(meta_path, chunk_dir, query_json).map(|(v, _)| v)
}

/// Run a query and return both the parsed blocks and the exact bytes the engine
/// emitted. The bytes matter: `serde_json::Value` normalises object key order
/// away (`Map` is a `BTreeMap`, and even under `preserve_order` `IndexMap`'s
/// `PartialEq` ignores order), so a value comparison alone cannot see the
/// ordering that YAML column order is responsible for producing.
fn run_fixture_query_raw(
    meta_path: &str,
    chunk_dir: &Path,
    query_json: &[u8],
) -> anyhow::Result<(serde_json::Value, Vec<u8>)> {
    let meta = load_dataset_description(Path::new(meta_path))?;
    let parsed = parse_query(query_json, &meta)?;
    let plan = compile(&parsed, &meta)?;
    let mut result = Vec::new();
    let mut raw = b"[".to_vec();
    if let Some(mut blocks) = execute_plan(&plan, &meta, chunk_dir)? {
        let mut buf = Vec::new();
        while blocks.has_next_block() {
            buf.clear();
            blocks.write_next_block(&mut buf);
            if !result.is_empty() {
                raw.push(b',');
            }
            raw.extend_from_slice(&buf);
            result.push(serde_json::from_slice(&buf)?);
        }
    }
    raw.push(b']');
    Ok((serde_json::Value::Array(result), raw))
}

/// Datasets whose `result.json` key order matches what this engine emits.
///
/// Output field order follows *catalog column order* ([INV-O6]), which the spec
/// allows to differ from the reference's DSL declaration order — see "Deliberate
/// divergences from the reference" in `spec/GAPS.md`. So this is not a
/// conformance requirement; it is a regression guard for catalogs that were
/// deliberately written to reproduce the reference's order, as `tron.yaml` was.
/// An allowlist because most goldens predate later reference field-order changes
/// and would fail for reasons unrelated to this engine. Widen as they are
/// regenerated.
const KEY_ORDER_CHECKED: &[&str] = &["tron"];

/// A JSON value that remembers object key order, which `serde_json::Value` does
/// not. Only the shape is kept — leaves collapse, since values are already
/// compared separately.
#[derive(Debug)]
enum KeyOrder {
    Obj(Vec<(String, KeyOrder)>),
    Arr(Vec<KeyOrder>),
    Leaf,
}

impl<'de> serde::Deserialize<'de> for KeyOrder {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = KeyOrder;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut m: A,
            ) -> Result<KeyOrder, A::Error> {
                let mut v = Vec::new();
                while let Some(e) = m.next_entry::<String, KeyOrder>()? {
                    v.push(e);
                }
                Ok(KeyOrder::Obj(v))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut s: A,
            ) -> Result<KeyOrder, A::Error> {
                let mut v = Vec::new();
                while let Some(e) = s.next_element::<KeyOrder>()? {
                    v.push(e);
                }
                Ok(KeyOrder::Arr(v))
            }
            fn visit_bool<E>(self, _: bool) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
            fn visit_i64<E>(self, _: i64) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
            fn visit_u64<E>(self, _: u64) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
            fn visit_f64<E>(self, _: f64) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
            fn visit_str<E>(self, _: &str) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
            fn visit_unit<E>(self) -> Result<KeyOrder, E> {
                Ok(KeyOrder::Leaf)
            }
        }
        d.deserialize_any(V)
    }
}

/// First path where `actual`'s key order departs from `expected`, if any.
fn first_order_mismatch(expected: &KeyOrder, actual: &KeyOrder, path: &str) -> Option<String> {
    match (expected, actual) {
        (KeyOrder::Obj(e), KeyOrder::Obj(a)) => {
            let ek: Vec<&str> = e.iter().map(|(k, _)| k.as_str()).collect();
            let ak: Vec<&str> = a.iter().map(|(k, _)| k.as_str()).collect();
            if ek != ak {
                return Some(format!("{}: expected {:?}, got {:?}", path, ek, ak));
            }
            e.iter().zip(a).find_map(|((k, ev), (_, av))| {
                first_order_mismatch(ev, av, &format!("{}.{}", path, k))
            })
        }
        (KeyOrder::Arr(e), KeyOrder::Arr(a)) => e
            .iter()
            .zip(a)
            .enumerate()
            .find_map(|(i, (ev, av))| first_order_mismatch(ev, av, &format!("{}[{}]", path, i))),
        _ => None,
    }
}

fn test_fixture(dataset: &str, meta_path: &str, query_name: &str) {
    let base = fixture_dir().join(dataset);
    let chunk = base.join("chunk");
    let query_file = base.join("queries").join(query_name).join("query.json");
    let result_file = base.join("queries").join(query_name).join("result.json");

    if !chunk.exists() || !query_file.exists() {
        eprintln!("SKIP {}/{}: fixtures not found", dataset, query_name);
        return;
    }

    let query_json = std::fs::read(&query_file).unwrap();
    let mut actual_raw: Option<Vec<u8>> = None;
    let actual =
        match std::panic::catch_unwind(|| run_fixture_query_raw(meta_path, &chunk, &query_json)) {
            // A query error is a legitimate result: legacy serializes it as a bare
            // JSON string, and some fixtures expect exactly that (e.g. requesting a
            // schema column that's absent from the parquet). Use the root cause so
            // the message matches legacy's `err.to_string()` without anyhow context.
            Ok(Ok((v, raw))) => {
                actual_raw = Some(raw);
                v
            }
            Ok(Err(err)) => serde_json::Value::String(err.root_cause().to_string()),
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                panic!(
                    "{}/{}: query execution panicked: {}",
                    dataset, query_name, msg
                );
            }
        };

    if !result_file.exists() {
        // No expected result — write actual for manual review
        let actual_path = base
            .join("queries")
            .join(query_name)
            .join("actual.temp.json");
        serde_json::to_writer_pretty(std::fs::File::create(&actual_path).unwrap(), &actual)
            .unwrap();
        eprintln!(
            "SKIP {}/{}: no result.json, wrote actual to {:?}",
            dataset, query_name, actual_path
        );
        return;
    }

    let expected_bytes = std::fs::read(&result_file).unwrap();
    let expected: serde_json::Value = serde_json::from_slice(&expected_bytes).unwrap();

    if expected != actual {
        // Write actual for diff inspection
        let actual_path = base
            .join("queries")
            .join(query_name)
            .join("actual.temp.json");
        serde_json::to_writer_pretty(std::fs::File::create(&actual_path).unwrap(), &actual)
            .unwrap();
        panic!(
            "{}/{}: output mismatch! Expected {} blocks, got {}. Diff: {:?}",
            dataset,
            query_name,
            expected.as_array().map(|a| a.len()).unwrap_or(0),
            actual.as_array().map(|a| a.len()).unwrap_or(0),
            actual_path,
        );
    }

    // Values match. Now check the property a `Value` comparison structurally
    // cannot see: that object keys come out in the same order legacy emitted
    // them. Output field order is driven by YAML column order
    // (`order_columns_by_metadata`), so without this a column reordering in a
    // metadata file is an invisible regression.
    if KEY_ORDER_CHECKED.contains(&dataset) {
        if let Some(raw) = actual_raw {
            let expected_order: KeyOrder = serde_json::from_slice(&expected_bytes).unwrap();
            let actual_order: KeyOrder = serde_json::from_slice(&raw).unwrap();
            if let Some(m) = first_order_mismatch(&expected_order, &actual_order, "$") {
                panic!(
                    "{}/{}: JSON key order differs from legacy at {}",
                    dataset, query_name, m
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Solana fixtures
// ---------------------------------------------------------------------------

macro_rules! solana_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<solana_ $name>]() {
                test_fixture("solana", "metadata/solana.yaml", stringify!($name));
            }
        }
    };
}

solana_fixture!(include_all_blocks);
solana_fixture!(instruction_first);
solana_fixture!(whirpool_usdc_sol_swaps);
solana_fixture!(is_committed);
solana_fixture!(balance_first);
solana_fixture!(balances_from_instruction);
solana_fixture!(token_balance_first);
solana_fixture!(token_balances_from_instruction);
solana_fixture!(transaction_fee_payer);
solana_fixture!(transaction_mentions_account);
solana_fixture!(instruction_mentions_account);
solana_fixture!(log_kind);
solana_fixture!(log_program_id);
solana_fixture!(rewards);
solana_fixture!(tx_instructions_from_instruction);

// Production query patterns (from ClickHouse worker_query_logs)
solana_fixture!(prod_pattern_01_instr_d1_d4_x11_inner_logs_tx_tokbal);
solana_fixture!(prod_pattern_02_instr_d1_d8_x3_inner_logs_tx_tokbal);
solana_fixture!(prod_pattern_03_instr_d1_d8_x3_inner_tx_tokbal);
solana_fixture!(prod_pattern_04_instr_a0_a1_x14_inner_tx_txinstr_tokbal);
solana_fixture!(prod_pattern_05_instr_a1_d1_x4_inner_logs_tx_bal);
solana_fixture!(prod_pattern_06_instr_d8_programId_inner_tx_tokbal);
solana_fixture!(prod_pattern_07_instr_d1_d8_x8_inner_logs_tx_tokbal);
solana_fixture!(prod_pattern_08_instr_d8_programId_x2_inner_logs_tx_tokbal);
solana_fixture!(prod_pattern_09_instr_programId_inner_logs_tx_tokbal);
solana_fixture!(prod_pattern_10_instr_d8_programId_inner_tx);
solana_fixture!(prod_pattern_11_instr_a0_a1_x13_inner_tx_tokbal);

// ---------------------------------------------------------------------------
// Ethereum (EVM) fixtures
// ---------------------------------------------------------------------------

macro_rules! evm_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<evm_ $name>]() {
                test_fixture("ethereum", "metadata/evm.yaml", stringify!($name));
            }
        }
    };
    // Allow test name different from directory name
    ($name:ident, $dir:expr) => {
        paste::paste! {
            #[test]
            fn [<evm_ $name>]() {
                test_fixture("ethereum", "metadata/evm.yaml", $dir);
            }
        }
    };
}

evm_fixture!(include_all_blocks);
evm_fixture!(sighash_filtering);
evm_fixture!(topics_filtering);
evm_fixture!(empty_filter);
evm_fixture!(statediffs);
evm_fixture!(subtraces);
evm_fixture!(trace_parents);
evm_fixture!(transaction_traces);
evm_fixture!(transaction_logs_for_logs);
evm_fixture!(transaction_logs_for_traces);
evm_fixture!(transaction_statediffs_for_logs);
evm_fixture!(nonce_filtering);
evm_fixture!(create_result_address_filtering);
evm_fixture!(suicide_address);
evm_fixture!(load_all_tx_and_logs);
evm_fixture!(logs_from_transaction_and_request);
evm_fixture!(logs_from_transaction_and_request_uppercase);
evm_fixture!(evm_all_logs_regression, "all_logs_and_logs+tx_regression");
evm_fixture!(degen_reference);
evm_fixture!(degen_request);
evm_fixture!(large_list_filter);
evm_fixture!(example_showcase01_all_usdc_transfers);
evm_fixture!(example_showcase02_all_transfers_to_a_wallet);
evm_fixture!(example_showcase03_all_events_caused_by_contract_calls);
evm_fixture!(example_showcase04_all_mint_events);
evm_fixture!(example_showcase06_all_bayc_call_traces);
evm_fixture!(example_showcase07_grab_all_nft_transfers);
evm_fixture!(example_uniswapv3_abridged_squid_no_preloaded_pools);
evm_fixture!(example_uniswapv3_abridged_squid_preloaded_pools);
evm_fixture!(example_evm_ipfs_example);
evm_fixture!(example_modified_dia_prices_squid);

// NET-536 planner-bug regression: relation request with no predicate on the
// child table (logs/traces/state_diffs with `transaction: true` and friends).
evm_fixture!(logs_no_predicate_with_transaction);
evm_fixture!(logs_no_predicate_with_transaction_logs);
evm_fixture!(logs_no_predicate_with_transaction_state_diffs);
evm_fixture!(logs_no_predicate_with_transaction_traces);
evm_fixture!(traces_no_predicate_with_transaction);
evm_fixture!(traces_no_predicate_with_transaction_logs);
evm_fixture!(state_diffs_no_predicate_with_transaction);

// Extended EVM trace API (#73): callCallType + *NonZero trace filters.
evm_fixture!(trace_call_type);
evm_fixture!(trace_value_non_zero);

// Production query patterns (from ClickHouse worker_query_logs)
evm_fixture!(
    evm_prod_01_multi_log_with_tx,
    "prod_pattern_01_logs_address_topic0_x7_with_tx"
);
evm_fixture!(evm_prod_02_fields_only, "prod_pattern_02_fields_only");
evm_fixture!(evm_prod_03_logs_topic0, "prod_pattern_03_logs_topic0");
evm_fixture!(
    evm_prod_04_txs_with_logs_all_blocks,
    "prod_pattern_04_txs_with_logs_all_blocks"
);
evm_fixture!(
    evm_prod_05_logs_addr_topic0,
    "prod_pattern_05_logs_address_topic0"
);
evm_fixture!(evm_prod_06_fields_only_2, "prod_pattern_06_fields_only");
evm_fixture!(
    evm_prod_07_logs_topic0_with_tx,
    "prod_pattern_07_logs_topic0_with_tx"
);
evm_fixture!(
    evm_prod_08_logs_addr_topic0_x2,
    "prod_pattern_08_logs_address_topic0_x2"
);
evm_fixture!(evm_prod_09_logs_topic0_2, "prod_pattern_09_logs_topic0");
evm_fixture!(
    evm_prod_10_logs_all_all_blocks,
    "prod_pattern_10_logs_all_all_blocks"
);
evm_fixture!(
    evm_prod_11_multi_table_heavy,
    "prod_pattern_11_logs_topic0_x2_with_tx_txs_traces"
);
evm_fixture!(
    evm_prod_12_logs_topic0_with_tx_2,
    "prod_pattern_12_logs_topic0_with_tx"
);
evm_fixture!(evm_prod_13_all_blocks, "prod_pattern_13_all_blocks");
evm_fixture!(
    evm_prod_14_logs_addr_topic0_with_tx,
    "prod_pattern_14_logs_address_topic0_with_tx"
);
evm_fixture!(
    evm_prod_15_logs_addr_topic0_x2_with_tx,
    "prod_pattern_15_logs_address_topic0_x2_with_tx"
);

// ---------------------------------------------------------------------------
// Bitcoin fixtures
// ---------------------------------------------------------------------------

macro_rules! bitcoin_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<bitcoin_ $name>]() {
                test_fixture("bitcoin", "metadata/bitcoin.yaml", stringify!($name));
            }
        }
    };
}

bitcoin_fixture!(include_all_blocks);
bitcoin_fixture!(input_address_filtering);
bitcoin_fixture!(input_coinbase_filtering);
bitcoin_fixture!(input_filtering_with_tx_data);
bitcoin_fixture!(input_script_type_filtering);
bitcoin_fixture!(output_address_filtering);
bitcoin_fixture!(output_filtering_with_tx_data);
bitcoin_fixture!(output_script_type_filtering);

// ---------------------------------------------------------------------------
// Fuel fixtures
// ---------------------------------------------------------------------------

macro_rules! fuel_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<fuel_ $name>]() {
                test_fixture("fuel", "metadata/fuel.yaml", stringify!($name));
            }
        }
    };
}

fuel_fixture!(asset_transfers);
fuel_fixture!(created_contracts);
fuel_fixture!(log_data_from_contract);
// NET-92: requesting a schema column absent from the parquet is a hard error.
fuel_fixture!(missing_block_fields);
fuel_fixture!(missing_field_with_join);

// ---------------------------------------------------------------------------
// Optimism fixtures (uses EVM metadata)
// ---------------------------------------------------------------------------

macro_rules! optimism_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<optimism_ $name>]() {
                test_fixture("optimism", "metadata/evm.yaml", stringify!($name));
            }
        }
    };
}

optimism_fixture!(all);

// ---------------------------------------------------------------------------
// Binance fixtures (uses EVM metadata)
// ---------------------------------------------------------------------------

macro_rules! binance_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<binance_ $name>]() {
                test_fixture("binance", "metadata/evm.yaml", stringify!($name));
            }
        }
    };
}

binance_fixture!(example_showcase00_analyzing_a_large_number_of_wallets);
binance_fixture!(example_showcase05_dex_pair_creation_and_swaps);
binance_fixture!(example_thena_squid_no_preloaded_pools);
binance_fixture!(example_thena_squid_preloaded_pools);

// ---------------------------------------------------------------------------
// Tempo fixtures. Tempo is served as `kind: evm` (its extra block/tx fields live
// in the shared EVM schema as nullable columns, matching the legacy data model),
// so these use metadata/evm.yaml — there is no separate Tempo dataset kind.
// ---------------------------------------------------------------------------

macro_rules! tempo_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<tempo_ $name>]() {
                test_fixture("tempo", "metadata/evm.yaml", stringify!($name));
            }
        }
    };
}

tempo_fixture!(tempo_block_fields);
tempo_fixture!(tempo_transaction_fields);
tempo_fixture!(tempo_all_fields);

// ---------------------------------------------------------------------------
// Kusama fixtures (uses Substrate metadata)
// ---------------------------------------------------------------------------

macro_rules! kusama_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<kusama_ $name>]() {
                test_fixture("kusama", "metadata/substrate.yaml", stringify!($name));
            }
        }
    };
}

kusama_fixture!(example_balances_squid);
kusama_fixture!(example_giant_squid_explorer);
kusama_fixture!(example_giant_squid_main);
kusama_fixture!(example_giant_squid_stats);
kusama_fixture!(example_substrate_calls_example);
kusama_fixture!(example_substrate_remark_example);
kusama_fixture!(example_substrate_storage_example);

// ---------------------------------------------------------------------------
// Moonbeam fixtures (uses Substrate metadata)
// ---------------------------------------------------------------------------

macro_rules! moonbeam_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<moonbeam_ $name>]() {
                test_fixture("moonbeam", "metadata/substrate.yaml", stringify!($name));
            }
        }
    };
}

moonbeam_fixture!(all);
moonbeam_fixture!(call_relations);
moonbeam_fixture!(call_subcalls);
moonbeam_fixture!(event_call_stack);
moonbeam_fixture!(event_relations);
moonbeam_fixture!(evm_logs_query);
moonbeam_fixture!(example_balances_squid);
moonbeam_fixture!(example_fearless_parachain_staking_squid);
moonbeam_fixture!(example_giant_squid_explorer);
moonbeam_fixture!(example_giant_squid_main);
moonbeam_fixture!(example_giant_squid_stats);
moonbeam_fixture!(example_modified_substrate_frontier_example);
moonbeam_fixture!(example_proposals_squid);
moonbeam_fixture!(include_all_blocks);
moonbeam_fixture!(simple_call_query);
moonbeam_fixture!(simple_event_query);

// ---------------------------------------------------------------------------
// Hyperliquid Fills fixtures
// ---------------------------------------------------------------------------

macro_rules! hyperliquid_fills_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<hyperliquid_ $name>]() {
                test_fixture("hyperliquid", "metadata/hyperliquid_fills.yaml", stringify!($name));
            }
        }
    };
}

hyperliquid_fills_fixture!(coin_fills);
hyperliquid_fills_fixture!(user_fills);
hyperliquid_fills_fixture!(include_all_blocks);

// ---------------------------------------------------------------------------
// Hyperliquid Replica Commands fixtures
// ---------------------------------------------------------------------------

macro_rules! hyperliquid_replica_cmds_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<hyperliquid_replica_cmds_ $name>]() {
                test_fixture("hyperliquid_replica_cmds", "metadata/hyperliquid_replica_cmds.yaml", stringify!($name));
            }
        }
    };
}

hyperliquid_replica_cmds_fixture!(action_type);
hyperliquid_replica_cmds_fixture!(action_user);
hyperliquid_replica_cmds_fixture!(include_all_blocks);
hyperliquid_replica_cmds_fixture!(order_action);

// ---------------------------------------------------------------------------
// Tron fixtures
// ---------------------------------------------------------------------------

macro_rules! tron_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            fn [<tron_ $name>]() {
                test_fixture("tron", "metadata/tron.yaml", stringify!($name));
            }
        }
    };
}

tron_fixture!(all_fields);
tron_fixture!(include_all_blocks);
tron_fixture!(internal_transactions);
tron_fixture!(logs_with_transaction);
tron_fixture!(topics_filtering);
tron_fixture!(transactions_by_type);
tron_fixture!(transfer_asset_transactions);
tron_fixture!(transfer_transactions);
tron_fixture!(trigger_smart_contract);
tron_fixture!(trigger_smart_contract_with_relations);

// INV-P8: upper-casing a hex filter value leaves the response byte-identical.
//
// Which filter keys are foldable is resolved from the metadata, not hard-coded:
// a key is folded iff it resolves to a column declared `hex` / `hex_unprefixed`.
// That keeps the test honest when the schema changes — notably
// `_transfer_asset_contract_asset`, which is filterable but compared
// byte-exactly because legacy omits `to_lowercase_list` for asset names.
fn tron_metadata() -> DatasetDescription {
    load_dataset_description(Path::new("metadata/tron.yaml")).unwrap()
}

/// Resolve a query filter key to the physical column it filters on, the way
/// `parse_query` does: alias `filter_aliases`, then the table's
/// `column_alias` special filters, then the key itself.
fn resolve_filter_column<'a>(
    meta: &'a DatasetDescription,
    request_key: &str,
    filter_key: &str,
) -> Option<&'a ColumnDescription> {
    let snake = camel_to_snake(filter_key);
    let (table_name, alias) = match meta.query_aliases.get(request_key) {
        Some(a) => (a.table.as_str(), Some(a)),
        None => (
            meta.tables
                .iter()
                .find(|(n, d)| d.query_name.as_deref() == Some(request_key) || *n == request_key)
                .map(|(n, _)| n.as_str())?,
            None,
        ),
    };
    let table = meta.table(table_name)?;
    if let Some(a) = alias {
        if let Some(col) = a.filter_aliases.get(&snake) {
            return table.column(col);
        }
    }
    if let Some(SpecialFilter::ColumnAlias { column }) = table.special_filters.get(&snake) {
        return table.column(column);
    }
    table.column(&snake)
}

fn is_case_folded(col: &ColumnDescription) -> bool {
    matches!(
        col.json_encoding,
        Some(JsonEncoding::Hex) | Some(JsonEncoding::HexUnprefixed)
    )
}

/// Count items (everything that is not the block header) across a response.
fn count_items(v: &serde_json::Value) -> usize {
    v.as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|b| b.as_object())
                .flat_map(|b| b.iter())
                .filter(|(k, _)| k.as_str() != "header")
                .filter_map(|(_, v)| v.as_array())
                .map(|a| a.len())
                .sum()
        })
        .unwrap_or(0)
}

#[test]
fn tron_hex_filters_are_case_folded() {
    let base = fixture_dir().join("tron");
    let chunk = base.join("chunk");
    if !chunk.exists() {
        eprintln!("SKIP tron_hex_filters_are_case_folded: fixtures not found");
        return;
    }
    let meta = tron_metadata();

    for query_name in [
        "all_fields",
        "logs_with_transaction",
        "internal_transactions",
        "transfer_transactions",
    ] {
        let query_file = base.join("queries").join(query_name).join("query.json");
        let query_json = std::fs::read(&query_file).unwrap();
        let lower = run_fixture_query("metadata/tron.yaml", &chunk, &query_json).unwrap();

        // Upper-case exactly those filter values whose column is case-folded.
        let mut q: serde_json::Value = serde_json::from_slice(&query_json).unwrap();
        let mut folded_keys = 0usize;
        for (request_key, value) in q.as_object_mut().unwrap() {
            if request_key == "fields" {
                continue;
            }
            let Some(items) = value.as_array_mut() else {
                continue;
            };
            for item in items {
                let Some(filters) = item.as_object_mut() else {
                    continue;
                };
                for (filter_key, v) in filters {
                    let Some(col) = resolve_filter_column(&meta, request_key, filter_key) else {
                        continue;
                    };
                    if !is_case_folded(col) {
                        continue;
                    }
                    if let Some(list) = v.as_array_mut() {
                        folded_keys += 1;
                        for e in list {
                            if let Some(s) = e.as_str() {
                                *e = serde_json::Value::String(s.to_uppercase());
                            }
                        }
                    }
                }
            }
        }
        assert!(
            folded_keys > 0,
            "tron/{}: no folded filter key found, the check proves nothing",
            query_name
        );

        let upper =
            run_fixture_query("metadata/tron.yaml", &chunk, q.to_string().as_bytes()).unwrap();
        assert_eq!(
            upper, lower,
            "tron/{}: upper-cased hex filters changed the response",
            query_name
        );

        // Anti-vacuity: a filter that matches nothing still returns the range's
        // boundary blocks header-only, so counting *blocks* proves nothing.
        // Count matched items, and confirm the filters actually narrow the
        // result — otherwise `upper == lower` would hold trivially.
        let matched = count_items(&lower);
        assert!(
            matched > 0,
            "tron/{}: fixture matched no items, the check proves nothing",
            query_name
        );

        let mut unfiltered: serde_json::Value = serde_json::from_slice(&query_json).unwrap();
        for (request_key, value) in unfiltered.as_object_mut().unwrap() {
            if request_key == "fields" {
                continue;
            }
            let Some(items) = value.as_array_mut() else {
                continue;
            };
            for item in items {
                if let Some(filters) = item.as_object_mut() {
                    filters.retain(|_, v| !v.is_array());
                }
            }
        }
        let unfiltered =
            run_fixture_query("metadata/tron.yaml", &chunk, unfiltered.to_string().as_bytes())
                .unwrap();
        assert!(
            count_items(&unfiltered) > matched,
            "tron/{}: dropping the filters did not widen the result ({} vs {}), \
             so the filters are not doing anything and folding is untested",
            query_name,
            count_items(&unfiltered),
            matched
        );
    }
}

/// Pin the exact set of case-folded Tron columns. Needs no fixtures.
///
/// Legacy passes precisely these through `to_lowercase_list`. The notable
/// absence is `_transfer_asset_contract_asset`: it is filterable, but
/// `TransferAssetTransactionRequest::predicate` passes `asset` straight to
/// `col_in_list`, so asset names compare byte-exactly. The fixture data cannot
/// catch that regression — every asset value in the chunk is digit-only hex —
/// which is why it is pinned here instead.
#[test]
fn tron_case_folded_column_set_is_exact() {
    let meta = tron_metadata();
    let mut folded: Vec<String> = meta
        .tables
        .iter()
        .flat_map(|(t, d)| {
            d.columns
                .iter()
                .filter(|(_, c)| is_case_folded(c))
                .map(move |(c, _)| format!("{}.{}", t, c))
        })
        .collect();
    folded.sort();

    let expected = vec![
        "internal_transactions.caller_address",
        "internal_transactions.transfer_to_address",
        "logs.address",
        "logs.topic0",
        "logs.topic1",
        "logs.topic2",
        "logs.topic3",
        "transactions._transfer_asset_contract_owner",
        "transactions._transfer_asset_contract_to",
        "transactions._transfer_contract_owner",
        "transactions._transfer_contract_to",
        "transactions._trigger_smart_contract_contract",
        "transactions._trigger_smart_contract_owner",
        "transactions._trigger_smart_contract_sighash",
    ];
    assert_eq!(folded, expected);
    assert!(
        !folded.contains(&"transactions._transfer_asset_contract_asset".to_string()),
        "legacy compares Tron asset names byte-exactly (no to_lowercase_list in \
         TransferAssetTransactionRequest::predicate); declaring the column hex \
         would silently diverge and no fixture would catch it"
    );
}
