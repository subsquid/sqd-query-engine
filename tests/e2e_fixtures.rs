//! End-to-end fixture tests: run queries against parquet chunks and compare
//! output with expected results from the legacy engine.
//!
//! Fixture data is expected at `tests/fixtures/` (symlink to legacy repo's
//! `crates/query/fixtures/`). Tests are skipped if fixtures are not present.

use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::path::{Path, PathBuf};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn run_fixture_query(meta_path: &str, chunk_dir: &Path, query_json: &[u8]) -> serde_json::Value {
    let meta = load_dataset_description(Path::new(meta_path)).unwrap();
    let parsed = parse_query(query_json, &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let result = execute_plan(&plan, &meta, chunk_dir, Vec::new()).unwrap();
    serde_json::from_slice(&result).unwrap()
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
    let actual =
        match std::panic::catch_unwind(|| run_fixture_query(meta_path, &chunk, &query_json)) {
            Ok(v) => v,
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
