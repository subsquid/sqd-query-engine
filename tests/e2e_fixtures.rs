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
