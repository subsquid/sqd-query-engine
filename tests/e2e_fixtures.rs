//! End-to-end fixture tests: run queries against parquet chunks and compare
//! output with expected results from the legacy engine.
//!
//! Fixture data is supplied externally under `tests/fixtures/`. These tests are
//! ignored by the portable suite and selected by `make test-data`.
//!
//! Once the tree *is* there, a declared fixture that is missing from it is a
//! failure, not a skip: a test that quietly does nothing reads exactly like a
//! test that passes.

use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::path::{Path, PathBuf};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn run_fixture_query(
    meta_path: &str,
    chunk_dir: &Path,
    query_json: &[u8],
) -> anyhow::Result<serde_json::Value> {
    let meta = load_dataset_description(Path::new(meta_path))?;
    let parsed = parse_query(query_json, &meta)?;
    let plan = compile(&parsed, &meta)?;
    let mut result = Vec::new();
    if let Some(mut blocks) = execute_plan(&plan, &meta, chunk_dir)? {
        let mut buf = Vec::new();
        while blocks.has_next_block() {
            buf.clear();
            blocks.write_next_block(&mut buf);
            result.push(serde_json::from_slice(&buf)?);
        }
    }
    Ok(serde_json::Value::Array(result))
}

/// Whether the external fixture tree is available. The data-backed target sets
/// `SQD_REQUIRE_FIXTURES=1`, making an absent tree a failure.
fn fixture_tree_is_present() -> bool {
    if fixture_dir().join("ethereum").join("chunk").is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but tests/fixtures is not checked out, so the \
         fixture suite would report green having compared nothing"
    );

    false
}

fn test_fixture(dataset: &str, meta_path: &str, query_name: &str) {
    if !fixture_tree_is_present() {
        return;
    }

    let base = fixture_dir().join(dataset);
    let chunk = base.join("chunk");
    let query_file = base.join("queries").join(query_name).join("query.json");
    let result_file = base.join("queries").join(query_name).join("result.json");

    assert!(
        chunk.exists(),
        "{dataset}/{query_name}: no chunk at {chunk:?}"
    );
    assert!(
        query_file.exists(),
        "{dataset}/{query_name}: no query.json at {query_file:?}"
    );

    let query_json = std::fs::read(&query_file).unwrap();
    let actual =
        match std::panic::catch_unwind(|| run_fixture_query(meta_path, &chunk, &query_json)) {
            // A query error is a legitimate result: legacy serializes it as a bare
            // JSON string, and some fixtures expect exactly that (e.g. requesting a
            // schema column that's absent from the parquet). Use the root cause so
            // the message matches legacy's `err.to_string()` without anyhow context.
            Ok(Ok(v)) => v,
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
        // Nothing to compare against is not a passing test. Write what we
        // produced so it can be reviewed and promoted to result.json.
        let actual_path = base
            .join("queries")
            .join(query_name)
            .join("actual.temp.json");
        serde_json::to_writer_pretty(std::fs::File::create(&actual_path).unwrap(), &actual)
            .unwrap();
        panic!("{dataset}/{query_name}: no result.json; wrote actual to {actual_path:?}");
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
            fn [<evm_ $name>]() {
                test_fixture("ethereum", "metadata/evm.yaml", stringify!($name));
            }
        }
    };
    // Allow test name different from directory name
    ($name:ident, $dir:expr) => {
        paste::paste! {
            #[test]
            #[ignore = "requires external fixture data"]
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

evm_fixture!(transaction_traces_for_traces);

// ---------------------------------------------------------------------------
// Bitcoin fixtures
// ---------------------------------------------------------------------------

macro_rules! bitcoin_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            #[ignore = "requires external fixture data"]
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
// Optimism fixtures (uses EVM metadata)
// ---------------------------------------------------------------------------

macro_rules! optimism_fixture {
    ($name:ident) => {
        paste::paste! {
            #[test]
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
            #[ignore = "requires external fixture data"]
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
// Suite integrity
// ---------------------------------------------------------------------------

/// The suite is only as good as the fixtures behind it, and a declaration with
/// no data on disk used to print SKIP and pass. This walks the declarations in
/// this file against the fixture tree in both directions, so neither a test
/// without data nor data without a test can go unnoticed.
///
/// Datasets with no catalog entry yet are listed as exemptions: their fixtures
/// are on disk waiting for the dataset to be supported, and the list is meant to
/// shrink.
#[test]
#[ignore = "requires external fixture data"]
fn fixture_declarations_match_the_fixture_tree() {
    // Datasets the engine does not serve: `tron` was never ported, `fuel` was
    // dropped. Their fixtures stay in the shared tree for the reference
    // implementation and are nothing this suite should account for.
    const UNSUPPORTED_DATASETS: &[&str] = &["tron", "fuel"];

    if !fixture_tree_is_present() {
        return;
    }

    let source = include_str!("e2e_fixtures.rs");

    // macro name → fixture directory, read from each macro's own body.
    let mut macro_dataset: Vec<(String, String)> = Vec::new();
    for (index, line) in source.lines().enumerate() {
        let Some(rest) = line.trim().strip_prefix("macro_rules! ") else {
            continue;
        };
        let Some(name) = rest.strip_suffix(" {") else {
            continue;
        };
        let dataset = source
            .lines()
            .skip(index)
            .take(12)
            .find_map(|l| {
                l.split_once("test_fixture(\"")?
                    .1
                    .split_once('\"')
                    .map(|p| p.0)
            })
            .unwrap_or_else(|| panic!("macro {name} has no test_fixture call"));
        macro_dataset.push((name.to_string(), dataset.to_string()));
    }
    assert!(!macro_dataset.is_empty(), "found no fixture macros");

    // Declared fixture directories, per dataset. Both macro forms are used:
    // `m!(name)` takes the directory from the name, `m!(name, "dir")` names it.
    // Read from a whitespace-flattened copy of the source, because rustfmt wraps
    // a long invocation across lines and a line-by-line reader would then report
    // the fixture it declares as undeclared.
    let flat = source.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut declared: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        Default::default();

    for (macro_name, dataset) in &macro_dataset {
        let call = format!("{macro_name}!(");
        let mut rest = flat.as_str();

        while let Some(at) = rest.find(&call) {
            let after = &rest[at + call.len()..];
            let Some(end) = after.find(')') else { break };
            let args = &after[..end];
            rest = &after[end..];

            // A `$name` here is the macro's own definition, not a call site.
            if args.contains('$') {
                continue;
            }

            let dir = match args.split_once(',') {
                Some((_, explicit)) => explicit.trim().trim_matches('"').to_string(),
                None => args.trim().to_string(),
            };
            declared.entry(dataset.clone()).or_default().insert(dir);
        }
    }

    let root = fixture_dir();
    let mut problems = Vec::new();

    for (dataset, dirs) in &declared {
        for dir in dirs {
            let query = root
                .join(dataset)
                .join("queries")
                .join(dir)
                .join("query.json");
            if !query.exists() {
                problems.push(format!(
                    "{dataset}/{dir}: declared, but no query.json on disk"
                ));
            }
        }
    }

    for entry in std::fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        let dataset = path.file_name().unwrap().to_string_lossy().to_string();
        let queries = path.join("queries");
        if !queries.is_dir() || UNSUPPORTED_DATASETS.contains(&dataset.as_str()) {
            continue;
        }
        let known = declared.get(&dataset).cloned().unwrap_or_default();
        for query_entry in std::fs::read_dir(&queries).unwrap() {
            let name = query_entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .to_string();
            if !known.contains(&name) {
                problems.push(format!(
                    "{dataset}/{name}: fixture on disk, but no test declares it"
                ));
            }
        }
    }

    assert!(
        problems.is_empty(),
        "fixture suite is out of step with the fixture tree:\n  {}",
        problems.join("\n  ")
    );
}
