//! A relation's source addresses, indexed so that a target row finds its
//! relatives — the addresses its own extends, or those that extend it — by a
//! lookup in its group rather than by comparing itself with every source of
//! its transaction.

use super::key_columns::{pair_keys, typed_key_columns, TypedKeyColumn};
use super::pairs::pack16;
use crate::integers::IntColumn;
use arrow::array::{Array, BooleanArray, GenericListArray, RecordBatch};
use arrow::buffer::BooleanBuffer;
use rustc_hash::FxHashMap;
use std::sync::Arc;

/// Mode for hierarchical address filtering.
#[derive(Clone, Copy)]
pub enum HierarchicalMode {
    /// Keep rows whose address is a strict extension of a source address (children).
    Children,
    /// Keep rows whose address is a strict prefix of a source address (parents).
    Parents,
}

/// A filter for hierarchical joins (find_children / find_parents) that can be
/// applied as a RowFilter stage, avoiding decode of data columns for non-matching rows.
#[derive(Clone)]
pub struct HierarchicalFilter {
    /// Every source address, by the group it belongs to.
    sources: Arc<SourceAddresses>,
    /// Group key column names in the target table (e.g., ["block_number", "transaction_index"]).
    pub group_key_columns: Vec<String>,
    /// Address column name in target table (e.g., "instruction_address", "call_address").
    pub address_column: String,
    /// Whether to find children or parents.
    mode: HierarchicalMode,
    /// When `true`, same-depth addresses count as a match (cross-table relations).
    /// When `false`, only strictly deeper/shallower addresses match (self-join).
    /// See `find_children` in `hierarchical.rs` for full explanation.
    inclusive: bool,
}

impl HierarchicalFilter {
    /// Build from primary scan results.
    ///
    /// - `source_address_column`: address column name in source (primary) batches
    /// - `target_address_column`: address column name in target batches (stored for scan-time use)
    /// - `inclusive`: see `find_children` in `hierarchical.rs`
    pub fn build(
        primary_batches: &[RecordBatch],
        group_key_columns: &[&str],
        source_address_column: &str,
        target_address_column: &str,
        mode: HierarchicalMode,
        inclusive: bool,
    ) -> Self {
        let index = AddressIndex::build(primary_batches, group_key_columns, source_address_column);
        index.filter(target_address_column, mode, inclusive)
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Which rows of `batch` hold an address related to a source address of
    /// their group. Only `candidates` are asked.
    pub(super) fn mask(
        &self,
        batch: &RecordBatch,
        candidates: Option<&BooleanBuffer>,
    ) -> BooleanArray {
        let (mode, inclusive) = (self.mode, self.inclusive);
        self.sources.mask(
            batch,
            &self.group_key_columns,
            &self.address_column,
            candidates,
            |sources, group, target| sources.relates(group, target, mode, inclusive),
        )
    }
}

/// The addresses of some source rows by group, built once and related to any
/// target by [`AddressIndex::filter`].
pub struct AddressIndex {
    sources: Arc<SourceAddresses>,
    group_key_columns: Vec<String>,
}

impl AddressIndex {
    pub fn build(
        primary_batches: &[RecordBatch],
        group_key_columns: &[&str],
        source_address_column: &str,
    ) -> Self {
        let sources =
            SourceAddresses::build(primary_batches, group_key_columns, source_address_column);

        AddressIndex {
            sources: Arc::new(sources),
            group_key_columns: group_key_columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Target rows whose `target_address_column` relates to a source address
    /// as `mode` and `inclusive` say.
    pub fn filter(
        &self,
        target_address_column: &str,
        mode: HierarchicalMode,
        inclusive: bool,
    ) -> HierarchicalFilter {
        HierarchicalFilter {
            sources: self.sources.clone(),
            group_key_columns: self.group_key_columns.clone(),
            address_column: target_address_column.to_string(),
            mode,
            inclusive,
        }
    }
}

/// Group ids by group key. Two integer columns, the key of every bundled
/// relation, pack into one `u128`; any other key is the bytes
/// [`TypedKeyColumn::append_to`] writes.
enum GroupIds {
    Pair(FxHashMap<u128, u32>),
    Bytes(FxHashMap<Vec<u8>, u32>),
}

impl GroupIds {
    fn len(&self) -> usize {
        match self {
            Self::Pair(ids) => ids.len(),
            Self::Bytes(ids) => ids.len(),
        }
    }

    /// The id of the group `row` belongs to, assigned on first sight.
    fn insert(
        &mut self,
        keys: &[Option<TypedKeyColumn>],
        row: usize,
        buf: &mut Vec<u8>,
    ) -> Option<u32> {
        let next = self.len() as u32;
        match self {
            Self::Pair(ids) => Some(*ids.entry(pair_key(keys, row)?).or_insert(next)),
            Self::Bytes(ids) => {
                let key = bytes_key(keys, row, buf)?;
                if let Some(&id) = ids.get(key) {
                    return Some(id);
                }
                ids.insert(key.to_vec(), next);
                Some(next)
            }
        }
    }

    /// The id of the group `row` belongs to, if any source does.
    fn get(&self, keys: &[Option<TypedKeyColumn>], row: usize, buf: &mut Vec<u8>) -> Option<u32> {
        match self {
            Self::Pair(ids) => ids.get(&pair_key(keys, row)?).copied(),
            Self::Bytes(ids) => ids.get(bytes_key(keys, row, buf)?).copied(),
        }
    }
}

/// A two-integer group key, or `None` when the row has none.
#[inline]
fn pair_key(keys: &[Option<TypedKeyColumn>], row: usize) -> Option<u128> {
    let [Some(TypedKeyColumn::Int(first)), Some(TypedKeyColumn::Int(second))] = keys else {
        return None;
    };
    let present = !first.is_null(row) && !second.is_null(row);

    present.then(|| pack16(first.join_key(row), second.join_key(row)))
}

/// A group key as bytes, or `None` when a component is missing or null.
#[inline]
fn bytes_key<'b>(
    keys: &[Option<TypedKeyColumn>],
    row: usize,
    buf: &'b mut Vec<u8>,
) -> Option<&'b [u8]> {
    buf.clear();
    let complete = keys
        .iter()
        .all(|key| matches!(key, Some(key) if key.append_to(buf, row)));

    complete.then_some(buf.as_slice())
}

/// The relation's source addresses, sorted within each group, so a target row
/// finds its relatives by binary search instead of by comparing itself with
/// every source of its transaction.
///
/// Address elements are kept as join keys, so a path compares by value
/// whatever width each side stored it at, and big-endian, so a deep path
/// compares as one run of bytes.
pub(super) struct SourceAddresses {
    groups: GroupIds,
    /// Per group id, the range of `paths` holding the group's addresses.
    ranges: Vec<std::ops::Range<u32>>,
    /// `(start, len)` into `elements`, sorted by group and then by path, with
    /// no path twice in one group.
    paths: Vec<(u32, u32)>,
    /// Each element's join key, big-endian, so that comparing two paths'
    /// bytes orders them as comparing their elements does.
    elements: Vec<u8>,
    /// For deep paths, where a binary search compares long shared prefixes:
    /// every source path by hash.
    exact: Option<PathIndex>,
    /// Every strict prefix of a deep source path by hash, which only a parent
    /// lookup needs: built the first time one asks.
    prefixes: std::sync::OnceLock<PathIndex>,
}

/// Paths a lookup finds by a hash of their group and elements, then confirms
/// by comparing the elements.
struct PathIndex {
    /// The first entry with a hash.
    first: FxHashMap<u64, u32>,
    /// `(hash, group, start, len)`, sorted by hash.
    entries: Vec<(u64, u32, u32, u32)>,
}

impl PathIndex {
    fn new(mut entries: Vec<(u64, u32, u32, u32)>) -> Self {
        entries.sort_unstable_by_key(|entry| entry.0);
        let mut first = FxHashMap::default();
        for (index, entry) in entries.iter().enumerate().rev() {
            first.insert(entry.0, index as u32);
        }

        Self { first, entries }
    }

    fn contains(&self, hash: u64, group: u32, path: &[u8], elements: &[u8]) -> bool {
        let Some(&first) = self.first.get(&hash) else {
            return false;
        };

        self.entries[first as usize..]
            .iter()
            .take_while(|entry| entry.0 == hash)
            .any(|&(_, g, start, len)| {
                g == group && &elements[start as usize * 8..(start + len) as usize * 8] == path
            })
    }
}

/// Paths deeper than this on average are looked up by hash.
const HASHED_DEPTH: usize = 4;

/// The hash of a group's empty path; [`path_hash_step`] extends it by one
/// element.
#[inline]
fn path_hash_start(group: u32) -> u64 {
    (group as u64 ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0x517c_c1b7_2722_0a95)
}

#[inline]
fn path_hash_step(hash: u64, element: &[u8; 8]) -> u64 {
    let element = u64::from_be_bytes(*element);
    (hash.rotate_left(5) ^ element).wrapping_mul(0x517c_c1b7_2722_0a95)
}

impl SourceAddresses {
    pub(super) fn build(
        batches: &[RecordBatch],
        group_key_columns: &[&str],
        address_column: &str,
    ) -> Self {
        let pair = group_key_columns.len() == 2
            && batches.iter().filter(|b| b.num_rows() > 0).all(|batch| {
                group_key_columns.iter().all(|name| {
                    batch
                        .column_by_name(name)
                        .is_some_and(|c| IntColumn::resolve(c.as_ref()).is_some())
                })
            });
        let mut groups = if pair {
            GroupIds::Pair(Default::default())
        } else {
            GroupIds::Bytes(Default::default())
        };

        let mut entries: Vec<(u32, u32, u32)> = Vec::new();
        let mut elements: Vec<u8> = Vec::new();
        let mut buf = Vec::new();
        for batch in batches {
            let keys = typed_key_columns(batch, group_key_columns);
            let Some((addresses, values)) = address_list(batch, address_column) else {
                continue;
            };
            let offsets = addresses.value_offsets();
            let element_bytes = values.join_key_bytes();
            let pairs = pair_keys(batch, group_key_columns);
            let group_of =
                |groups: &mut GroupIds, row: usize, buf: &mut Vec<u8>| match (&pairs, groups) {
                    (Some((first, second, nulls)), GroupIds::Pair(ids)) => {
                        let present = nulls.as_ref().is_none_or(|n| n.is_valid(row));
                        let next = ids.len() as u32;
                        present.then(|| *ids.entry(pack16(first[row], second[row])).or_insert(next))
                    }
                    (_, groups) => groups.insert(&keys, row, buf),
                };

            for row in 0..batch.num_rows() {
                // A null address is not the empty one, which is the root call.
                if addresses.is_null(row) {
                    continue;
                }
                let Some(group) = group_of(&mut groups, row, &mut buf) else {
                    continue;
                };

                let start = (elements.len() / 8) as u32;
                let path = offsets[row] as usize * 8..offsets[row + 1] as usize * 8;
                elements.extend_from_slice(&element_bytes[path]);
                entries.push((group, start, (elements.len() / 8) as u32 - start));
            }
        }

        // Group ids are dense, so one counting pass orders the paths by group,
        // and only each group's few paths are compared.
        let mut starts = vec![0u32; groups.len() + 1];
        for &(group, ..) in &entries {
            starts[group as usize + 1] += 1;
        }
        for group in 0..groups.len() {
            starts[group + 1] += starts[group];
        }
        let mut ordered = vec![(0u32, 0u32); entries.len()];
        let mut next = starts.clone();
        for &(group, start, len) in &entries {
            ordered[next[group as usize] as usize] = (start, len);
            next[group as usize] += 1;
        }

        let path =
            |&(start, len): &(u32, u32)| &elements[start as usize * 8..(start + len) as usize * 8];
        let mut ranges = Vec::with_capacity(groups.len());
        let mut paths = Vec::with_capacity(ordered.len());
        for group in 0..groups.len() {
            let own = &mut ordered[starts[group] as usize..starts[group + 1] as usize];
            own.sort_unstable_by(|a, b| path(a).cmp(path(b)));
            let first = paths.len() as u32;
            for &entry in own.iter() {
                let repeated =
                    paths.len() as u32 > first && path(paths.last().unwrap()) == path(&entry);
                if !repeated {
                    paths.push(entry);
                }
            }
            ranges.push(first..paths.len() as u32);
        }

        let deep =
            paths.iter().map(|&(_, len)| len as usize).sum::<usize>() > HASHED_DEPTH * paths.len();
        let mut addresses = SourceAddresses {
            groups,
            ranges,
            paths,
            elements,
            exact: None,
            prefixes: std::sync::OnceLock::new(),
        };
        if deep {
            addresses.exact = Some(addresses.exact_index());
        }
        addresses
    }

    /// Every source path by its hash.
    fn exact_index(&self) -> PathIndex {
        let mut entries = Vec::with_capacity(self.paths.len());
        self.walk(|group, (start, len), _, hashes| {
            entries.push((hashes[len as usize], group, start, len));
        });

        PathIndex::new(entries)
    }

    /// Every distinct strict prefix of a source path by its hash.
    fn prefix_index(&self) -> PathIndex {
        let mut entries = Vec::new();
        self.walk(|group, (start, len), new_from, hashes| {
            let strict = hashes[..len as usize].iter().enumerate().skip(new_from);
            entries.extend(strict.map(|(depth, &hash)| (hash, group, start, depth as u32)));
        });

        PathIndex::new(entries)
    }

    /// Calls `visit` with each source path's group, its place in `elements`,
    /// the depth from which its strict prefixes are not a strict prefix of a
    /// path before it, and the hash of each of its prefixes, itself included.
    ///
    /// The paths of a group are sorted, so a prefix shared by several is the
    /// prefix of each in a run: it is new only where it is longer than what the
    /// path shares with the one before, or is that whole path. The hashes of
    /// the shared part are the ones before's.
    fn walk(&self, mut visit: impl FnMut(u32, (u32, u32), usize, &[u64])) {
        let mut hashes = Vec::new();
        for (group, range) in self.ranges.iter().enumerate() {
            let group = group as u32;
            hashes.clear();
            hashes.push(path_hash_start(group));

            let mut previous: &[[u8; 8]] = &[];
            for &(start, len) in &self.paths[range.start as usize..range.end as usize] {
                let (path, _) = self.path((start, len)).as_chunks::<8>();
                let shared = previous
                    .iter()
                    .zip(path)
                    .take_while(|(a, b)| a == b)
                    .count();
                let new_from = if shared == previous.len() {
                    shared
                } else {
                    shared + 1
                };

                hashes.truncate(shared + 1);
                for element in &path[shared..] {
                    let last = hashes[hashes.len() - 1];
                    hashes.push(path_hash_step(last, element));
                }
                visit(group, (start, len), new_from, &hashes);
                previous = path;
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Which rows of `batch` hold an address that `related` accepts for their
    /// group: an address column of integer paths, grouped by
    /// `group_key_columns`. Only `candidates` are asked.
    pub(super) fn mask(
        &self,
        batch: &RecordBatch,
        group_key_columns: &[impl AsRef<str>],
        address_column: &str,
        candidates: Option<&BooleanBuffer>,
        related: impl Fn(&Self, u32, &[u8]) -> bool,
    ) -> BooleanArray {
        let len = batch.num_rows();
        let Some((addresses, values)) = address_list(batch, address_column) else {
            return BooleanArray::new(BooleanBuffer::new_unset(len), None);
        };
        let keys = typed_key_columns(batch, group_key_columns);
        let pairs = match &self.groups {
            GroupIds::Pair(_) => pair_keys(batch, group_key_columns),
            GroupIds::Bytes(_) => None,
        };
        let offsets = addresses.value_offsets();
        let element_bytes = values.join_key_bytes();

        let mut buf = Vec::new();
        let found = BooleanBuffer::collect_bool(len, |row| {
            let candidate = candidates.is_none_or(|c| c.value(row)) && !addresses.is_null(row);
            let group = match (&pairs, &self.groups) {
                _ if !candidate => None,
                (Some((first, second, nulls)), GroupIds::Pair(ids)) => nulls
                    .as_ref()
                    .is_none_or(|n| n.is_valid(row))
                    .then(|| ids.get(&pack16(first[row], second[row])).copied())
                    .flatten(),
                (_, groups) => groups.get(&keys, row, &mut buf),
            };

            group.is_some_and(|group| {
                let path = offsets[row] as usize * 8..offsets[row + 1] as usize * 8;
                related(self, group, &element_bytes[path])
            })
        });
        BooleanArray::new(found, None)
    }

    fn path(&self, (start, len): (u32, u32)) -> &[u8] {
        &self.elements[start as usize * 8..(start + len) as usize * 8]
    }

    /// Whether `target` is a source address of `group`.
    pub(super) fn holds(&self, group: u32, target: &[u8]) -> bool {
        if let Some(exact) = &self.exact {
            let (elements, _) = target.as_chunks::<8>();
            let hash = elements.iter().fold(path_hash_start(group), path_hash_step);
            return exact.contains(hash, group, target, &self.elements);
        }

        let range = &self.ranges[group as usize];
        let paths = &self.paths[range.start as usize..range.end as usize];

        paths
            .binary_search_by(|&path| self.path(path).cmp(target))
            .is_ok()
    }

    /// Whether `target` is related to a source address of `group`.
    fn relates(&self, group: u32, target: &[u8], mode: HierarchicalMode, inclusive: bool) -> bool {
        if let Some(exact) = &self.exact {
            let elements = &self.elements;
            let (steps, _) = target.as_chunks::<8>();
            return match mode {
                // A source that is a prefix of the target, strict unless inclusive.
                HierarchicalMode::Children => {
                    let depth = steps.len();
                    let mut hash = path_hash_start(group);
                    let mut found = false;
                    for d in 0..=depth {
                        if d == depth && !inclusive {
                            break;
                        }
                        if exact.contains(hash, group, &target[..d * 8], elements) {
                            found = true;
                            break;
                        }
                        if d < depth {
                            hash = path_hash_step(hash, &steps[d]);
                        }
                    }
                    found
                }
                // A source the target is a strict prefix of, or equal to when
                // inclusive.
                HierarchicalMode::Parents => {
                    let prefixes = self.prefixes.get_or_init(|| self.prefix_index());
                    let hash = steps.iter().fold(path_hash_start(group), path_hash_step);
                    prefixes.contains(hash, group, target, elements)
                        || (inclusive && exact.contains(hash, group, target, elements))
                }
            };
        }

        let range = &self.ranges[group as usize];
        let paths = &self.paths[range.start as usize..range.end as usize];

        match mode {
            // A parent is one of the target's prefixes, one lookup per depth.
            HierarchicalMode::Children => {
                let depth = target.len() / 8;
                let depths = if inclusive { depth + 1 } else { depth };
                (0..depths).any(|depth| {
                    paths
                        .binary_search_by(|&path| self.path(path).cmp(&target[..depth * 8]))
                        .is_ok()
                })
            }
            // The addresses that extend the target sort right after it.
            HierarchicalMode::Parents => {
                let first = paths.partition_point(|&path| self.path(path) < target);
                let mut extensions = paths[first..].iter().map(|&path| self.path(path));

                match extensions.next() {
                    Some(path) if path == target => {
                        inclusive || extensions.next().is_some_and(|p| p.starts_with(target))
                    }
                    Some(path) => path.starts_with(target),
                    None => false,
                }
            }
        }
    }
}

/// A hierarchical address column and its elements. `None` where the column is
/// absent or its elements are not integers: an address is a path of item
/// indices, so anything else is a chunk that disagrees with its catalog, and
/// nothing matches.
pub(super) fn address_list<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Option<(&'a GenericListArray<i32>, IntColumn<'a>)> {
    let addresses = batch
        .column_by_name(column)?
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()?;
    let values = IntColumn::resolve(addresses.values().as_ref())?;

    Some((addresses, values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::keys::{CompositeKeySet, KeyFilter};
    use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, UInt32Array};

    /// The address index answers what comparing a target with every source of
    /// its group answers, in both modes, inclusive or not, over random groups,
    /// paths of any depth including the root, repeated sources, null keys and
    /// addresses, element widths that differ between the two sides, and keys
    /// that pack into a pair and keys that do not.
    #[test]
    fn the_address_index_relates_what_a_full_comparison_relates() {
        use arrow::datatypes::{UInt16Type, UInt32Type};

        type Row = (Option<u32>, Option<u32>, Option<Vec<u32>>);

        let mut seed = 0x5EED_0049u64;
        let mut next = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };

        // Deep paths are looked up by hash, shallow ones by binary search.
        let rows = |count: usize, deep: bool, next: &mut dyn FnMut(u64) -> u64| -> Vec<Row> {
            (0..count)
                .map(|_| {
                    let groups = if deep { 2 } else { 3 };
                    let key = |next: &mut dyn FnMut(u64) -> u64| {
                        (next(10) > 0).then(|| next(groups) as u32)
                    };
                    let block = key(next);
                    let tx = key(next);
                    let address = (next(10) > 0).then(|| {
                        let depth = if deep { 4 + next(7) } else { next(4) } as usize;
                        (0..depth)
                            .map(|_| next(if deep { 2 } else { 3 }) as u32)
                            .collect()
                    });
                    (block, tx, address)
                })
                .collect()
        };

        let batch = |rows: &[Row], wide: bool, text_key: bool| -> RecordBatch {
            let blocks: ArrayRef = Arc::new(Int32Array::from_iter(
                rows.iter().map(|r| r.0.map(|v| v as i32)),
            ));
            let txs: ArrayRef = if text_key {
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|r| r.1.map(|v| v.to_string())),
                ))
            } else {
                Arc::new(UInt32Array::from_iter(rows.iter().map(|r| r.1)))
            };
            let paths = rows.iter().map(|r| {
                r.2.as_ref()
                    .map(|path| path.iter().map(|&v| Some(v)).collect::<Vec<_>>())
            });
            let addresses: ArrayRef = if wide {
                Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(paths))
            } else {
                let paths = paths.map(|p| p.map(|p| p.into_iter().map(|v| v.map(|v| v as u16))));
                Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(paths))
            };
            RecordBatch::try_from_iter(vec![("block", blocks), ("tx", txs), ("address", addresses)])
                .unwrap()
        };

        let related = |target: &[u32], source: &[u32], mode, inclusive: bool| match mode {
            HierarchicalMode::Children => {
                let deep_enough = if inclusive {
                    target.len() >= source.len()
                } else {
                    target.len() > source.len()
                };
                deep_enough && target.starts_with(source)
            }
            HierarchicalMode::Parents => {
                let deep_enough = if inclusive {
                    source.len() >= target.len()
                } else {
                    source.len() > target.len()
                };
                deep_enough && source.starts_with(target)
            }
        };

        let mut seen_both = [false; 2];
        let mut seen_hashed = [false; 2];
        for case in 0..800 {
            let deep = case % 16 >= 8;
            let sources = rows(1 + next(30) as usize, deep, &mut next);
            let mut targets = rows(1 + next(30) as usize, deep, &mut next);
            // Half the targets cut or extend a source's path, which is where a
            // prefix the index left out would show.
            for target in targets.iter_mut() {
                if next(2) == 0 {
                    continue;
                }
                let source = &sources[next(sources.len() as u64) as usize];
                let Some(path) = &source.2 else { continue };
                let mut path = path[..next(path.len() as u64 + 1) as usize].to_vec();
                if next(3) == 0 {
                    path.push(next(2) as u32);
                }
                *target = (source.0, source.1, Some(path));
            }
            let mode = if case % 2 == 0 {
                HierarchicalMode::Children
            } else {
                HierarchicalMode::Parents
            };
            let inclusive = case % 4 < 2;
            let text_key = case % 8 >= 6;

            let source_batch = batch(&sources, true, text_key);
            let target_batch = batch(&targets, case % 3 == 0, text_key);
            let filter = HierarchicalFilter::build(
                &[source_batch.slice(0, sources.len())],
                &["block", "tx"],
                "address",
                "address",
                mode,
                inclusive,
            );
            let mask = filter.mask(&target_batch, None);
            seen_hashed[usize::from(filter.sources.exact.is_some())] = true;
            // Only a parent lookup needs every prefix of a source path.
            let parents = matches!(mode, HierarchicalMode::Parents);
            if !parents || filter.sources.exact.is_none() {
                assert!(filter.sources.prefixes.get().is_none(), "case {case}");
            }

            // The same sources as exact keys: a target matches one that has
            // its block, its transaction and its whole path.
            let keys = ["block", "tx", "address"];
            let exact = KeyFilter::build(
                &[source_batch.slice(0, sources.len())],
                &keys,
                &keys,
                "block",
                "block",
            );
            let matched = exact.mask(&target_batch, None).unwrap();
            if let CompositeKeySet::PairPath(sources) = exact.key_set() {
                assert!(
                    sources.prefixes.get().is_none(),
                    "a join built prefixes, case {case}"
                );
            }
            for (row, target) in targets.iter().enumerate() {
                let expected = target.0.is_some()
                    && target.1.is_some()
                    && target.2.is_some()
                    && sources.contains(target);
                assert_eq!(
                    matched.value(row),
                    expected,
                    "case {case}, exact key {target:?}"
                );
            }

            for (row, (block, tx, address)) in targets.iter().enumerate() {
                let expected = match (block, tx, address) {
                    (Some(block), Some(tx), Some(target)) => sources.iter().any(|source| {
                        source.0 == Some(*block)
                            && source.1 == Some(*tx)
                            && source
                                .2
                                .as_ref()
                                .is_some_and(|s| related(target, s, mode, inclusive))
                    }),
                    _ => false,
                };
                seen_both[usize::from(expected)] = true;
                assert_eq!(
                    mask.value(row),
                    expected,
                    "case {case}, target {row} {:?} against {sources:?}",
                    targets[row]
                );
            }
        }
        assert_eq!(seen_both, [true, true], "every answer was the same");
        assert_eq!(
            seen_hashed,
            [true, true],
            "one way of finding paths went untested"
        );
    }
}
