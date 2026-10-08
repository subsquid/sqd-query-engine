//! Sets of two-integer join keys: a block number and an item's index in the
//! block, the key of nearly every relation. A relation scan asks one of these
//! about every row of its target table.

use rustc_hash::FxHashSet;

/// Pack two join keys into one (bijective; build and probe must agree).
#[inline(always)]
pub(super) fn pack16(a: u64, b: u64) -> u128 {
    ((a as u128) << 64) | (b as u128)
}

pub(super) enum PairSet {
    Dense(DensePairs),
    Hashed(FxHashSet<u128>),
}

impl PairSet {
    pub(super) fn new(mut pairs: Vec<(u64, u64)>) -> Self {
        pairs.sort_unstable();
        pairs.dedup();

        match DensePairs::build(&pairs) {
            Some(dense) => Self::Dense(dense),
            None => Self::Hashed(pairs.iter().map(|&(a, b)| pack16(a, b)).collect()),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Dense(dense) => dense.len == 0,
            Self::Hashed(set) => set.is_empty(),
        }
    }

    #[inline]
    pub(super) fn contains(&self, a: u64, b: u64) -> bool {
        match self {
            Self::Dense(dense) => dense.contains(a, b),
            Self::Hashed(set) => set.contains(&pack16(a, b)),
        }
    }
}

/// A bitmap of second components for each first component, when the bitmaps
/// and their slots are no larger than the pairs' keys alone: a window of blocks
/// and the item indices within each.
pub(super) struct DensePairs {
    first: u64,
    /// By first component, offset from `first`.
    slots: Vec<Slot>,
    bits: Vec<u64>,
    len: usize,
}

/// Where a first component's bitmap starts in `bits`, the second component its
/// first bit stands for, and how many bits it has.
#[derive(Clone, Copy, Default)]
struct Slot {
    start: u64,
    low: u64,
    width: u64,
}

/// First components one set may span.
const MAX_SLOTS: u64 = 1 << 16;

/// The bytes of one pair's key, the least a hash set holds per pair.
const KEY_BYTES: u64 = 16;

impl DensePairs {
    /// `None` when the pairs are too sparse for bitmaps. `pairs` is sorted
    /// and unique.
    fn build(pairs: &[(u64, u64)]) -> Option<Self> {
        let (Some(&(first, _)), Some(&(last, _))) = (pairs.first(), pairs.last()) else {
            return Some(Self {
                first: 0,
                slots: Vec::new(),
                bits: Vec::new(),
                len: 0,
            });
        };
        if last - first >= MAX_SLOTS {
            return None;
        }
        let span = last - first + 1;
        let width = |group: &[(u64, u64)]| (group[group.len() - 1].1 - group[0].1).checked_add(1);

        // Sized before anything is allocated.
        let mut total = 0u64;
        for group in pairs.chunk_by(|x, y| x.0 == y.0) {
            total = total.checked_add(width(group)?)?;
        }
        let slot_bytes = span.saturating_mul(std::mem::size_of::<Slot>() as u64);
        let dense_bytes = slot_bytes.saturating_add(total.div_ceil(64).saturating_mul(8));
        if dense_bytes > (pairs.len() as u64).saturating_mul(KEY_BYTES) {
            return None;
        }

        let mut slots = vec![Slot::default(); span as usize];
        let mut start = 0u64;
        for group in pairs.chunk_by(|x, y| x.0 == y.0) {
            let width = width(group).expect("checked above");
            slots[(group[0].0 - first) as usize] = Slot {
                start,
                low: group[0].1,
                width,
            };
            start += width;
        }

        let mut bits = vec![0u64; total.div_ceil(64) as usize];
        for &(a, b) in pairs {
            let slot = slots[(a - first) as usize];
            let bit = slot.start + (b - slot.low);
            bits[(bit >> 6) as usize] |= 1 << (bit & 63);
        }

        Some(Self {
            first,
            slots,
            bits,
            len: pairs.len(),
        })
    }

    #[inline]
    fn contains(&self, a: u64, b: u64) -> bool {
        let Some(slot) = usize::try_from(a.wrapping_sub(self.first))
            .ok()
            .and_then(|index| self.slots.get(index))
        else {
            return false;
        };
        let offset = b.wrapping_sub(slot.low);
        if offset >= slot.width {
            return false;
        }

        let bit = slot.start + offset;
        (self.bits[(bit >> 6) as usize] >> (bit & 63)) & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn agrees(pairs: Vec<(u64, u64)>, probes: Vec<(u64, u64)>) {
        let set = PairSet::new(pairs.clone());
        let oracle: FxHashSet<(u64, u64)> = pairs.into_iter().collect();

        assert_eq!(set.is_empty(), oracle.is_empty());
        for (a, b) in oracle.iter().copied().chain(probes) {
            assert_eq!(set.contains(a, b), oracle.contains(&(a, b)), "({a}, {b})");
        }
    }

    #[test]
    fn edges_of_the_key_space_are_members_or_not() {
        let max = u64::MAX;
        agrees(vec![], vec![(0, 0), (max, max)]);
        agrees(vec![(0, 0)], vec![(0, 1), (1, 0), (max, 0), (0, max)]);
        agrees(
            vec![(max, max)],
            vec![(max, max - 1), (max - 1, max), (0, 0)],
        );
        agrees(
            vec![(5, 0), (5, max)],
            vec![(5, 1), (5, max - 1), (4, 0), (6, max)],
        );
        agrees(vec![(0, 7), (max, 7)], vec![(1, 7), (max - 1, 7)]);
        // Sign-extended keys of negative values sit at the top of the space.
        agrees(
            vec![(max - 2, 3), (max, 1)],
            vec![(max - 1, 3), (max, 3), (2, 3)],
        );
    }

    #[test]
    fn a_window_of_blocks_is_dense_and_a_wide_one_is_hashed() {
        let window: Vec<_> = (1000..1016)
            .flat_map(|b| (0..200).map(move |t| (b, t)))
            .collect();
        assert!(matches!(PairSet::new(window), PairSet::Dense(_)));

        let wide = vec![(0, 0), (MAX_SLOTS, 0)];
        assert!(matches!(PairSet::new(wide), PairSet::Hashed(_)));

        let sparse = vec![(0, 0), (0, 1 << 22)];
        assert!(matches!(PairSet::new(sparse), PairSet::Hashed(_)));
    }

    /// The heap a dense set holds.
    fn dense_bytes(dense: &DensePairs) -> usize {
        dense.slots.len() * std::mem::size_of::<Slot>() + dense.bits.len() * 8
    }

    /// Two keys need a few bytes in a hash set, whether their blocks or their
    /// indices are far apart, and the choice is made before the bitmaps are.
    #[test]
    fn a_few_keys_far_apart_are_hashed_without_building_bitmaps() {
        let far = [
            vec![(0, 0), (MAX_SLOTS - 1, 0)],
            vec![(7, 0), (7, (1 << 20) - 1)],
            vec![(0, 0), (MAX_SLOTS - 1, (1 << 20) - 1)],
        ];
        for pairs in far {
            let (set, peak) = crate::testing::peak_bytes(|| PairSet::new(pairs.clone()));
            assert!(matches!(set, PairSet::Hashed(_)), "{pairs:?}");
            assert!(peak < 4096, "{peak} bytes to choose a set of {pairs:?}");
        }
    }

    proptest! {
        /// A dense set is never larger than its keys alone, the least a hash
        /// set of them holds.
        #[test]
        fn a_dense_set_is_no_larger_than_its_keys(
            pairs in prop::collection::vec((0u64..(MAX_SLOTS + 8), 0u64..(1 << 21)), 1..64),
            window in prop::collection::vec((0u64..16, 0u64..400), 0..2000),
        ) {
            for pairs in [pairs, window] {
                let mut unique = pairs.clone();
                unique.sort_unstable();
                unique.dedup();
                if let PairSet::Dense(dense) = PairSet::new(pairs) {
                    prop_assert!(dense_bytes(&dense) <= unique.len() * 16);
                }
            }
        }

        #[test]
        fn a_dense_window_answers_like_a_set(
            base in prop_oneof![Just(0u64), Just(u64::MAX - 40), any::<u64>().prop_map(|v| v / 2)],
            pairs in prop::collection::vec((0u64..32, 0u64..400), 300..2000),
            probes in prop::collection::vec((0u64..40, 0u64..500), 0..400),
        ) {
            let shift = |(a, b): (u64, u64)| (base.wrapping_add(a), b);
            let pairs: Vec<_> = pairs.into_iter().map(shift).collect();
            prop_assert!(matches!(PairSet::new(pairs.clone()), PairSet::Dense(_)));
            agrees(pairs, probes.into_iter().map(shift).collect());
        }

        #[test]
        fn any_pairs_answer_like_a_set(
            pairs in prop::collection::vec((any::<u64>(), any::<u64>()), 0..64),
            probes in prop::collection::vec((any::<u64>(), any::<u64>()), 0..64),
        ) {
            agrees(pairs, probes);
        }
    }
}
