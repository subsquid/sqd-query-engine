//! CT-4 — relations.
//!
//! Two halves. `joins` writes chunks that disagree with their catalog about the
//! join key, which is the shape no generator will stumble on and no fixture tree
//! can hold. `laws` asserts §8.5's table — a widening, a dedup, an idempotence,
//! one hop — over queries HC-4 composed, because each of those is a claim about
//! a *pair* of queries and a hand-written case only ever asserts one pair of it.

mod joins;
mod laws;
