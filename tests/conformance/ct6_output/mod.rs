//! CT-6 — output and determinism.
//!
//! Two questions that look alike and are not. `encoding` asks what a value
//! renders as, which the catalog decides. `determinism` asks whether the answer
//! moves when something that is not the question moves — the physical width a
//! column is stored at, the row groups it is split into, the order the rows sit
//! in on disk, the number of threads that read them.
//!
//! The second half is the one HC-3 was built for: every test in it writes the
//! same logical chunk a different way and asserts the response is byte-identical.

mod determinism;
mod encoding;
