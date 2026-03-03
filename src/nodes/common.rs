use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Generates a stable i64 hash from a string.
/// Used for deterministic entity ID generation.
pub fn hash_string_to_i64(s: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() as i64
}
