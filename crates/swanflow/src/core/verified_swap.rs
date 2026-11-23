//! Verified swap — marked-block text replacement gated by a source hash.
//!
//! Pattern origin: fast-anything (github.com/EL4CTEO/fast-anything, reviewed
//! 2026-11-23, claw wiki #2026112311). Its enable/disable cycle keeps three
//! invariants that any "generated artifact may only go live after it passed a
//! mechanical gate" flow needs, so they are encoded here verbatim:
//!
//! 1. **Append-only swap.** Enabling generated code never edits the original
//!    region: a marked block is appended after a blank line and can be
//!    removed byte-for-byte (fast-anything's CI asserts this with
//!    `git diff --exit-code` after disable).
//! 2. **Hash-sealed gate.** The block may only be (re)enabled while its
//!    sources are still the ones verification checked; an FNV-1a hash over
//!    the source tree detects drift (fast-anything issues #26/#67: build and
//!    enable could bypass verify by editing the Rust after it passed).
//! 3. **Corruption is an error, never a silent success.** A begin marker
//!    without its end marker refuses to strip instead of reporting success
//!    (fast-anything issue #69: `disable` reported success on incomplete
//!    blocks).
//!
//! Non-goals (kept out until a call site needs them): inserting blocks at an
//! arbitrary offset (fast-anything's `insert_marked` for main-guards), and
//! any form of marker escaping — bodies must not contain their own marker
//! lines at line start.

use std::path::Path;

// ------------------------------------------------------------------ marked blocks

/// Marker pair for `id`, prefixed with the file's line comment (`#`, `--`,
/// `//`, `REM`, …).
pub fn markers(comment: &str, id: &str) -> (String, String) {
    (
        format!("{comment} >>> swanflow-swap: {id} >>>"),
        format!("{comment} <<< swanflow-swap: {id} <<<"),
    )
}

/// The file's newline style: CRLF anywhere wins, like the reference impl.
fn newline(text: &str) -> &'static str {
    if text.contains("\r\n") {
        "\r\n"
    } else {
        "\n"
    }
}

/// Byte offset of `needle` only where it starts a line (position 0 or right
/// after a `\n`). Substring matches inside a line are ignored, so a body that
/// merely mentions a marker mid-line cannot confuse stripping.
fn line_anchored_find(text: &str, needle: &str, from: usize) -> Option<usize> {
    let mut search_from = from;
    while let Some(rel) = text[search_from..].find(needle) {
        let at = search_from + rel;
        let at_line_start = at == 0 || text[..at].ends_with('\n');
        if at_line_start {
            return Some(at);
        }
        search_from = at + needle.len();
    }
    None
}

/// Does a block for `id` exist (begin marker at a line start)?
pub fn has_block(text: &str, comment: &str, id: &str) -> bool {
    let (begin, _) = markers(comment, id);
    line_anchored_find(text, &begin, 0).is_some()
}

/// Outcome of [`strip_block`]: the payload is always a usable text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripOutcome {
    /// The block was present and removed; byte-for-byte restore.
    Restored(String),
    /// No block with this id existed; the original text, unchanged.
    NotPresent(String),
}

/// Remove the block for `id`. Invariant 3: a begin marker whose end marker is
/// missing is corruption and returns `Err` — never a silent success.
pub fn strip_block(text: &str, comment: &str, id: &str) -> Result<StripOutcome, String> {
    let (begin, end) = markers(comment, id);
    let Some(start) = line_anchored_find(text, &begin, 0) else {
        return Ok(StripOutcome::NotPresent(text.to_string()));
    };
    let after_begin = start + begin.len();
    let Some(end_rel) = line_anchored_find(text, &end, after_begin) else {
        return Err(format!(
            "unbalanced swanflow-swap block '{id}': begin marker found at byte {start} without its end marker — refusing to strip"
        ));
    };
    let end_at = end_rel + end.len();
    let nl = newline(text);
    let rest = text[end_at..].strip_prefix(nl).unwrap_or(&text[end_at..]);
    let restored = if rest.is_empty() {
        // Appended at the end, after one blank line: drop that blank line.
        text[..start]
            .strip_suffix(nl)
            .unwrap_or(&text[..start])
            .to_string()
    } else {
        // Foreign mid-file shape: keep everything else, drop the blank line
        // the inserter would have added after the block.
        let rest = rest.strip_prefix(nl).unwrap_or(rest);
        format!("{}{}", &text[..start], rest)
    };
    Ok(StripOutcome::Restored(restored))
}

fn block(text: &str, comment: &str, id: &str, body: &str) -> String {
    let (begin, end) = markers(comment, id);
    format!("{begin}\n{body}\n{end}").replace('\n', newline(text))
}

/// Append (or replace) the block for `id` at the end of the text, after one
/// blank line. Upsert semantics: an existing block for `id` is stripped
/// first; if that strip finds corruption, the error propagates instead of
/// building on top of a broken file.
///
/// Documented normalization: a text that does not end with a newline gains
/// one (the blank-line gap cannot distinguish "abc" from "abc\n" once the
/// block is in, so both round-trip to "abc\n").
pub fn append_block(text: &str, comment: &str, id: &str, body: &str) -> Result<String, String> {
    let stripped = match strip_block(text, comment, id)? {
        StripOutcome::Restored(t) | StripOutcome::NotPresent(t) => t,
    };
    let nl = newline(&stripped);
    let gap = if stripped.is_empty() {
        String::new()
    } else if stripped.ends_with('\n') {
        nl.to_string()
    } else {
        format!("{nl}{nl}")
    };
    Ok(format!(
        "{stripped}{gap}{}{nl}",
        block(&stripped, comment, id, body)
    ))
}

// ------------------------------------------------------------------ source hash

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
/// Domain separator: hashing scheme changes ⇒ bump this string, old hashes
/// stop comparing equal instead of silently aliasing.
const HASH_DOMAIN: &[u8] = b"swanflow-verified-swap-v1";

/// FNV-1a over the domain, then over each file's relative path + `\0` +
/// content + `\0`. Input order is irrelevant (files are sorted by path), so
/// the hash depends only on the (path, content) set.
pub fn source_hash(files: Vec<(String, Vec<u8>)>) -> String {
    let mut sorted = files;
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut h = FNV_OFFSET_BASIS;
    for b in HASH_DOMAIN {
        h = (h ^ u64::from(*b)).wrapping_mul(FNV_PRIME);
    }
    for (path, content) in &sorted {
        for b in path
            .as_bytes()
            .iter()
            .copied()
            .chain([0u8])
            .chain(content.iter().copied())
            .chain([0u8])
        {
            h = (h ^ u64::from(b)).wrapping_mul(FNV_PRIME);
        }
    }
    format!("{h:016x}")
}

/// [`source_hash`] over every file under `root`, recursively, keyed by the
/// `/`-separated path relative to `root`. Read errors propagate.
pub fn hash_tree(root: &Path) -> std::io::Result<String> {
    fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, Vec<u8>)>) -> std::io::Result<()> {
        let entries = std::fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, out)?;
            } else {
                let rel = path
                    .strip_prefix(root)
                    .map(|p| p.to_string_lossy().replace('\\', "/"))
                    .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"));
                out.push((rel, std::fs::read(&path)?));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(root, root, &mut files)?;
    Ok(source_hash(files))
}

// ------------------------------------------------------------------ swap gate

/// Invariant 2: the hash of the sources exactly as verification checked them.
/// [`SwapGate::check`] refuses while the current tree drifted — "verified"
/// must stay a fact, not a memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapGate {
    sealed_hash: String,
}

impl SwapGate {
    pub fn seal(source_hash: &str) -> Self {
        Self {
            sealed_hash: source_hash.to_string(),
        }
    }

    pub fn sealed_hash(&self) -> &str {
        &self.sealed_hash
    }

    /// Ok(()) while `current_hash` is still the sealed one; Err names both
    /// hashes and the remedy (re-run verification, then seal again).
    pub fn check(&self, current_hash: &str) -> Result<(), String> {
        if current_hash == self.sealed_hash {
            Ok(())
        } else {
            Err(format!(
                "sources drifted since verification: sealed {} != current {} — re-run verification and seal again before enabling",
                self.sealed_hash, current_hash
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "def levenshtein(a, b):\n    return 0\n";

    #[test]
    fn round_trip_is_byte_for_byte_lf() {
        let swapped = append_block(
            BASE,
            "#",
            "fuzzy.find_duplicates",
            "find_duplicates = rust()",
        )
        .expect("append");
        assert!(has_block(&swapped, "#", "fuzzy.find_duplicates"));
        match strip_block(&swapped, "#", "fuzzy.find_duplicates").expect("strip ok") {
            StripOutcome::Restored(restored) => assert_eq!(restored, BASE),
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_is_byte_for_byte_crlf() {
        let base = "SELECT 1;\r\nSELECT 2;\r\n";
        let swapped = append_block(base, "--", "hot.sql", "SELECT rust();").expect("append");
        assert!(swapped.contains("\r\n"));
        match strip_block(&swapped, "--", "hot.sql").expect("strip ok") {
            StripOutcome::Restored(restored) => assert_eq!(restored, base),
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_normalizes_missing_trailing_newline() {
        let swapped = append_block("abc", "#", "id", "body").expect("append");
        match strip_block(&swapped, "#", "id").expect("strip ok") {
            StripOutcome::Restored(restored) => assert_eq!(restored, "abc\n"), // documented normalization
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn empty_text_round_trip() {
        let swapped = append_block("", "#", "id", "body").expect("append");
        match strip_block(&swapped, "#", "id").expect("strip ok") {
            StripOutcome::Restored(restored) => assert!(restored.is_empty()),
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn append_upserts_existing_block() {
        let once = append_block(BASE, "#", "id", "old body").expect("append");
        let twice = append_block(&once, "#", "id", "new body").expect("append");
        assert_eq!(twice.matches(">>> swanflow-swap: id >>>").count(), 1);
        assert!(twice.contains("new body"));
        assert!(!twice.contains("old body"));
        match strip_block(&twice, "#", "id").expect("strip ok") {
            StripOutcome::Restored(restored) => assert_eq!(restored, BASE),
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn strip_missing_block_reports_not_present_not_error() {
        match strip_block(BASE, "#", "nope").expect("strip ok") {
            StripOutcome::NotPresent(text) => assert_eq!(text, BASE),
            other => panic!("expected NotPresent, got {other:?}"),
        }
        assert!(!has_block(BASE, "#", "nope"));
    }

    #[test]
    fn unbalanced_block_is_error_not_silent_success() {
        // fast-anything #69: begin marker without end marker must refuse.
        let corrupt = format!("{BASE}# >>> swanflow-swap: id >>>\norphan body\n");
        let err = strip_block(&corrupt, "#", "id").expect_err("must refuse");
        assert!(err.contains("unbalanced"), "error names the problem: {err}");
        // And append refuses to build on top of corruption.
        assert!(append_block(&corrupt, "#", "id", "body").is_err());
    }

    #[test]
    fn unrelated_block_survives_stripping_its_sibling() {
        let both = append_block(
            &append_block(BASE, "#", "a", "body a").expect("append"),
            "#",
            "b",
            "body b",
        )
        .expect("append");
        match strip_block(&both, "#", "a").expect("strip ok") {
            StripOutcome::Restored(restored) => {
                assert!(has_block(&restored, "#", "b"));
                assert!(restored.contains("body b"));
                assert!(!restored.contains("body a"));
            }
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn marker_like_substring_mid_line_is_ignored() {
        // A body line that merely mentions the marker (not at line start)
        // must not confuse line-anchored stripping.
        let body = "x = '# >>> swanflow-swap: decoy >>> mentioned in a string'";
        let swapped = append_block(BASE, "#", "real", body).expect("append");
        assert!(!has_block(&swapped, "#", "decoy"));
        match strip_block(&swapped, "#", "real").expect("strip ok") {
            StripOutcome::Restored(restored) => assert_eq!(restored, BASE),
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    #[test]
    fn source_hash_is_independent_of_input_order() {
        let files = vec![
            ("src/lib.rs".to_string(), b"fn f() {}".to_vec()),
            ("Cargo.toml".to_string(), b"[package]".to_vec()),
        ];
        let a = source_hash(files.clone());
        let b = source_hash(files.into_iter().rev().collect());
        assert_eq!(a, b);
    }

    #[test]
    fn source_hash_tracks_content_and_path() {
        let base = source_hash(vec![("a".to_string(), b"1".to_vec())]);
        assert_ne!(
            base,
            source_hash(vec![("a".to_string(), b"2".to_vec())]),
            "content change must drift"
        );
        assert_ne!(
            base,
            source_hash(vec![("b".to_string(), b"1".to_vec())]),
            "path change must drift"
        );
        assert_eq!(
            base,
            source_hash(vec![("a".to_string(), b"1".to_vec())]),
            "same set must be stable"
        );
    }

    #[test]
    fn hash_tree_walks_and_tracks_edits() {
        let dir = std::env::temp_dir().join(format!("swanflow-vswap-tree-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).expect("mkdir");
        std::fs::write(dir.join("Cargo.toml"), "[package]\n").expect("write");
        std::fs::write(dir.join("src/lib.rs"), "fn f() {}\n").expect("write");
        let before = hash_tree(&dir).expect("hash");
        std::fs::write(dir.join("src/lib.rs"), "fn g() {}\n").expect("edit");
        let after = hash_tree(&dir).expect("hash");
        assert_ne!(before, after, "editing one file must drift the tree hash");
        // Restoring the content restores the hash (mtime-independent).
        std::fs::write(dir.join("src/lib.rs"), "fn f() {}\n").expect("restore");
        assert_eq!(before, hash_tree(&dir).expect("hash"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gate_refuses_drift_and_allows_match() {
        let gate = SwapGate::seal("deadbeefdeadbeef");
        gate.check("deadbeefdeadbeef")
            .expect("sealed hash must pass");
        let err = gate
            .check("000000000000000")
            .expect_err("drift must refuse");
        assert!(
            err.contains("deadbeefdeadbeef") && err.contains("000000000000000"),
            "error names both hashes: {err}"
        );
        assert_eq!(gate.sealed_hash(), "deadbeefdeadbeef");
    }
}
