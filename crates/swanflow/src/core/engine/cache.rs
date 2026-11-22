//! Cache management — eviction, size tracking, cleanup.
//!
//! SwanFlow materializes intermediate results to the cache directory.
//! Without cleanup, the cache grows unbounded and can fill the disk.

use std::path::Path;

/// Manages the SwanFlow cache directory — eviction and size tracking.
pub struct CacheManager {
    /// Absolute path to the cache directory.
    pub cache_dir: String,
    /// Maximum cache size in megabytes (0 = unlimited).
    pub max_size_mb: u64,
}

impl CacheManager {
    /// Create a new cache manager.
    pub fn new(cache_dir: &str, max_size_mb: u64) -> Self {
        let _ = std::fs::create_dir_all(cache_dir);
        Self {
            cache_dir: cache_dir.to_string(),
            max_size_mb,
        }
    }

    /// Get the current total size of the cache directory in bytes.
    pub fn total_size(&self) -> u64 {
        dir_size(Path::new(&self.cache_dir))
    }

    /// Get the current cache size in megabytes.
    pub fn total_size_mb(&self) -> f64 {
        self.total_size() as f64 / (1024.0 * 1024.0)
    }

    /// Evict oldest files until total cache size is below max_size_mb.
    /// Returns the number of files removed.
    pub fn evict(&self) -> u64 {
        if self.max_size_mb == 0 {
            return 0;
        }
        let max_bytes = self.max_size_mb * 1024 * 1024;
        let mut removed = 0u64;

        // Collect all files, sorted by modification time (oldest first)
        let mut entries: Vec<_> = std::fs::read_dir(&self.cache_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter(|e| {
                // Only evict cache files, not the metadata duckdb
                let name = e.file_name().to_string_lossy().to_string();
                !name.ends_with(".duckdb") && !name.ends_with(".db")
            })
            .collect();

        entries.sort_by_key(|e| {
            std::fs::metadata(e.path())
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        let mut current_size = self.total_size();
        for entry in &entries {
            if current_size <= max_bytes {
                break;
            }
            if let Ok(meta) = std::fs::metadata(entry.path()) {
                let file_size = meta.len();
                if std::fs::remove_file(entry.path()).is_ok() {
                    current_size = current_size.saturating_sub(file_size);
                    removed += 1;
                }
            }
        }
        removed
    }

    /// Clear the entire cache (all files except metadata DuckDB).
    pub fn clear(&self) -> u64 {
        let mut removed = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.cache_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !name.ends_with(".duckdb") && !name.ends_with(".db")
                        && std::fs::remove_file(&path).is_ok() {
                            removed += 1;
                        }
                }
            }
        }
        removed
    }
}

/// Calculate the total size of a directory and all its contents.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += dir_size(&path);
            } else if let Ok(meta) = std::fs::metadata(&path) {
                total += meta.len();
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn setup_temp_cache() -> (tempfile::TempDir, CacheManager) {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let cm = CacheManager::new(tmp.path().to_str().unwrap(), 1); // 1 MB max
        (tmp, cm)
    }

    #[test]
    fn test_cache_size_tracking_tracks_bytes() {
        let (_tmp, cm) = setup_temp_cache();

        // Create a 500KB file
        let mut f = std::fs::File::create(format!("{}/test_data.parquet", cm.cache_dir))
            .expect("create test file");
        let data = vec![0u8; 500 * 1024];
        f.write_all(&data).expect("write test data");
        drop(f);

        let size_bytes = cm.total_size();
        assert!(
            size_bytes >= 500 * 1024,
            "size should be at least 500KB, got {}",
            size_bytes
        );

        let size_mb = cm.total_size_mb();
        assert!(
            (size_mb - 0.5).abs() < 0.1,
            "size should be ~0.5 MB, got {}",
            size_mb
        );
    }

    #[test]
    fn test_cache_eviction_respects_size_limit() {
        let (_tmp, cm) = setup_temp_cache();

        // Create files that exceed the 1MB limit
        std::fs::write(format!("{}/file_a.parquet", cm.cache_dir), &vec![0u8; 700 * 1024])
            .expect("write A 700KB");
        std::fs::write(format!("{}/file_b.parquet", cm.cache_dir), &vec![0u8; 500 * 1024])
            .expect("write B 500KB");

        // Total: ~1.2 MB. Max: 1 MB. Eviction should remove at least 1 file.
        let removed = cm.evict();
        assert!(removed >= 1, "should remove at least 1 file, got {}", removed);

        let remaining_size = cm.total_size();
        assert!(
            remaining_size <= 1 * 1024 * 1024,
            "after eviction size should be ≤1MB, got {}",
            remaining_size
        );
    }

    #[test]
    fn test_cache_clear_removes_all_non_db_files() {
        let (_tmp, cm) = setup_temp_cache();

        // Create some cache files
        std::fs::write(format!("{}/data1.parquet", cm.cache_dir), &[0u8; 100]).ok();
        std::fs::write(format!("{}/data2.parquet", cm.cache_dir), &[0u8; 200]).ok();
        std::fs::write(format!("{}/data3.csv", cm.cache_dir), &[0u8; 50]).ok();
        // Metadata DB should be preserved
        std::fs::write(format!("{}/metadata.duckdb", cm.cache_dir), b"db content").ok();

        let removed = cm.clear();
        assert_eq!(removed, 3, "should clear 3 non-DB files, got {}", removed);

        // metadata.duckdb should still exist
        assert!(
            std::fs::metadata(format!("{}/metadata.duckdb", cm.cache_dir)).is_ok(),
            "metadata.duckdb should survive clear"
        );
    }
}
