use serde::{Deserialize, Serialize};

/// SwanFlow semantic version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SwanFlowVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl SwanFlowVersion {
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self { major, minor, patch }
    }

    /// Pack version into a u64 for FFI (major<<32 | minor<<16 | patch).
    pub fn to_u64(self) -> u64 {
        ((self.major as u64) << 32) | ((self.minor as u64) << 16) | (self.patch as u64)
    }

    /// Unpack version from u64.
    pub fn from_u64(v: u64) -> Self {
        Self {
            major: ((v >> 32) & 0xFFFF) as u16,
            minor: ((v >> 16) & 0xFFFF) as u16,
            patch: (v & 0xFFFF) as u16,
        }
    }
}

impl std::fmt::Display for SwanFlowVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// A simplified semver requirement.
///
/// Supports:
/// - `"0.1.0"` — exact match
/// - `">=0.1.0"` — minimum version
/// - `"^0.1.0"` — compatible (same major, >= minor)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionReq {
    pub op: VersionOp,
    pub version: SwanFlowVersion,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VersionOp {
    Exact,
    Gte,
    Compatible,
}

impl VersionReq {
    /// Parse a version requirement string.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix(">=") {
            let v = parse_version(rest)?;
            return Some(Self { op: VersionOp::Gte, version: v });
        }
        if let Some(rest) = s.strip_prefix('^') {
            let v = parse_version(rest)?;
            return Some(Self { op: VersionOp::Compatible, version: v });
        }
        let v = parse_version(s)?;
        Some(Self { op: VersionOp::Exact, version: v })
    }

    /// Check if the given version satisfies this requirement.
    pub fn matches(&self, version: &SwanFlowVersion) -> bool {
        match self.op {
            VersionOp::Exact => *version == self.version,
            VersionOp::Gte => *version >= self.version,
            VersionOp::Compatible => {
                version.major == self.version.major
                    && version.minor >= self.version.minor
            }
        }
    }
}

fn parse_version(s: &str) -> Option<SwanFlowVersion> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    Some(SwanFlowVersion::new(
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ))
}

/// Check if the engine version satisfies the plugin's minimum version.
pub fn check_compatibility(
    engine_version: &SwanFlowVersion,
    required: &str,
) -> anyhow::Result<()> {
    let req = VersionReq::parse(required)
        .ok_or_else(|| anyhow::anyhow!("Invalid version requirement: {}", required))?;
    if req.matches(engine_version) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Plugin requires SwanFlow {}, but engine is {}",
            required,
            engine_version
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_packing() {
        let v = SwanFlowVersion::new(1, 2, 3);
        let packed = v.to_u64();
        let unpacked = SwanFlowVersion::from_u64(packed);
        assert_eq!(v, unpacked);
    }

    #[test]
    fn test_version_req_gte() {
        let req = VersionReq::parse(">=0.2.0").unwrap();
        assert!(req.matches(&SwanFlowVersion::new(0, 2, 0)));
        assert!(req.matches(&SwanFlowVersion::new(1, 0, 0)));
        assert!(!req.matches(&SwanFlowVersion::new(0, 1, 0)));
    }

    #[test]
    fn test_version_req_compatible() {
        let req = VersionReq::parse("^0.2.0").unwrap();
        assert!(req.matches(&SwanFlowVersion::new(0, 2, 1)));
        assert!(req.matches(&SwanFlowVersion::new(0, 5, 0)));
        assert!(!req.matches(&SwanFlowVersion::new(0, 1, 0)));
        assert!(!req.matches(&SwanFlowVersion::new(1, 0, 0)));
    }

    #[test]
    fn test_compatibility_check() {
        let engine = SwanFlowVersion::new(0, 3, 0);
        assert!(check_compatibility(&engine, ">=0.2.0").is_ok());
        assert!(check_compatibility(&engine, ">=0.4.0").is_err());
        assert!(check_compatibility(&engine, "invalid").is_err());
    }
}
