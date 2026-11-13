//! The raw, string-level configuration — the parsed INI — from which the typed `Config`
//! is built. `${VAR}` references substitute from the process environment.

use anyhow::{Context, Result, anyhow};
use configparser::ini::Ini;

/// Looks up `[section] key` values in the INI and substitutes their `${VAR}` references
/// from the environment.
pub(super) struct RawConfig {
    ini: Ini,
}

impl RawConfig {
    pub(super) fn new(ini: Ini) -> Self {
        Self { ini }
    }

    /// A resolved value that must be present in the file. Trimmed: edge whitespace can
    /// only come from a substituted env var, where it's an accident (e.g. a trailing
    /// newline) — never meaning.
    pub(super) fn req(&self, section: &str, key: &str) -> Result<String> {
        let raw = self
            .ini
            .get(section, key)
            .with_context(|| format!("missing [{section}] {key}"))?;
        let value = resolve(&raw).with_context(|| format!("resolving [{section}] {key}"))?;
        Ok(value.trim().to_string())
    }

    /// A resolved value (trimmed, like [`req`](Self::req)), or empty string if the key
    /// is absent.
    pub(super) fn opt(&self, section: &str, key: &str) -> Result<String> {
        match self.ini.get(section, key) {
            Some(raw) => {
                let value =
                    resolve(&raw).with_context(|| format!("resolving [{section}] {key}"))?;
                Ok(value.trim().to_string())
            }
            None => Ok(String::new()),
        }
    }
}

/// Substitute `${VAR}` / `${VAR:-default}` references in `raw`. Lookup order: process
/// environment, then the `:-` default; missing from both is an error.
fn resolve(raw: &str) -> Result<String> {
    let mut out = String::new();
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .with_context(|| format!("unterminated ${{ in {raw:?}"))?;
        let (name, default) = match after[..end].split_once(":-") {
            Some((n, d)) => (n, Some(d)),
            None => (&after[..end], None),
        };
        let value = match std::env::var(name) {
            Ok(v) => v,
            Err(_) => default.map(str::to_string).ok_or_else(|| {
                anyhow!("{name} is not set in the environment and has no default (in {raw:?})")
            })?,
        };
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_passes_through_literals() {
        assert_eq!(resolve("plain value").unwrap(), "plain value");
    }

    #[test]
    fn resolve_uses_default_when_var_unset() {
        // Given a `${VAR:-default}` reference whose variable is guaranteed unset (the
        // NORDPOOL_CFG_UNSET name exists only to never be in the environment).
        // When / Then resolving falls through to the `:-` default rather than erroring, which is
        // how the shipped INI keeps optional features off when their env var isn't injected.
        assert_eq!(
            resolve("${NORDPOOL_CFG_UNSET:-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn resolve_composes_multiple_refs() {
        assert_eq!(
            resolve("${A_CFG_UNSET:-http}://${B_CFG_UNSET:-host}/x").unwrap(),
            "http://host/x"
        );
    }

    #[test]
    fn resolve_errors_on_missing_var_without_default() {
        let err = resolve("${NORDPOOL_CFG_UNSET}").unwrap_err();
        assert!(err.to_string().contains("NORDPOOL_CFG_UNSET"));
    }

    #[test]
    fn resolve_reads_from_the_environment() {
        // PATH is always set in the test environment.
        assert_eq!(resolve("${PATH}").unwrap(), std::env::var("PATH").unwrap());
    }
}
