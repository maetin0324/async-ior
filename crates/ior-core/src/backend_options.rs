use std::collections::BTreeMap;

/// Value of a backend-specific option.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionValue {
    /// Boolean flag with no value (e.g., `--posix.odirect`).
    Flag,
    /// String value (e.g., `--benchfs.registry=/tmp`).
    Str(String),
}

impl OptionValue {
    pub fn is_flag(&self) -> bool {
        matches!(self, OptionValue::Flag)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            OptionValue::Str(s) => Some(s),
            OptionValue::Flag => None,
        }
    }

    /// Parse as i64. Flag is treated as 1.
    pub fn as_i64(&self) -> Result<i64, crate::IorError> {
        match self {
            OptionValue::Flag => Ok(1),
            OptionValue::Str(s) => s
                .parse::<i64>()
                .map_err(|_| crate::IorError::InvalidArgument),
        }
    }

    /// Parse as bool. Flag → true, "0"/"false"/"no" → false, otherwise true.
    pub fn as_bool(&self) -> bool {
        match self {
            OptionValue::Flag => true,
            OptionValue::Str(s) => !matches!(s.as_str(), "0" | "false" | "no"),
        }
    }
}

/// Collection of backend-specific options extracted from command-line arguments.
#[derive(Debug, Clone, Default)]
pub struct BackendOptions {
    /// Stored as "prefix.key" → OptionValue.
    opts: BTreeMap<String, OptionValue>,
}

impl BackendOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: String, value: OptionValue) {
        self.opts.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&OptionValue> {
        self.opts.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.opts.is_empty()
    }

    /// Iterate over options matching a given prefix.
    ///
    /// For example, `for_prefix("posix")` yields `("odirect", &Flag)` for the
    /// option stored as `"posix.odirect"`.
    pub fn for_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a OptionValue)> {
        let prefix_dot = format!("{}.", prefix);
        self.opts.iter().filter_map(move |(k, v)| {
            k.strip_prefix(&prefix_dot).map(|suffix| (suffix, v))
        })
    }

    /// Check if any option has the given prefix.
    pub fn has_prefix(&self, prefix: &str) -> bool {
        let prefix_dot = format!("{}.", prefix);
        self.opts.keys().any(|k| k.starts_with(&prefix_dot))
    }
}

/// Check if an argument looks like a backend option (`--word.word[.word...]`).
fn is_backend_option(arg: &str) -> bool {
    let Some(body) = arg.strip_prefix("--") else {
        return false;
    };
    // Take the part before '=' if present
    let name = body.split('=').next().unwrap_or(body);
    // Must contain at least one dot, and each segment must be non-empty alphanumeric/underscore/hyphen
    let segments: Vec<&str> = name.split('.').collect();
    if segments.len() < 2 {
        return false;
    }
    segments
        .iter()
        .all(|s| !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-'))
}

/// Extract backend-specific options from raw command-line arguments.
///
/// Returns `(filtered_args, backend_options)` where `filtered_args` has all
/// backend options removed (suitable for passing to clap) and `backend_options`
/// contains the extracted key-value pairs.
///
/// Supported forms:
/// - `--prefix.key=value`  → `Str(value)`
/// - `--prefix.key value`  → `Str(value)` (if next arg doesn't start with `-`)
/// - `--prefix.key`        → `Flag` (if next arg starts with `-` or is last)
pub fn extract_backend_options(args: Vec<String>) -> (Vec<String>, BackendOptions) {
    let mut filtered = Vec::new();
    let mut opts = BackendOptions::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];

        if !is_backend_option(arg) {
            filtered.push(arg.clone());
            i += 1;
            continue;
        }

        let body = arg.strip_prefix("--").unwrap();

        if let Some((name, value)) = body.split_once('=') {
            // --prefix.key=value
            opts.insert(name.to_string(), OptionValue::Str(value.to_string()));
            i += 1;
        } else if i + 1 < args.len() && !args[i + 1].starts_with('-') {
            // --prefix.key value
            opts.insert(body.to_string(), OptionValue::Str(args[i + 1].clone()));
            i += 2;
        } else {
            // --prefix.key (flag)
            opts.insert(body.to_string(), OptionValue::Flag);
            i += 1;
        }
    }

    (filtered, opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_backend_option() {
        // Positive cases
        assert!(is_backend_option("--posix.odirect"));
        assert!(is_backend_option("--benchfs.registry=/tmp"));
        assert!(is_backend_option("--foo.bar.baz"));
        assert!(is_backend_option("--my-backend.some_opt"));

        // Negative cases
        assert!(!is_backend_option("--direct-io"));
        assert!(!is_backend_option("-w"));
        assert!(!is_backend_option("--verbose"));
        assert!(!is_backend_option("somefile.txt"));
        assert!(!is_backend_option("--"));
        assert!(!is_backend_option("--.foo"));
        assert!(!is_backend_option("--foo."));
    }

    #[test]
    fn test_extract_equals_form() {
        let args = vec![
            "prog".into(),
            "--benchfs.registry=/tmp/reg".into(),
            "-w".into(),
        ];
        let (filtered, opts) = extract_backend_options(args);
        assert_eq!(filtered, vec!["prog", "-w"]);
        assert_eq!(
            opts.get("benchfs.registry"),
            Some(&OptionValue::Str("/tmp/reg".into()))
        );
    }

    #[test]
    fn test_extract_space_form() {
        let args = vec![
            "prog".into(),
            "--benchfs.registry".into(),
            "/tmp/reg".into(),
            "-w".into(),
        ];
        let (filtered, opts) = extract_backend_options(args);
        assert_eq!(filtered, vec!["prog", "-w"]);
        assert_eq!(
            opts.get("benchfs.registry"),
            Some(&OptionValue::Str("/tmp/reg".into()))
        );
    }

    #[test]
    fn test_extract_flag_form() {
        let args = vec![
            "prog".into(),
            "--posix.odirect".into(),
            "-w".into(),
        ];
        let (filtered, opts) = extract_backend_options(args);
        assert_eq!(filtered, vec!["prog", "-w"]);
        assert_eq!(opts.get("posix.odirect"), Some(&OptionValue::Flag));
    }

    #[test]
    fn test_extract_flag_at_end() {
        let args = vec!["prog".into(), "--posix.odirect".into()];
        let (filtered, opts) = extract_backend_options(args);
        assert_eq!(filtered, vec!["prog"]);
        assert_eq!(opts.get("posix.odirect"), Some(&OptionValue::Flag));
    }

    #[test]
    fn test_for_prefix() {
        let mut opts = BackendOptions::new();
        opts.insert("posix.odirect".into(), OptionValue::Flag);
        opts.insert("posix.alignment".into(), OptionValue::Str("4096".into()));
        opts.insert("benchfs.registry".into(), OptionValue::Str("/tmp".into()));

        let posix: Vec<_> = opts.for_prefix("posix").collect();
        assert_eq!(posix.len(), 2);
        // BTreeMap is sorted, so "alignment" comes before "odirect"
        assert_eq!(posix[0], ("alignment", &OptionValue::Str("4096".into())));
        assert_eq!(posix[1], ("odirect", &OptionValue::Flag));

        let benchfs: Vec<_> = opts.for_prefix("benchfs").collect();
        assert_eq!(benchfs.len(), 1);
        assert_eq!(benchfs[0], ("registry", &OptionValue::Str("/tmp".into())));

        assert!(opts.has_prefix("posix"));
        assert!(opts.has_prefix("benchfs"));
        assert!(!opts.has_prefix("mpiio"));
    }

    #[test]
    fn test_option_value_conversions() {
        assert!(OptionValue::Flag.is_flag());
        assert!(OptionValue::Flag.as_bool());
        assert_eq!(OptionValue::Flag.as_i64().unwrap(), 1);
        assert_eq!(OptionValue::Flag.as_str(), None);

        let val = OptionValue::Str("42".into());
        assert!(!val.is_flag());
        assert!(val.as_bool());
        assert_eq!(val.as_i64().unwrap(), 42);
        assert_eq!(val.as_str(), Some("42"));

        assert!(!OptionValue::Str("0".into()).as_bool());
        assert!(!OptionValue::Str("false".into()).as_bool());
        assert!(!OptionValue::Str("no".into()).as_bool());
        assert!(OptionValue::Str("yes".into()).as_bool());
        assert!(OptionValue::Str("1".into()).as_bool());
    }

    #[test]
    fn test_mixed_args_preserved() {
        let args = vec![
            "prog".into(),
            "-w".into(),
            "-r".into(),
            "--block-size".into(),
            "1m".into(),
            "--posix.odirect".into(),
            "--transfer-size".into(),
            "256k".into(),
            "--benchfs.registry=/tmp".into(),
        ];
        let (filtered, opts) = extract_backend_options(args);
        assert_eq!(
            filtered,
            vec!["prog", "-w", "-r", "--block-size", "1m", "--transfer-size", "256k"]
        );
        assert_eq!(opts.get("posix.odirect"), Some(&OptionValue::Flag));
        assert_eq!(
            opts.get("benchfs.registry"),
            Some(&OptionValue::Str("/tmp".into()))
        );
    }
}
