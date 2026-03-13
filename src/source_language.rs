#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceLanguage {
    JsLike,
    Python,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsExtensionProfile {
    FunctionsPush,
    #[allow(dead_code)]
    Eval,
}

pub fn classify_runtime_extension(
    ext: &str,
    _js_profile: JsExtensionProfile,
) -> Option<SourceLanguage> {
    let normalized = ext.to_ascii_lowercase();
    if normalized == "py" {
        return Some(SourceLanguage::Python);
    }

    let is_js_like = matches!(normalized.as_str(), "ts" | "tsx" | "js" | "jsx");
    if is_js_like {
        Some(SourceLanguage::JsLike)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_runtime_extensions_case_insensitively() {
        assert_eq!(
            classify_runtime_extension("TS", JsExtensionProfile::FunctionsPush),
            Some(SourceLanguage::JsLike)
        );
        assert_eq!(
            classify_runtime_extension("Py", JsExtensionProfile::FunctionsPush),
            Some(SourceLanguage::Python)
        );
        assert_eq!(
            classify_runtime_extension("mjs", JsExtensionProfile::FunctionsPush),
            None
        );
        assert_eq!(
            classify_runtime_extension("cjs", JsExtensionProfile::FunctionsPush),
            None
        );
    }
}
