use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum DetectedLanguage {
    Python,
    Typescript,
    Go,
    Java,
    Ruby,
    Csharp,
}

impl DetectedLanguage {
    pub fn slug(self) -> &'static str {
        match self {
            DetectedLanguage::Python => "python",
            DetectedLanguage::Typescript => "typescript",
            DetectedLanguage::Go => "go",
            DetectedLanguage::Java => "java",
            DetectedLanguage::Ruby => "ruby",
            DetectedLanguage::Csharp => "csharp",
        }
    }

    pub fn display(self) -> &'static str {
        match self {
            DetectedLanguage::Python => "Python",
            DetectedLanguage::Typescript => "TypeScript",
            DetectedLanguage::Go => "Go",
            DetectedLanguage::Java => "Java",
            DetectedLanguage::Ruby => "Ruby",
            DetectedLanguage::Csharp => "C#",
        }
    }

    pub fn all() -> &'static [DetectedLanguage] {
        &[
            DetectedLanguage::Python,
            DetectedLanguage::Typescript,
            DetectedLanguage::Go,
            DetectedLanguage::Java,
            DetectedLanguage::Ruby,
            DetectedLanguage::Csharp,
        ]
    }
}

const FILENAME_INDICATORS: &[(&str, DetectedLanguage)] = &[
    ("pyproject.toml", DetectedLanguage::Python),
    ("setup.py", DetectedLanguage::Python),
    ("requirements.txt", DetectedLanguage::Python),
    ("package.json", DetectedLanguage::Typescript),
    ("tsconfig.json", DetectedLanguage::Typescript),
    ("go.mod", DetectedLanguage::Go),
    ("pom.xml", DetectedLanguage::Java),
    ("build.gradle", DetectedLanguage::Java),
    ("build.gradle.kts", DetectedLanguage::Java),
    ("Gemfile", DetectedLanguage::Ruby),
];

const EXTENSION_INDICATORS: &[(&str, DetectedLanguage)] = &[
    (".csproj", DetectedLanguage::Csharp),
    (".sln", DetectedLanguage::Csharp),
    (".gemspec", DetectedLanguage::Ruby),
];

pub fn detect_languages(dir: &Path) -> Vec<DetectedLanguage> {
    let mut found: BTreeSet<DetectedLanguage> = BTreeSet::new();
    scan(dir, &mut found);
    if found.is_empty() {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    scan(&entry.path(), &mut found);
                }
            }
        }
    }
    found.into_iter().collect()
}

fn scan(dir: &Path, found: &mut BTreeSet<DetectedLanguage>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let lower = name.to_ascii_lowercase();
        for (indicator, lang) in FILENAME_INDICATORS {
            if lower == indicator.to_ascii_lowercase() {
                found.insert(*lang);
            }
        }
        for (ext, lang) in EXTENSION_INDICATORS {
            if lower.ends_with(&ext.to_ascii_lowercase()) {
                found.insert(*lang);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn detects_python_via_pyproject() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("pyproject.toml"), "").unwrap();
        let langs = detect_languages(dir.path());
        assert_eq!(langs, vec![DetectedLanguage::Python]);
    }

    #[test]
    fn detects_typescript_and_go_together() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("package.json"), "{}").unwrap();
        fs::write(dir.path().join("go.mod"), "module x").unwrap();
        let langs = detect_languages(dir.path());
        assert!(langs.contains(&DetectedLanguage::Typescript));
        assert!(langs.contains(&DetectedLanguage::Go));
    }

    #[test]
    fn recurses_one_level_when_top_level_empty() {
        let dir = tempdir().unwrap();
        let sub = dir.path().join("svc");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("Gemfile"), "").unwrap();
        let langs = detect_languages(dir.path());
        assert_eq!(langs, vec![DetectedLanguage::Ruby]);
    }

    #[test]
    fn detects_csharp_via_extension() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("MyApp.csproj"), "").unwrap();
        let langs = detect_languages(dir.path());
        assert_eq!(langs, vec![DetectedLanguage::Csharp]);
    }
}
