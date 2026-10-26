//! Paths that must never be synced (VCS, virtualenvs, editor junk).

const IGNORE_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
    ".ruff_cache",
];

const IGNORE_NAMES: &[&str] = &[".DS_Store", "Thumbs.db", ".directory"];

/// True if a relative sync path should be skipped.
/// Matches `.git/` at any depth (`LinuxOS/KvNix/.git/objects/…`), `venv/`, etc.
pub fn is_ignored(rel: &str) -> bool {
    let rel = rel.replace('\\', "/");
    let rel = rel.trim_matches('/');
    if rel.is_empty() || rel.ends_with('~') {
        return true;
    }
    for part in rel.split('/') {
        if IGNORE_DIRS.iter().any(|d| *d == part) {
            return true;
        }
        if IGNORE_NAMES.iter().any(|n| *n == part) {
            return true;
        }
        if part.ends_with(".pyc") || part.ends_with(".pyo") {
            return true;
        }
    }
    false
}

/// True if this directory name should not be descended into while walking.
pub fn is_ignored_dir_name(name: &str) -> bool {
    IGNORE_DIRS.iter().any(|d| *d == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_nested_git() {
        assert!(is_ignored("LinuxOS/KvNix/configs/.git/objects/ab/cd"));
        assert!(is_ignored(".git/HEAD"));
        assert!(is_ignored(".git"));
    }

    #[test]
    fn skips_venv_and_pycache() {
        assert!(is_ignored("EnvisionTech.io/STL-Tools/venv/bin/python"));
        assert!(is_ignored("proj/.venv/lib/site.py"));
        assert!(is_ignored("pkg/__pycache__/mod.cpython-313.pyc"));
    }

    #[test]
    fn keeps_real_files() {
        assert!(!is_ignored("TODAY"));
        assert!(!is_ignored("Android/TODO-IN-PROGRESS"));
        assert!(!is_ignored("LinuxOS/KvNix/configs/bashrc"));
    }
}
