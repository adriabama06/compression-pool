//! TOML deserialization, validation and argument splitting.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Settings {
    pub workers: Vec<String>,
    pub folders: Folders,
    pub crf_search: CrfSearch,
    pub encoder: Encoder,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Folders {
    pub input_folder: PathBuf,
    pub output_folder: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CrfSearch {
    pub ab_av1_arguments: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Encoder {
    pub ffmpeg_arguments: String,
    #[serde(default)]
    pub ffmpeg_container: String,
}

/// Validated configuration ready to use.
pub struct Config {
    pub workers: Vec<String>,
    pub input_folder: PathBuf,
    pub output_folder: PathBuf,
    pub ab_av1_args: Vec<String>,
    pub ffmpeg_args: Vec<String>,
    pub container: String,
}

/// Splits an argument string using shell syntax (without invoking a shell).
pub fn split_args(s: &str) -> Result<Vec<String>> {
    shell_words::split(s).context("arguments with unclosed quotes")
}

/// Checks whether the ffmpeg arguments already fix quality/rate (-crf or -b:v),
/// whether separate ("-crf", "24") or combined ("-crf=24", "-b:v=1M").
pub fn args_fixed_quality(args: &[String]) -> bool {
    args.iter().any(|a| {
        a == "-crf" || a.starts_with("-crf") || a == "-b:v" || a.starts_with("-b:v")
    })
}

impl Settings {
    pub fn load(path: &Path) -> Result<Settings> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let settings: Settings = toml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?;
        Ok(settings)
    }

    pub fn validate(self) -> Result<Config> {
        if self.workers.is_empty() {
            bail!("at least one worker must be configured");
        }
        for w in &self.workers {
            url::Url::parse(w).with_context(|| format!("invalid worker URL: {w}"))?;
        }

        let input = &self.folders.input_folder;
        if !input.is_dir() {
            bail!(
                "the input folder does not exist or is not a directory: {}",
                input.display()
            );
        }
        std::fs::create_dir_all(&self.folders.output_folder).with_context(|| {
            format!("creating {}", self.folders.output_folder.display())
        })?;

        let container = crate::paths::normalize_container(&self.encoder.ffmpeg_container)?;
        let ab_av1_args = split_args(&self.crf_search.ab_av1_arguments)?;
        let ffmpeg_args = split_args(&self.encoder.ffmpeg_arguments)?;

        Ok(Config {
            workers: self.workers,
            input_folder: input.clone(),
            output_folder: self.folders.output_folder,
            ab_av1_args,
            ffmpeg_args,
            container,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_quoted_args() {
        let v = split_args("-metadata \"title=Mi vídeo\" -c:v libsvtav1").unwrap();
        assert_eq!(v, vec!["-metadata", "title=Mi vídeo", "-c:v", "libsvtav1"]);
        let v = split_args("--min-vmaf 95 --preset 4").unwrap();
        assert_eq!(v, vec!["--min-vmaf", "95", "--preset", "4"]);
        assert_eq!(split_args("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn unclosed_quotes_error() {
        assert!(split_args("-metadata \"title=sin cerrar").is_err());
    }

    #[test]
    fn detects_quality_args() {
        assert!(args_fixed_quality(&["-crf".into(), "24".into()]));
        assert!(args_fixed_quality(&["-crf=24".into()]));
        assert!(args_fixed_quality(&["-preset".into(), "6".into(), "-b:v".into(), "1M".into()]));
        assert!(args_fixed_quality(&["-b:v=2M".into()]));
        assert!(!args_fixed_quality(&["-preset".into(), "6".into(), "-c:v".into(), "libsvtav1".into()]));
        assert!(!args_fixed_quality(&[]));
    }
}
