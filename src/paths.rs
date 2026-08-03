//! Filename, container validation and output name computation.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;

pub const VIDEO_EXTENSIONS: [&str; 5] = ["mp4", "mkv", "mov", "avi", "webm"];

/// A filename is valid if it is simple: no '/', '\\', no control
/// characters and different from "." and "..".
pub fn is_valid_filename(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    !name
        .chars()
        .any(|c| c == '/' || c == '\\' || c.is_control())
}

pub fn validate_filename(name: &str) -> Result<()> {
    if !is_valid_filename(name) {
        bail!("invalid filename: {name:?}");
    }
    Ok(())
}

/// Normalizes the container: empty -> "mp4". It must be a simple extension
/// made only of ASCII letters or numbers.
pub fn normalize_container(container: &str) -> Result<String> {
    let c = container.trim();
    if c.is_empty() {
        return Ok("mp4".to_string());
    }
    if !c.chars().all(|c| c.is_ascii_alphanumeric()) {
        bail!("invalid container: {container:?} (ASCII letters/numbers only)");
    }
    Ok(c.to_string())
}

/// Checks whether a filename (case-insensitive) is a recognized video.
pub fn is_video(name: &str) -> bool {
    match name.rsplit_once('.') {
        Some((_, ext)) => VIDEO_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()),
        None => false,
    }
}

/// Output filename: same base name with the container extension.
pub fn output_name(input: &str, container: &str) -> String {
    let filename = match input.rsplit_once('.') {
        Some((fname, _)) => fname,
        None => input,
    };
    format!("{filename}.{container}")
}

/// Returns an error if two videos would generate the same output name.
pub fn check_output_collisions(files: &[String], container: &str) -> Result<()> {
    let mut seen: HashMap<String, &str> = HashMap::new();

    for f in files {
        let out = output_name(f, container).to_ascii_lowercase();

        // If I can get some prev (if hashmap has repeated key it returns the previous value before the remplace for the new value)
        if let Some(prev) = seen.insert(out.clone(), f) {
            bail!(
                "output collision: {prev:?} and {f:?} would both generate {out:?}"
            );
        }
    }
    Ok(())
}

/// Scans the input directory and returns the (validated) videos.
pub fn scan_videos(input_dir: &std::path::Path) -> Result<Vec<String>> {
    let mut videos = Vec::new();
    let entries = std::fs::read_dir(input_dir)
        .with_context(|| format!("reading {}", input_dir.display()))?;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_video(&name) {
            validate_filename(&name)?;
            videos.push(name);
        }
    }
    videos.sort();
    Ok(videos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_filenames() {
        assert!(is_valid_filename("video.mkv"));
        assert!(is_valid_filename("video clip+#.webm"));
        assert!(is_valid_filename("Mi vídeo.mp4"));
        assert!(!is_valid_filename("../archivo.mp4"));
        assert!(!is_valid_filename("carpeta/archivo.mp4"));
        assert!(!is_valid_filename("carpeta\\archivo.mp4"));
        assert!(!is_valid_filename("."));
        assert!(!is_valid_filename(".."));
        assert!(!is_valid_filename(""));
        assert!(!is_valid_filename("a\0b.mkv"));
        assert!(!is_valid_filename("a\nb.mkv"));
    }

    #[test]
    fn containers() {
        assert_eq!(normalize_container("").unwrap(), "mp4");
        assert_eq!(normalize_container("  ").unwrap(), "mp4");
        assert_eq!(normalize_container("mkv").unwrap(), "mkv");
        assert_eq!(normalize_container("webm").unwrap(), "webm");
        assert!(normalize_container("m.kv").is_err());
        assert!(normalize_container("mk v").is_err());
        assert!(normalize_container("../x").is_err());
        assert!(normalize_container("mk-v").is_err());
    }

    #[test]
    fn video_detection() {
        assert!(is_video("a.mp4"));
        assert!(is_video("a.MKV"));
        assert!(is_video("a.WebM"));
        assert!(!is_video("a.txt"));
        assert!(!is_video("noext"));
    }

    #[test]
    fn output_names() {
        assert_eq!(output_name("pelicula.mkv", "mp4"), "pelicula.mp4");
        assert_eq!(output_name("pelicula.webm", "mp4"), "pelicula.mp4");
        assert_eq!(output_name("sin.ext.rara.mkv", "webm"), "sin.ext.rara.webm");
    }

    #[test]
    fn collisions() {
        assert!(check_output_collisions(
            &["pelicula.mkv".into(), "pelicula.webm".into()],
            "mp4"
        )
        .is_err());
        assert!(check_output_collisions(
            &["a.mkv".into(), "b.webm".into()],
            "mp4"
        )
        .is_ok());
    }
}
