//! Validación de nombres de archivo, contenedores y cálculo de nombres de salida.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;

pub const VIDEO_EXTENSIONS: [&str; 5] = ["mp4", "mkv", "mov", "avi", "webm"];

/// Un nombre de archivo es válido si es simple: sin '/', '\\', sin caracteres
/// de control y distinto de "." y "..".
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
        bail!("nombre de archivo no válido: {name:?}");
    }
    Ok(())
}

/// Normaliza el contenedor: vacío -> "mp4". Debe ser una extensión sencilla
/// formada solo por letras o números ASCII.
pub fn normalize_container(container: &str) -> Result<String> {
    let c = container.trim();
    if c.is_empty() {
        return Ok("mp4".to_string());
    }
    if !c.chars().all(|c| c.is_ascii_alphanumeric()) {
        bail!("contenedor no válido: {container:?} (solo letras/números ASCII)");
    }
    Ok(c.to_string())
}

/// Comprueba si un nombre de archivo (insensible a mayúsculas) es un vídeo reconocido.
pub fn is_video(name: &str) -> bool {
    match name.rsplit_once('.') {
        Some((_, ext)) => VIDEO_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()),
        None => false,
    }
}

/// Nombre del archivo de salida: mismo nombre base con la extensión del contenedor.
pub fn output_name(input: &str, container: &str) -> String {
    let stem = match input.rsplit_once('.') {
        Some((stem, _)) => stem,
        None => input,
    };
    format!("{stem}.{container}")
}

/// Devuelve error si dos vídeos generarían el mismo nombre de salida.
pub fn check_output_collisions(files: &[String], container: &str) -> Result<()> {
    let mut seen: HashMap<String, &str> = HashMap::new();
    for f in files {
        let out = output_name(f, container).to_ascii_lowercase();
        if let Some(prev) = seen.insert(out.clone(), f) {
            bail!(
                "colisión de salida: {prev:?} y {f:?} generarían ambos {out:?}"
            );
        }
    }
    Ok(())
}

/// Escanea el directorio de entrada y devuelve los vídeos (validados).
pub fn scan_videos(input_dir: &std::path::Path) -> Result<Vec<String>> {
    let mut videos = Vec::new();
    let entries = std::fs::read_dir(input_dir)
        .with_context(|| format!("leyendo {}", input_dir.display()))?;
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
