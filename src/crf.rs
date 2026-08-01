//! Extracción y validación del CRF desde la salida de `ab-av1`.

/// Extrae un CRF de la salida de ab-av1. Soporta formatos como "crf 42" y
/// "CRF: 37". Devuelve el valor validado (0..=63) o None.
pub fn parse_crf(output: &str) -> Option<u32> {
    for line in output.lines() {
        let lower = line.to_ascii_lowercase();
        let Some(pos) = lower.find("crf") else {
            continue;
        };
        let rest = &lower[pos + 3..];
        let rest = rest.trim_start_matches(|c: char| c == ':' || c == '=' || c.is_whitespace());
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            continue;
        }
        if let Ok(v) = digits.parse::<u32>() {
            if v <= 63 {
                return Some(v);
            }
        }
    }
    None
}

/// Detecta si ab-av1 indicó específicamente que no existe un CRF adecuado.
pub fn is_no_suitable_crf(output: &str) -> bool {
    output.to_ascii_lowercase().contains("no suitable crf")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_crf() {
        assert_eq!(parse_crf("blah blah\ncrf 42\n"), Some(42));
        assert_eq!(parse_crf("CRF: 37"), Some(37));
        assert_eq!(parse_crf("using crf=20 today"), Some(20));
        assert_eq!(parse_crf("  crf   55 done"), Some(55));
        assert_eq!(parse_crf("crf 0"), Some(0));
        assert_eq!(parse_crf("crf 63"), Some(63));
    }

    #[test]
    fn rejects_invalid_crf() {
        assert_eq!(parse_crf("crf 64"), None);
        assert_eq!(parse_crf("crf 100"), None);
        assert_eq!(parse_crf("crf"), None);
        assert_eq!(parse_crf("nothing here"), None);
        assert_eq!(parse_crf(""), None);
    }

    #[test]
    fn no_suitable() {
        assert!(is_no_suitable_crf("Error: no suitable crf found"));
        assert!(is_no_suitable_crf("No Suitable CRF"));
        assert!(!is_no_suitable_crf("crf 37"));
    }
}
