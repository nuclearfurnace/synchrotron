use slog::Level;

pub trait LevelExt {
    fn from_str(&str) -> Level;
}

impl LevelExt for Level {
    fn from_str(raw: &str) -> Level {
        match raw.to_string().to_lowercase().as_str() {
            "trace" => Level::Trace,
            "debug" => Level::Debug,
            "info" => Level::Info,
            "warn" => Level::Warning,
            "error" => Level::Error,
            "crit" => Level::Critical,
            "critical" => Level::Critical,
            _ => Level::Debug,
        }
    }
}
