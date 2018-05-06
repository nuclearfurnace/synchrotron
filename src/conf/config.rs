use std::env;
use std::collections::HashMap;
use std::net::SocketAddr;
use config::{ConfigError, Config, File, Environment};

#[derive(Deserialize, Debug)]
pub struct Configuration {
    pub logging: LoggingConfiguration,
    pub pools: Vec<PoolConfiguration>,
}

#[derive(Deserialize, Default, Debug)]
pub struct LoggingConfiguration {
    pub level: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PoolConfiguration {
    pub protocol: String,
    pub address: String,
    pub options: HashMap<String, String>,
    pub backends: Vec<SocketAddr>,
    pub routing: HashMap<String, String>,
}

impl Configuration {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name("config/synchrotron"))?;

        let env = env::var("ENV").unwrap_or("development".into());
        s.merge(File::with_name(&format!("config/synchrotron.{}", env)).required(false))?;
        s.merge(File::with_name("config/synchrotron.local").required(false))?;
        s.merge(Environment::with_prefix("sync"))?;

        s.try_into()
    }
}
