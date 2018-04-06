use serde_json;
use std::collections::HashMap;
use std::io;
use std::fs::File;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub struct Configuration {
    pub pools: Vec<PoolConfiguration>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PoolConfiguration {
    pub protocol: String,
    pub address: String,
    pub options: HashMap<String, String>,
    pub backends: HashMap<String, Vec<String>>,
    pub routing: HashMap<String, String>,
}

impl Configuration {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Configuration, io::Error> {
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}
