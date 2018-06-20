// Copyright (c) 2018 Nuclear Furnace
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
use config::{Config, ConfigError, Environment, File};
use std::{collections::HashMap, env, net::SocketAddr};

#[derive(Deserialize, Debug)]
pub struct Configuration {
    pub logging: LoggingConfiguration,
    pub listeners: Vec<ListenerConfiguration>,
}

#[derive(Deserialize, Default, Debug)]
pub struct LoggingConfiguration {
    pub level: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListenerConfiguration {
    pub protocol: String,
    pub address: String,
    pub pools: HashMap<String, PoolConfiguration>,
    pub routing: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PoolConfiguration {
    pub addresses: Vec<SocketAddr>,
    pub options: Option<HashMap<String, String>>,
}

impl Configuration {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name("config/synchrotron"))?;

        let env = env::var("ENV").unwrap_or("development".into());
        s.merge(File::with_name(&format!("config/synchrotron.{}", env)).required(false))?;
        s.merge(File::with_name("config/synchrotron.local").required(false))?;

        s.try_into()
    }
}
