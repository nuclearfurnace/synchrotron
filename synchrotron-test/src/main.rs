extern crate redis;
extern crate tempfile;

use std::str;
use std::thread;
use std::fs::File;
use std::io::{Error, Write};
use std::process::{Command, Child, Stdio};
use tempfile::{Builder, TempDir};

fn main() {
    println!("This binary does nothing.  Run `cargo test` on this to actually run the tests.")
}

pub struct SynchrotronRunner {
    handle: Child,
    conf_dir: Option<TempDir>,
}

impl SynchrotronRunner {
    pub fn new(config_data: &str) -> Result<SynchrotronRunner, Error> {
        // Create our configuration file from the data we got.
        let conf_dir = Builder::new()
            .prefix("synchrotron-test-")
            .tempdir()?;

        let file_path = conf_dir.path().join("synchrotron");
        let file_path_w_ext = conf_dir.path().join("synchrotron.json");
        let mut conf_file = File::create(file_path_w_ext)?;
        conf_file.write(config_data.as_bytes())?;

        // Now try and launch Synchrotron.
        let handle = Command::new("../target/debug/synchrotron")
            .env("SYNC_CONFIG", file_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        Ok(SynchrotronRunner {
            handle: handle,
            conf_dir: Some(conf_dir),
        })
    }
}

impl Drop for SynchrotronRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();
        self.conf_dir.take().unwrap().close().unwrap();
    }
}

pub struct RedisRunner {
    handle: Child,
}

impl RedisRunner {
    pub fn new(port: u16) -> Result<RedisRunner, Error> {
        // Launch Redis on the specified port.
        let handle = Command::new("/usr/local/bin/redis-server")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // Wait for the instance to be ready.
        wait_until(|| check_redis(port));

        Ok(RedisRunner {
            handle: handle,
        })
    }
}

impl Drop for RedisRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();
    }
}

fn wait_until<F>(f: F)
    where F: Fn() -> bool
{
    let mut sleep_ms = 50;

    loop {
        let status = f();
        if status {
            return;
        }

        thread::sleep_ms(sleep_ms);

        if sleep_ms < 5000 {
            sleep_ms *= 2;
        }
    }
}

fn check_redis(port: u16) -> bool {
    let result = Command::new("redis-cli")
        .args(&["-h", "localhost", "-p", port.to_string().as_str(), "ping"])
        .output()
        .expect("failed to run redis-cli");

println!("res for {}: {:?}", port, result);

    match str::from_utf8(&result.stdout) {
        Ok(output) => output == "PONG\n",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use redis;
    use redis::Commands;
    use super::{SynchrotronRunner, RedisRunner};

    const DEFAULT_CONFIG: &str = r#"
        {
            "listeners": [
                {
                    "protocol": "redis",
                    "address": "127.0.0.1:56379",
                    "pools": {
                        "default": {
                            "addresses": ["127.0.0.1:46379", "127.0.0.1:46380"]
                        }
                    },
                    "routing": "normal"
                }
            ]
        }
    "#;

    fn start_daemons() -> (SynchrotronRunner, RedisRunner, RedisRunner) {
        let synchrotron = SynchrotronRunner::new(DEFAULT_CONFIG).unwrap();
        let redis1 = RedisRunner::new(46379).unwrap();
        let redis2 = RedisRunner::new(46380).unwrap();

        // We sleep here because Synchrotron doesn't start up fast enough otherwise to be available
        // before tests try to open their connections.
        //
        // Twemproxy's tests handle this by checking for aliveness using a sort of exponential
        // backoff.  For Redis, they wait for PING -> PONG, and for Twemproxy itself, they wait
        // until it starts to be able to return metrics on the stats port.  We don't have that
        // (yet?) but we could do the check for Redis.
        thread::sleep(Duration::from_millis(500));

        (synchrotron, redis1, redis2)
    }

    #[test]
    fn test_set_get() {
        let _daemons = start_daemons();

        // A simple set and then get.
        let client = redis::Client::open("redis://127.0.0.1:56379/").unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("my_key", 42).unwrap();
        let value: isize = conn.get("my_key").unwrap();
        assert_eq!(value, 42);

        drop(_daemons);
    }

    #[test]
    fn test_mget() {
        let _daemons = start_daemons();

        // A simple set and then get.
        let client = redis::Client::open("redis://127.0.0.1:56379/").unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("key_one", 42).unwrap();
        let _: () = conn.set("key_two", 43).unwrap();
        let _: () = conn.set("key_three", 44).unwrap();
        let value: Vec<isize> = conn.get(&["key_one", "key_two", "key_three"]).unwrap();
        assert_eq!(value, vec![42, 43, 44]);

        drop(_daemons);
    }
}
