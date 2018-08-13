extern crate redis;
extern crate tempfile;

use std::str;
use std::fs::File;
use std::io::{Error, Write};
use std::process::{Command, Child, Stdio};
use tempfile::{Builder, TempDir};

fn main() {
    println!("This binary does nothing.  Run `cargo test` on this to actually run the tests.")
}

macro_rules! get_config {
    ($($arg:tt)*) => (format!(r#"
        {{
            "listeners": [
                {{
                    "protocol": "redis",
                    "address": "127.0.0.1:{}",
                    "pools": {{
                        "default": {{
                            "addresses": ["127.0.0.1:{}", "127.0.0.1:{}"]
                        }}
                    }},
                    "routing": {{
                        "type": "fixed"
                    }}
                }}
            ]
        }}
    "#, $($arg)*));

}

pub struct SynchrotronRunner {
    handle: Child,
    port: u16,
    conf_dir: Option<TempDir>,
}

impl SynchrotronRunner {
    pub fn new(listen_port: u16, redis1_port: u16, redis2_port: u16) -> Result<SynchrotronRunner, Error> {
        let full_config = get_config!(listen_port, redis1_port, redis2_port);

        // Create our configuration file from the data we got.
        let conf_dir = Builder::new()
            .prefix("synchrotron-test-")
            .tempdir()?;

        let file_path = conf_dir.path().join("synchrotron");
        let file_path_w_ext = conf_dir.path().join("synchrotron.json");
        let mut conf_file = File::create(file_path_w_ext)?;
        conf_file.write(full_config.as_bytes())?;

        // Now try and launch Synchrotron.
        let handle = Command::new("../target/debug/synchrotron")
            .env("SYNC_CONFIG", file_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        wait_until(|| check_synchrotron(listen_port));

        Ok(SynchrotronRunner {
            handle: handle,
            port: listen_port,
            conf_dir: Some(conf_dir),
        })
    }
}

impl Drop for SynchrotronRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();
        self.conf_dir.take().unwrap().close().unwrap();

        println!("Synchrotron ({}) killed!", self.port);
    }
}

pub struct RedisRunner {
    handle: Child,
    port: u16,
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
            port: port,
        })
    }
}

impl Drop for RedisRunner {
    fn drop(&mut self) {
        // If it panics, it panics. ¯\_(ツ)_/¯
        self.handle.kill().unwrap();

        println!("redis-server ({}) killed!", self.port);
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

    match str::from_utf8(&result.stdout) {
        Ok(output) => match output == "PONG\n" {
            true => {
                println!("redis-server ({}) is running!", port);
                true
            },
            false => {
                println!("redis-server ({}) not running yet.", port);
                false
            },
        },
        _ => {
            println!("redis-server ({}) not running yet.", port);
            false
        },
    }
}

fn check_synchrotron(port: u16) -> bool {
    let result = Command::new("redis-cli")
        .args(&["-h", "localhost", "-p", port.to_string().as_str(), "ping"])
        .output()
        .expect("failed to run redis-cli");

    match str::from_utf8(&result.stdout) {
        Ok(output) => match output == "PONG\n" {
            true => {
                println!("Synchrotron ({}) is running!", port);
                true
            },
            false => {
                println!("Synchrotron ({}) not running yet.", port);
                false
            },
        },
        _ => {
            println!("Synchrotron ({}) not running yet.", port);
            false
        },
    }
}

#[cfg(test)]
mod redis_tests {
    use redis;
    use redis::Commands;
    use super::{SynchrotronRunner, RedisRunner};

    fn start_daemons(offset: u16) -> ((SynchrotronRunner, RedisRunner, RedisRunner), String) {
        let synchrotron_port = 43000 + offset;
        let redis1_port = 44000 + offset;
        let redis2_port = 45000 + offset;

        let redis1 = RedisRunner::new(redis1_port).unwrap();
        let redis2 = RedisRunner::new(redis2_port).unwrap();
        let synchrotron = SynchrotronRunner::new(synchrotron_port, redis1_port, redis2_port).unwrap();

        let redis_url = format!("redis://127.0.0.1:{}", synchrotron_port);
        ((synchrotron, redis1, redis2), redis_url)
    }

    #[test]
    fn test_set_get() {
        let (_daemons, redis_url) = start_daemons(0);

        // A simple set and then get.
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("my_key", 42).unwrap();
        let value: isize = conn.get("my_key").unwrap();
        assert_eq!(value, 42);

        drop(_daemons);
    }

    #[test]
    fn test_mget() {
        let (_daemons, redis_url) = start_daemons(1);

        // A simple set and then get.
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let conn = client.get_connection().unwrap();
        let _: () = conn.set("key_one", 42).unwrap();
        let _: () = conn.set("key_two", 43).unwrap();
        let _: () = conn.set("key_three", 44).unwrap();
        let value: Vec<isize> = conn.get(&["key_one", "key_two", "key_three"]).unwrap();
        assert_eq!(value, vec![42, 43, 44]);

        drop(_daemons);
    }

    #[test]
    fn test_null_key() {
        let (_daemons, redis_url) = start_daemons(2);

        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.set("", 19).unwrap();
        let value: isize = conn.get("").unwrap();
        assert_eq!(value, 19);

        let _: () = conn.set("", "").unwrap();
        let value: String = conn.get("").unwrap();
        assert_eq!(value, "");

        let _: () = conn.set_multiple(&[("", "x"), ("d", "t")]).unwrap();
        let value: String = conn.get("").unwrap();
        assert_eq!(value, "x");

        drop(_daemons);
    }

    #[test]
    #[should_panic]
    fn test_quit_drops_conn() {
        let (_daemons, redis_url) = start_daemons(3);

        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let conn = client.get_connection().unwrap();

        let _: () = conn.set("", 19).unwrap();
        let value: isize = conn.get("").unwrap();
        assert_eq!(value, 19);

        let _ = conn.send_packed_command(b"quit").unwrap();
        let _ = conn.recv_response().unwrap();

        drop(_daemons);
    }

}
