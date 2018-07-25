use std::process::Command;
use std::str;

fn main() {
    let output = Command::new("git").args(&["describe", "--always", "--dirty"]).output();
    let git_hash = match output {
        Ok(result) => {
            let buf = result.stdout.clone();
            match String::from_utf8(buf) {
                Ok(s) => s,
                Err(_) => "¯\\_(ツ)_/¯".to_owned(),
            }
        },
        Err(_) => "¯\\_(ツ)_/¯".to_owned(),
    };
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
