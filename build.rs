// capture git short hash + build date into env vars for the `version` command
use std::process::Command;

fn run(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

fn main() {
    let mut hash = run("git", &["rev-parse", "--short", "HEAD"]);
    if hash != "unknown" {
        // mark dirty working tree
        let dirty = !run("git", &["status", "--porcelain"]).is_empty()
            && run("git", &["status", "--porcelain"]) != "unknown";
        if dirty {
            hash.push_str("-dirty");
        }
    }
    let date = run("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]);
    println!("cargo:rustc-env=AGENT_BUS_GIT={}", hash);
    println!("cargo:rustc-env=AGENT_BUS_BUILD={}", date);
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
