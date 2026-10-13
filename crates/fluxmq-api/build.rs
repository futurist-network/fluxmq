// The console is embedded from `ui/dist`: rebuild when it changes (rust-embed only tracks the
// files that existed at the last build).
fn main() {
    println!("cargo:rerun-if-changed=../../ui/dist");
}
