//! build.rs
//! Build script to handle database migrations for apalis-sqlite.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
