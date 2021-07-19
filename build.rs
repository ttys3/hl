extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/index.capnp")
        .output_path("src")
        .run()
        .expect("schema compiler command");
}
