use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");

    let protos: Vec<PathBuf> = std::fs::read_dir(&proto_dir)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "proto").then_some(path)
        })
        .collect();

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Generate `bytes::Bytes` for bytes fields so payload chunks can be
        // passed through the engine without copying.
        .bytes(["."])
        // The system protoc may predate first-class proto3 optional support.
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&protos, &[proto_dir])?;

    Ok(())
}
