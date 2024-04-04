use std::fs;
use std::path::{PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_package_dirs:Vec<&str> = ["protos/flyteidl/admin/", "protos/flyteidl/cacheservice/", "protos/flyteidl/core/", "protos/flyteidl/datacatalog/", "protos/flyteidl/event/", "protos/flyteidl/plugins/", "protos/flyteidl/service/"].to_vec();
    let out_dir = concat!("src/", env!("PB_OUT_DIR")); // Avoid using `OUT_DIR`. It's already used by tonic_build and will have side effects in the target build folder.
    for package_dir in proto_package_dirs.iter() {
        let proto_files = find_proto_files(package_dir)?;
        let proto_files_paths: Vec<&str> =proto_files.iter().map(|path| path.to_str().unwrap()).collect();
        println!("{}", format!("{:?}", proto_files_paths));

        tonic_build::configure()
        .build_server(false)
        // .compile_well_known_types(true) // Defaults to false. Enable it if you don't want tonic_build to handle Well-known types by adding the `prost-types` crate automatically.
        .out_dir(out_dir)
        .compile(
            &proto_files_paths,
            &["protos/"], // same as arg `-I`` in `protoc`, it's the root folder when impoting other *.proto files.
        )?;
    }
    Ok(())
}

fn find_proto_files(dir: &str) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut proto_files = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |ext| ext == "proto") {
                    proto_files.push(path);
                } else if path.is_dir() {
                    if let Ok(mut nested_proto_files) = find_proto_files(&path.to_str().unwrap()) {
                        proto_files.append(&mut nested_proto_files);
                    }
                }
            }
        }
    }
    Ok(proto_files)
}
