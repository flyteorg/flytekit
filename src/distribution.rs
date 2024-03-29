use flate2::read::GzDecoder;
use log_derive::{logfn, logfn_inputs};
use object_store;
use object_store::{ObjectStore, parse_url};
use object_store::path::Path;
use tar::Archive;
use url::Url;

#[logfn_inputs(Info, fmt = "Downloading distribution from {} to {}")]
#[logfn(ok = "INFO", err = "ERROR")]
pub async fn download_unarchive_distribution(src: &Url, dst: &String) -> Result<(), Box<dyn std::error::Error>> {
    // Uses the object_store crate to download the distribution from the source to the destination path and untar and unzip it
    // The source is a URL to the distribution
    // The destination path is the path to the directory where the distribution will be downloaded and extracted
    // The function returns a Result with the success or error message
    let store_box = parse_url(src)?;
    let store = store_box.0;

    let src_path = Path::parse(src.path())?;
    let tar_gz_stream = store.get(&src_path).await.unwrap();

    // TODO figure out how to stream unarchive the tar.gz file
    let tar_gz_data = tar_gz_stream.bytes().await.unwrap();
    let tar_data = GzDecoder::new(tar_gz_data.as_ref());
    let mut archive = Archive::new(tar_data);
    archive.unpack(dst)?;
    Ok(())
}