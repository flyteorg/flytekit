use pyo3::prelude::*;
use object_store::{
    aws::AmazonS3Builder, ObjectStore, path::Path,
};
use tokio::io::AsyncWriteExt;
use std::{sync::Arc, fs::{self}};

#[pyclass]
pub struct FileSystem {
    store: Arc<dyn ObjectStore>,
}

fn get_s3_store() -> Arc<dyn ObjectStore> {

//     let s3 = AmazonS3Builder::new()
//         .with_endpoint("http://localhost:30002")
//         .with_allow_http(true)
//         .with_region("us-east-1") // Dummy region for local testing
//         .with_bucket_name("troy-flyte")
//         .with_access_key_id("minio")
//         .with_secret_access_key("miniostorage")
//         .build()
//         .expect("error creating s3");
    let s3 = AmazonS3Builder::from_env().with_region("us-west-2").with_bucket_name("flyte-benchmark").build().unwrap();
    Arc::new(s3)
    
}

#[pymethods]
impl FileSystem {
    #[new]
    pub fn new() -> Self {
        let store = get_s3_store();
        FileSystem { store }
    }

    fn put(&self, from_path: String, to_path: String, recursive: bool) -> PyResult<()> {
        let file_content = fs::read(from_path).unwrap();
        let path = Path::from("test/file.txt");
        println!("output {}", path);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            self.store.put(&path, file_content.into()).await.unwrap();
        });
        Ok(())
    }

    fn get(&self, from_path: String, to_path: String, recursive: bool) -> PyResult<()> {
        let path = Path::from(from_path);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let downloaded_bytes = self.store.get(&path).await.unwrap().bytes().await.unwrap();
            let mut file = tokio::fs::File::create(to_path).await.unwrap();
            file.write_all(&downloaded_bytes).await.unwrap();
        });
        Ok(())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_file_system(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FileSystem>()?;
    Ok(())
}
