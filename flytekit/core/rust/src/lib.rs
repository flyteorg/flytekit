use std::io::SeekFrom;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_http::byte_stream::{ByteStream, Length};
use futures::future::join_all;
use pyo3::prelude::*;
use aws_sdk_s3::{config::Region, Client};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt};
use std::cmp;

const CHUNK_SIZE: u64 = 1024 * 1024 * 50;

#[pyclass]
pub struct S3FileSystem {
    #[pyo3(get, set)]
    pub endpoint: String,

    s3_client: aws_sdk_s3::Client,
}

fn build_client(endpoint: &str) -> aws_sdk_s3::Client {
    let region = Region::new("us-west-1");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let conf = rt.block_on(async { aws_config::load_from_env().await });
    let s3_conf = match endpoint.is_empty() {
        true => aws_sdk_s3::config::Builder::from(&conf).region(region).build(),
        false => aws_sdk_s3::config::Builder::from(&conf)
            .endpoint_url(endpoint)
            .region(region)
            .force_path_style(true)
            .build(),
    };

    Client::from_conf(s3_conf)
}

impl S3FileSystem {
    fn get_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }
}

#[pymethods]
impl S3FileSystem {
    #[new]
    pub fn new(endpoint: String) -> S3FileSystem {
        let c = build_client(&endpoint);
        S3FileSystem {
            endpoint: endpoint,
            s3_client: c,
        }
    }

    pub fn put_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let file_size = std::fs::metadata(&lpath).unwrap().len();
            if file_size < cmp::min(5 * 2u64.pow(30), 2 * CHUNK_SIZE) {
                let body = ByteStream::from_path(lpath).await;
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(body.unwrap())
                    .send()
                    .await
                    .unwrap();
            } else {
                let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
                let mut size_of_last_chunk = file_size % CHUNK_SIZE;
                if size_of_last_chunk == 0 {
                    size_of_last_chunk = CHUNK_SIZE;
                    chunk_count -= 1;
                }

                let multipart_upload_res: CreateMultipartUploadOutput = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .unwrap();
                let upload_id = multipart_upload_res.upload_id.unwrap();

                // Create futures for each chunk upload
                let upload_futures: Vec<_> = (0..chunk_count).map(|chunk_index| {
                    let client = &client;
                    let lpath = &lpath;
                    let bucket = &bucket;
                    let key = &key;
                    let upload_id = &upload_id;

                    async move {
                        let this_chunk = if chunk_count - 1 == chunk_index {
                            size_of_last_chunk
                        } else {
                            CHUNK_SIZE
                        };
                        let stream = ByteStream::read_from()
                            .path(lpath)
                            .offset(chunk_index * CHUNK_SIZE)
                            .length(Length::Exact(this_chunk))
                            .build()
                            .await
                            .unwrap();

                        // Chunk index needs to start at 0, but part numbers start at 1.
                        let part_number = (chunk_index as i32) + 1;
                        let upload_part_res = client.upload_part()
                            .key(key)
                            .bucket(bucket)
                            .upload_id(upload_id)
                            .body(stream)
                            .part_number(part_number)
                            .send()
                            .await
                            .unwrap();

                        CompletedPart::builder()
                            .e_tag(upload_part_res.e_tag.unwrap_or_default())
                            .part_number(part_number)
                            .build()
                    }
                }).collect();

                // Join all the futures to upload the chunks concurrently.
                let upload_parts = join_all(upload_futures).await;

                let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
                    .set_parts(Some(upload_parts))
                    .build();

                let _complete_multipart_upload_res = client
                    .complete_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .multipart_upload(completed_multipart_upload)
                    .upload_id(&upload_id)
                    .send()
                    .await
                    .unwrap();
            }
        });
        Ok(())
    }

    pub fn get_file(&self, lpath: String, bucket: String, key: String) -> PyResult<()> {
        let client = self.get_client();
        let rt = tokio::runtime::Runtime::new().unwrap();
    
        rt.block_on(async {
            // 1. Determine the size of the object in S3.
            let head_object_output = client.head_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .unwrap();
            let file_size = head_object_output.content_length() as u64;
    
            // 2. Split the object's byte range into chunks.
            let mut chunk_count = file_size / CHUNK_SIZE;
            if file_size % CHUNK_SIZE > 0 {
                chunk_count += 1;
            }
    
            // 3. Download each chunk concurrently.
            let download_futures: Vec<_> = (0..chunk_count).map(|chunk_index| {
                let client = &client;
                let bucket = &bucket;
                let key = &key;
                let lpath = &lpath;
            
                async move {
                    let start_byte = chunk_index * CHUNK_SIZE;
                    let end_byte = std::cmp::min(start_byte + CHUNK_SIZE, file_size) - 1;
                    
                    let get_object_output = client.get_object()
                        .bucket(bucket)
                        .key(key)
                        .range(&format!("bytes={}-{}", start_byte, end_byte))
                        .send()
                        .await
                        .expect("Failed to get object");
            
                    let mut buffer = Vec::new();
                    get_object_output.body.into_async_read().read_to_end(&mut buffer).await.expect("Failed to read object body");
            
                    // Open the file and seek to the appropriate position
                    let mut file = tokio::fs::OpenOptions::new().write(true).create(true).open(&lpath).await.expect("Unable to open file");
                    file.seek(SeekFrom::Start((chunk_index * CHUNK_SIZE) as u64)).await.expect("Failed to seek in file");
                    file.write_all(&buffer).await.expect("Failed to write chunk to file");
                }
            }).collect();
    
            join_all(download_futures).await;
        });
        Ok(())
    }
}

#[pymodule]
fn rustfs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<S3FileSystem>()?;
    Ok(())
}