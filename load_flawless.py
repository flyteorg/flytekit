import os
import asyncio
from typing import List, Dict, Any, Coroutine
import s3fs

# Constants
NUMBER_OF_REQUESTS: int = 80
BUCKET_NAME: str = 's3://union-cloud-oc-staging-dogfood/load-test/'
OBJECT_KEY: str = 'your_object_key'
DATA: bytes = b'abc'

# AWS Credentials
aws_access_key_id: str = 'your_access_key_id'
aws_secret_access_key: str = 'your_secret_access_key'
AWS_REGION: str = 'your_aws_region'


s3fs = s3fs.S3FileSystem()


def make_file_if_not_exists(file_path: str) -> None:
    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            f.write("hello fjdkslaf jdsakljfladskfjsa")


# Function to make S3 requests
async def make_s3_request(object_key: str, data: bytes, idx) -> Dict[str, Any]:
    if idx % 10 == 0:
        print(f"Making request for object_key: {idx}")
    source_file = f"/Users/ytong/temp/sources/source_{idx%10000}.txt"
    make_file_if_not_exists(source_file)
    response = await s3fs._put(source_file, f"s3://union-cloud-oc-staging-dogfood/load-test/{object_key}")
    if idx % 10 == 0:
        print(f"Done with request {idx}")
    return response


async def send_requests() -> List[Coroutine]:
    tasks: List[Coroutine] = [make_s3_request(f"{OBJECT_KEY}_{i}", DATA, i) for i in range(NUMBER_OF_REQUESTS)]
    print("Sending requests")
    return await asyncio.gather(*tasks)


async def main() -> None:

    responses: List[Dict[str, Any]] = await send_requests()
    # Do something with the responses


if __name__ == "__main__":
    asyncio.run(main())





