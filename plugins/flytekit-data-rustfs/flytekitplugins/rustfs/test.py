# import csv
# import os
# from collections import defaultdict
# from typing import List

# import flytekit
# from flytekit import task, workflow
# from flytekit.configuration import Config, ImageConfig
# from flytekit.remote.remote import FlyteRemote
# from flytekit.types.file import FlyteFile

# @task
# def normalize_columns(
#     csv_url: FlyteFile,
#     column_names: List[str],
#     columns_to_normalize: List[str],
#     output_location: str,
# ) -> FlyteFile:
#     # read the data from the raw csv file
#     parsed_data = defaultdict(list)
#     with open(csv_url, newline="\n") as input_file:
#         reader = csv.DictReader(input_file, fieldnames=column_names)
#         for row in (x for i, x in enumerate(reader) if i > 0):
#             for column in columns_to_normalize:
#                 parsed_data[column].append(float(row[column].strip()))

#     # normalize the data
#     normalized_data = defaultdict(list)
#     for colname, values in parsed_data.items():
#         mean = sum(values) / len(values)
#         std = (sum([(x - mean) ** 2 for x in values]) / len(values)) ** 0.5
#         normalized_data[colname] = [(x - mean) / std for x in values]

#     # write to local path
#     out_path = os.path.join(
#         flytekit.current_context().working_directory,
#         f"normalized-{os.path.basename(csv_url.path).rsplit('.')[0]}.csv",
#     )
#     with open(out_path, mode="w") as output_file:
#         writer = csv.DictWriter(output_file, fieldnames=columns_to_normalize)
#         writer.writeheader()
#         for row in zip(*normalized_data.values()):
#             writer.writerow({k: row[i] for i, k in enumerate(columns_to_normalize)})

#     if output_location:
#         return FlyteFile(path=out_path, remote_path=output_location)
#     else:
#         return FlyteFile(path=out_path)
    
# @workflow
# def normalize_csv_file(
#     csv_url: FlyteFile,
#     column_names: List[str],
#     columns_to_normalize: List[str],
#     output_location: str = "",
# ) -> FlyteFile:
#     return normalize_columns(
#         csv_url=csv_url,
#         column_names=column_names,
#         columns_to_normalize=columns_to_normalize,
#         output_location=output_location,
#     )

# if __name__ == "__main__":
#     default_files = [
#         (
#             "https://people.sc.fsu.edu/~jburkardt/data/csv/biostats.csv",
#             ["Name", "Sex", "Age", "Heights (in)", "Weight (lbs)"],
#             ["Age"],
#         ),
#         (
#             "https://people.sc.fsu.edu/~jburkardt/data/csv/faithful.csv",
#             ["Index", "Eruption length (mins)", "Eruption wait (mins)"],
#             ["Eruption length (mins)"],
#         ),
#     ]
#     print(f"Running {__file__} main...")
#     remote = FlyteRemote(Config.auto(), "flytesnacks", "development")
#     for index, (csv_url, column_names, columns_to_normalize) in enumerate(default_files):
#         execution = remote.execute(normalize_columns, inputs={"csv_url": csv_url, "column_names": column_names, "columns_to_normalize": columns_to_normalize, "output_location": ""}, execution_name_prefix="flyte-file-test", version="v1", wait=True, image_config=ImageConfig.auto_default_image())

import asyncio
import time
from fsspec import filesystem
import fsspec
import pandas
from flytekit import task, workflow
from flytekit.configuration import Config, DataConfig, ImageConfig, PlatformConfig, S3Config
from flytekit.core.data_persistence import s3_setup_args
from flytekit.core.rustS3 import RustS3FileSystem
from flytekit.remote.remote import FlyteRemote
from flytekit.types import schema  # noqa: F401
from flytekit.types.file import FlyteFile
import matplotlib.pyplot as plt

@task
def fileTask(url: FlyteFile) -> int:
    return len(url)


def testFileSystem(name, fileSystem, times):
    lpath = "/Users/troy/Documents/flyte/flytekit/testfile1"
    rpath = "s3://troy-test-flyte3/testfile1"
    totalSpent = 0
    loop = asyncio.get_event_loop()
    print(f'{name} start')
    for i in range(times):
        start_time = time.time()
        task = loop.create_task(fileSystem._put_file(lpath=lpath, rpath=rpath))
        loop.run_until_complete(task)
        spent = time.time() - start_time
        print(f'{i}: spent {spent} seconds')
        totalSpent += spent
    print(f'{name} average time {totalSpent / times}')

def testFileSystemPut(name, fileSystem, times, size):
    lpath = f"/Users/troy/Documents/flyte/flytekit/{size}M.txt"
    rpath = f"s3://troy-test-flyte3/{size}M.txt"
    spentTime = []
    loop = asyncio.get_event_loop()
    print(f'{name} start')
    for i in range(times):
        start_time = time.time()
        task = loop.create_task(fileSystem._put_file(lpath=lpath, rpath=rpath))
        loop.run_until_complete(task)
        spent = time.time() - start_time
        print(f'{i}: spent {spent} seconds')
        spentTime.append(spent)
    average = sum(sorted(spentTime)[:-1]) / (times - 1)
    print(f'{name} average time {average}')
    return average


def testFileSystemGet(name, fileSystem, times, size):
    lpath = f"/Users/troy/Documents/flyte/flytekit/{size}M.txt"
    rpath = f"s3://troy-test-flyte3/{size}M.txt"
    spentTime = []
    loop = asyncio.get_event_loop()
    print(f'{name} start')
    for i in range(times):
        start_time = time.time()
        task = loop.create_task(fileSystem._get_file(lpath=lpath, rpath=rpath))
        loop.run_until_complete(task)
        spent = time.time() - start_time
        print(f'{i}: spent {spent} seconds')
        spentTime.append(spent)
    average = sum(sorted(spentTime)[:-1]) / (times - 1)
    print(f'{name} average time {average}')
    return average

def checkFunctionality(name, fileSystem):
    lpath = "/Users/troy/Documents/flyte/flytekit/128M.txt"
    rpath = "s3://troy-test-flyte3/128M.txt"
    newlpath = "/Users/troy/Documents/flyte/flytekit/128M-1.txt"
    loop = asyncio.get_event_loop()
    print(f'{name} check functionality start')
    task = loop.create_task(fileSystem._put_file(lpath=lpath, rpath=rpath))
    loop.run_until_complete(task)
    task = loop.create_task(fileSystem._get_file(lpath=newlpath, rpath=rpath))
    loop.run_until_complete(task)
    print(f'{name} check functionality end')


if __name__ == "__main__":
    print(f"Running {__file__} main...")
    # df = pandas.DataFrame(data={"col1": [10, 2], "col2": [5, 4]})
    # print(f"Running df_wf(a=42) {df_wf(df = df)}")
    # s3kwargs = {'cache_regions': False, 'key': 'minio', 'secret': 'miniostorage', 'client_kwargs': {'endpoint_url': 'http://localhost:30002'}}
    s3kwargs = {'cache_regions': False, 'key': 'AKIAYOPTRXWPTRJXHWSQ', 'secret': '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', 'use_ssl': False, 'client_kwargs': {'endpoint_url': 'https://s3.us-east-2.amazonaws.com'}}
    s3fs = fsspec.filesystem("s3", **s3kwargs)
    rustfs = RustS3FileSystem(**s3kwargs)
    sizes = [32, 64, 128, 256, 512]
    s3time = []
    rusttime = []
    for size in sizes:
        rusttime.append(testFileSystemPut("rustfs", rustfs, 4, size))
        s3time.append(testFileSystemPut("s3fs", s3fs, 4, size))
    plt.title("Get Object")
    plt.xlabel("Object size (MB)")
    plt.ylabel("Average time")
    plt.plot(sizes, s3time, '--bo', label='s3fs')
    plt.plot(sizes, rusttime, '--ro',  label='rustfs')
    plt.legend(loc='best')
    plt.show()
    # checkFunctionality("rustfs", rustfs)

    
    
    




    # config = Config(
    #     platform=PlatformConfig(endpoint="localhost:30080", auth_mode="Pkce", insecure=True),
    #     data_config=DataConfig(
    #         s3=S3Config(endpoint="http://troy-test-flyte.s3-website-us-west-2.amazonaws.com", access_key_id="AKIAYOPTRXWPTRJXHWSQ", secret_access_key="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")
    #     ),
    # )
    # remote = FlyteRemote(config.for_sandbox(), "flytesnacks", "development")
    # img = ImageConfig.from_images(
    #     "flytekit:dev"
    # )
    # execution = remote.execute(fileTask, inputs={"url": "./README.md"}, execution_name_prefix="flyte-file-test", version="v1", wait=True, image_config=ImageConfig.auto_default_image())