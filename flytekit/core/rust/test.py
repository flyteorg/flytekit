import asyncio
import time
import fsspec
import matplotlib.pyplot as plt
from flytekit.core.rust.fs import RustS3FileSystem

def testFileSystemPut(name, fileSystem, times, size):
    lpath = f"/Users/troy/Documents/flyte/flytekit/{size}M.txt"
    rpath = f"s3://troy-flyte/{size}M.txt"
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
    rpath = f"s3://troy-flyte/{size}M.txt"
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
    rpath = "s3://troy-flyte/128M.txt"
    newlpath = "/Users/troy/Documents/flyte/flytekit/128M-1.txt"
    loop = asyncio.get_event_loop()
    print(f'{name} check functionality start')
    task = loop.create_task(fileSystem._put_file(lpath=lpath, rpath=rpath))
    loop.run_until_complete(task)
    task = loop.create_task(fileSystem._get_file(lpath=newlpath, rpath=rpath))
    loop.run_until_complete(task)
    print(f'{name} check functionality end')


if __name__ == "__main__":
    # s3kwargs = {'cache_regions': False, 'key': 'minio', 'secret': 'miniostorage', 'client_kwargs': {'endpoint_url': 'http://localhost:30002'}}
    s3kwargs = {'cache_regions': False, 'key': 'AKIAYOPTRXWPZFAZ7AT6', 'secret': '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', 'use_ssl': False, 'client_kwargs': {'endpoint_url': 'https://s3.us-west-1.amazonaws.com'}}
    s3fs = fsspec.filesystem("s3", **s3kwargs)
    rustfs = RustS3FileSystem(**s3kwargs)
    checkFunctionality('rustfs', rustfs)
    # sizes = [32, 64, 128, 256, 512]
    # s3time = []
    # rusttime = []
    # for size in sizes:
    #     rusttime.append(testFileSystemGet("rustfs", rustfs, 4, size))
    #     s3time.append(testFileSystemGet("s3fs", s3fs, 4, size))
    # plt.title("Get Object (s3)")
    # plt.xlabel("Object size (MB)")
    # plt.ylabel("Average time")
    # plt.plot(sizes, s3time, '--bo', label='s3fs')
    # plt.plot(sizes, rusttime, '--ro',  label='rustfs')
    # plt.legend(loc='best')
    # plt.show()