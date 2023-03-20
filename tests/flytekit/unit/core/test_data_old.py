# def test_mfldl_detect_slash(source_folder):
#     print("======")
#     dc = Config.for_sandbox().data_config
#     explicit_empty_folder = UUID(int=random.getrandbits(128)).hex
#     raw_output_path = f"s3://my-s3-bucket/testdata/{explicit_empty_folder}"
#     provider = FileAccessProvider(local_sandbox_dir="/tmp/unittest", raw_output_prefix=raw_output_path, data_config=dc)
#     ss = provider.get_filesystem("s3").find(raw_output_path)
#     assert len(ss) == 0  # start off with empty folder
#
#     ctx = FlyteContextManager.current_context()
#     local_fd = FlyteDirectory(path=source_folder)
#     local_fd_crawl = local_fd.crawl()
#     local_fd_crawl = [x for x in local_fd_crawl]
#     print(local_fd_crawl)
#     with FlyteContextManager.with_context(ctx.with_file_access(provider)):
#         fd = FlyteDirectory.new_remote()
#         assert raw_output_path in fd.path
#
#         # Write source folder files to new remote path
#         for root_path, suffix in local_fd_crawl:
#             suffix = str(suffix)
#             if "/" in suffix:
#                 subfolder, nested_file = suffix.split("/")
#                 ffd = fd.new_dir(subfolder)
#                 new_file = ffd.new_file(nested_file)
#                 with open(os.path.join(root_path, suffix), "rb") as r:  # noqa
#                     with new_file.open("w") as w:
#                         print(f"Type of w {type(w)} for {new_file.path}")
#                         w.write(str(r.read()))
#             else:
#                 new_file = fd.new_file(suffix)  # noqa
#                 print(f"Writing new file {new_file} from {suffix}")
#                 with open(os.path.join(root_path, suffix), "rb") as r:  # noqa
#                     with new_file.open("w") as w:
#                         print(f"Type of w {type(w)} for {new_file.path}")
#                         w.write(str(r.read()))
#
#         new_crawl = fd.crawl()
#         print(f"new directory {fd}")
#         new_suffix = [y for x, y in new_crawl]
#         old_suffix = [y for x, y in local_fd_crawl]
#         fss = provider.get_filesystem("s3")
#         xx = fss.find(fd.path)
#         print(f"Find: {[x for x in xx]}")
#
#         print(f"new --- {new_suffix}----------")
#         print(f"old ------- {old_suffix}-------")