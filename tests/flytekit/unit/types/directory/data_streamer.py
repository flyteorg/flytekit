import json
import math
import os
from typing import Annotated

from tqdm import tqdm

import flytekit
from flytekit.types.directory import FlyteDirectory, StreamingKwargs

image = flytekit.ImageSpec(name="mosaic-data-streaming", packages=["datasets", ""])


@flytekit.task(cache=True)
def prepare_hf_dataset_to_jsonl(
    dataset_name: str = "CohereForAI/aya_collection_language_split",
    language: str = "algerian_arabic",
    samples_per_file: int = 1000,
    split: str = "validation",
) -> FlyteDirectory:
    """Data preparation step."""
    from datasets import load_dataset

    output_dir = os.path.join(
        flytekit.current_context().working_directory, "aya_algerian_arabic_jsonl"
    )

    os.makedirs(output_dir, exist_ok=True)
    dataset = load_dataset(dataset_name, language, split=split)
    total_samples = len(dataset)
    num_files = math.ceil(total_samples / samples_per_file)

    for file_idx in range(num_files):
        start_idx = file_idx * samples_per_file
        end_idx = min((file_idx + 1) * samples_per_file, total_samples)

        output_file = os.path.join(output_dir, f"{split}_{file_idx:05d}.jsonl")
        with open(output_file, "w", encoding="utf-8") as f:
            subset = dataset.select(range(start_idx, end_idx))
            for sample in tqdm(subset, total=end_idx - start_idx):
                f.write(json.dumps(dict(sample), ensure_ascii=False) + "\n")

    return output_dir


@flytekit.task
def stream_data(
    dataset: Annotated[
        FlyteDirectory,
        StreamingKwargs(
            shards_config={
                "columns": {
                    "id": "int",
                    "inputs": str,
                    "targets": str,
                    "dataset_name": str,
                    "sub_dataset_name": str,
                    "task_type": str,
                    "template_id": int,
                    "language": str,
                    "split": str,
                    "script": str,
                }
            },
            stream_config={
                "batch_size": 5,
                "sampling_method": "fixed",
                "download_retry": 2,
                "download_timeout": 120,
                "sampling_granularity": 1,
            },
        ),
    ],
) -> int:
    return dataset.num_samples


@flytekit.task
def read_dir_without_streaming(dataset: FlyteDirectory) -> int:
    return 1


@flytekit.workflow
def mosaic_data_streaming() -> int:
    jsonl_dir = prepare_hf_dataset_to_jsonl()
    return stream_data(dataset=jsonl_dir)


@flytekit.workflow
def dir_without_streaming() -> int:
    jsonl_dir = prepare_hf_dataset_to_jsonl()
    return read_dir_without_streaming(dataset=jsonl_dir)
