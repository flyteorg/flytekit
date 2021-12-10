from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


@dataclass_json
@dataclass
class Level2ProxyConfiguration:
    # File and directory paths kept as 'str' so Flyte doesn't manage these static resources
    splat_data_dir: str
    atmosphere_apriori_file: str
    isrf_lut_file: str  # ISRF lookup table required for methaneair runs


@dataclass_json
@dataclass
class Level2ProxyParameters:
    id: str  # unique identifier for this collection
    job_i_step: int  # step size
    job_j_step: int  # step size
    control_template: str  # name of the control template to use from 'templates' folder


@dataclass_json
@dataclass
class Level2AprioriConfiguration:
    # Directory with static data directories.
    # TODO: Identify what this data is, possibly split into multiple values if necessary.
    static_data_dir: FlyteDirectory
    # Directory for external dynamic data.
    # TODO: Is this just GEOS data? If so, do we need all of it? Can we preprocess?
    external_data_dir: FlyteDirectory
    # Define vertical grid
    lmx: int
    # Albedo Wavelength
    albedo_wavelengths: List[float]


@dataclass_json
@dataclass
class Level2Input:
    level1b_product: FlyteFile
    apriori_config: Level2AprioriConfiguration
    proxy_config: Level2ProxyConfiguration
    proxy_params: Level2ProxyParameters
    level2_output_uri: str


def test_dataclass_complex_transform():
    pass