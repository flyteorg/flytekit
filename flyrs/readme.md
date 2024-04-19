### How to build Rust FlyteRemote Client?
1. Instal `matruin` to build Python packages with Rust `PyO3`.
    - `pip install maturin`
2. Compile Python extension and instal it into local Python virtual environment root.
    - `maturin develop`
    - or `maturin develop --release` (Pass `--release` to `cargo build`)

### How to test Rust FlyteRemote Client?
1. `pyflyte register ./t.py` to get flyte entity `version` id
2. Set previous fetched `version` id in `./test_flyte_remote.py`'s `VERSION_ID`
2. `pytest ./test_flyte_remote.py` inside `flyrs/`