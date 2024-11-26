"""Eager workflow integration tests.

These tests are currently not run in CI. In order to run this locally you'll need to start a
local flyte cluster, and build and push a flytekit development image:

```

# if you already have a local cluster running, tear it down and start fresh
flytectl demo teardown -v

# start a local flyte cluster
flytectl demo start

# build and push the image
docker build . -f Dockerfile.dev -t localhost:30000/flytekit:dev --build-arg PYTHON_VERSION=3.9
docker push localhost:30000/flytekit:dev

# run the tests
pytest tests/flytekit/integration/experimental/test_eager_workflows.py
```

These test will need to be re-written
"""
