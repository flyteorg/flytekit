# Flytekit docker Plugin

[docker](https://www.docker.com/) Docker is a platform that uses OS-level virtualization to package and run applications and their dependencies in isolated containers.


With `flytekitplugins-docker`, people easily create a docker image for their workflows using existing docker files.

## Installation
To install the plugin, run the following command:

```bash
pip install flytekitplugins-docker
```

## Usage

### Environment setup

You should have a buildkit compatible docker instance locally. one that is capable of running `docker buildx build ...`

The defaults are generally fine for local building, but you may want to consider setting up a remote builder to take
the computational and network load off of your develoment machine.

__NOTE #1: For production remote builders, follow the [buildkit remote driver](https://docs.docker.com/build/drivers/remote/) documentation.  The following information is for quick isolated environments and is NOT safe!__

__NOTE #2: There are many ways to setup your docker buildx environment, this is just one of many ways to do so.  Please read their documentation for more information__

To setup the temporary environment for remote builders you should:

1. Install a buildkit compatible docker instance locally (not shown)
2. Setup a remote buildkit builder

To do this, run the following command on your builder server, (or follow the [buildkit remote driver](https://docs.docker.com/build/drivers/remote/) for your desired builder-flavor)
```
docker run -d --rm \
  --name=remote-buildkitd \
  --privileged \
  -p 1234:1234 \
  moby/buildkit:latest \
  --addr tcp://0.0.0.0:1234 \
```

3. Register the remote buildkit builder with your local instance.
```
docker buildx create \
  --name flyte-builder \
  --driver remote \
  tcp://{IP to machine}:1234
```

3. Set your environment variables to set the flyte-builder as the default builder:

```
docker buildx use flyte-builder
# This is so that docker-py uses buildkit, may not be necessary
export DOCKER_BUILDKIT=1
export BUILDX_BUILDER=flyte-builder
```

### Python setup


Example:
```python
from flytekit import task
from flytekit.image_spec import ImageSpec

biopython_img = ImageSpec(builder="docker", dockerfile="docker/Dockerfile.biopython")

@task(container_image=biopython_img)
def t1() -> str:
    import Bio
    return Bio.__version__

```

## Developer Notes

The python docker api, aka [docker-py](https://github.com/docker/docker-py) is a bit behind the buildkit integration. See: https://github.com/docker/docker-py/issues/2230


For this reason, I decided to just use simple subprocess commands.  There are alternatives such as [python-on-whales](https://github.com/gabrieldemarmiesse/python-on-whales), but our complexity would have to be significantly larger to justify adding it as a dependency.
