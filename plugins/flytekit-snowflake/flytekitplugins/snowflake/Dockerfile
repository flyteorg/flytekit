FROM python:3.8-slim-buster

WORKDIR /app
ENV VENV /opt/venv
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONPATH /app

RUN pip install awscli
RUN pip install gsutil

#RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.16-linux_x86_64.bash | bash

# Virtual environment
RUN python3.8 -m venv ${VENV}
RUN ${VENV}/bin/pip install wheel

COPY snowflake/requirements.txt /app
RUN ${VENV}/bin/pip install -r /app/requirements.txt

# Copy over the helper script that the SDK relies on
RUN cp ${VENV}/bin/flytekit_venv /usr/local/bin
RUN chmod a+x /usr/local/bin/flytekit_venv

# Enable the virtualenv for this image. Note this relies on the VENV variable we've set in this image.
ENTRYPOINT ["/usr/local/bin/flytekit_venv"]