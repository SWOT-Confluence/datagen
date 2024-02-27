# Stage 0 - Create from Python3.9.5 image
# FROM python:3.9-slim-buster as stage0
FROM python:3.9-slim-buster

# Stage 1 - Debian dependencies
# FROM stage0 as stage1
RUN apt update \
        && DEBIAN_FRONTEND=noninteractive apt install -y curl zip python3-dev build-essential libhdf5-serial-dev netcdf-bin libnetcdf-dev wget

# # Stage 2 - Input Python dependencies
# # FROM stage1 as stage2
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python -m venv /app/env \
        && /app/env/bin/pip install -r /app/requirements.txt

# Stage 5 - Copy and execute module
# FROM stage3 as stage4
COPY ./datagen /app/datagen/
COPY ./sets /app/sets/
COPY conf.py /app/conf.py
COPY conf_lake.py /app/conf_lake.py
COPY generate_data.py /app/generate_data.py
COPY generate_data_lake.py /app/generate_data_lake.py
COPY generate.py /app/generate.py
LABEL version="1.0" \
        description="Containerized datagen module."
ENTRYPOINT ["/app/env/bin/python3", "/app/generate.py"]