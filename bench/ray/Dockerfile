# The Ray image for bench/ray: Ray, PyIceberg with pyarrow, and taxibench's
# query module, on python:3.12-slim. Built as carefully as taxibench's own
# PyIceberg image, so the size comparison is with a lean Python image and not
# a careless one: two stages, and no pip cache, tests, headers or bytecode.
#
#     docker build -f bench/ray/Dockerfile -t dista-ray:taxi <staged dir>
FROM python:3.12-slim-bookworm AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache-dir "ray==2.59.0" "pyiceberg[pyarrow]==0.11.1"
RUN find /opt/venv -type d -name '__pycache__' -prune -exec rm -rf {} + \
    && find /opt/venv -type d -name 'tests' -prune -exec rm -rf {} + \
    && find /opt/venv -type d -name 'include' -prune -exec rm -rf {} +

FROM python:3.12-slim-bookworm
COPY --from=build /opt/venv /opt/venv
COPY taxibench.example/python/taxibench /app/taxibench
COPY dista.mojo/bench/ray/taxi_ray.py /app/taxi_ray.py
ENV PATH=/opt/venv/bin:$PATH PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
