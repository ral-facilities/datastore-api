FROM python:3.11-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.local/bin:$PATH"

# Set the working directory in the container
WORKDIR /app

# Install system dependencies and Poetry
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl &&\
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy the project files to the container and install
COPY pyproject.toml poetry.lock /app/

# Builder stage: install dependencies
FROM base AS builder

ENV PATH="/root/.local/bin:$PATH"
RUN poetry config virtualenvs.create false


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        expect \
        perl \
        policycoreutils \
        selinux-utils \
        libreadline-dev \
        libxml2-dev \
        python3-dev \
        libmacaroons-dev \
        libjson-c-dev \
        uuid-dev \
        libssl-dev \
        libcurl4-openssl-dev \
        libfuse-dev \
        fuse \
        make \
        gcc \
        g++ \
        gdb \
        autoconf \
        automake \
        cmake \
        swig && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install development dependencies
RUN poetry install --without=dev --no-root



# ~~~ Development stage: ~~~#
# Set up development environment
FROM builder AS dev

ENV PATH="/root/.local/bin:$PATH"

# Copy the rest of the application code
COPY datastore_api/ /app/datastore_api/

# Install development dependencies
RUN poetry install --with dev 

# Copy the project files to the container and install
COPY config.yaml.example logging.ini.example /app/
COPY pytest.ini.docker /app/pytest.ini
COPY tests/ /app/tests/

RUN touch hostkey.pem && \
    touch hostcert.pem && \
    cp config.yaml.example config.yaml && \
    cp logging.ini.example logging.ini

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["fastapi","run",  "--host=0.0.0.0", "--port=8000", "--reload" ,"/app/datastore_api/main.py"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #



# ~~~Test stage: ~~~#
#Set up testing environment
FROM dev AS test

# Run tests
CMD ["pytest", "--config-file", "pytest.ini"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #



# ~~~Production stage: ~~~#
# Set up production environment
FROM python:3.11-slim AS prod
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.local/bin:$PATH"
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libfuse2 \
        libssl3 \
        libxml2 \
        libcurl4 \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy installed Python deps and source code
COPY --from=builder /usr/local /usr/local
COPY pyproject.toml poetry.lock /app/
COPY datastore_api/ /app/datastore_api/
RUN python -m pip install .

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["fastapi","run", "/app/datastore_api/main.py", "--reload", "--host", "0.0.0.0" , "--port", "8000"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #