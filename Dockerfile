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
    curl -sSf https://bootstrap.pypa.io/pip/pip.pyz -o /usr/local/bin/pip.pyz && \
    python3 /usr/local/bin/pip.pyz install pipx && \
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
    apt-get install -y --no-install-recommends cmake 

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
        cmake \
        make \
        gcc \
        g++ \
        gdb \
        autoconf \
        automake \
        swig && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Initialize cmake
RUN cmake --version || true

# Install development dependencies
RUN poetry install --without=dev --no-root

# Copy the rest of the application code
COPY datastore_api/ /app/datastore_api/



# ~~~ Development stage: ~~~#
# Set up development environment
FROM builder AS dev

ENV PATH="/root/.local/bin:$PATH"

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
CMD ["fastapi","run", "/app/datastore_api/main.py"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #



# ~~~Test stage: ~~~#
#Set up testing environment
FROM dev AS test

# Run tests
CMD ["pytest", "--config-file", "pytest.ini"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #



# ~~~Production stage: ~~~#
# Set up production environment
FROM builder AS prod

ENV PATH="/root/.local/bin:$PATH"
WORKDIR /app

# Copy installed Python deps and source code
COPY --from=builder /app /app

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["fastapi","run", "/app/datastore_api/main.py"]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #