FROM python:3.11-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1
# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

#ARG ENVIRONMENT="PROD"
#RUN echo "ENVIRONMENT value is ${ENVIRONMENT}"

# Set the working directory in the container
WORKDIR /app

# Install system dependencies and Poetry
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # expect \
        # perl \
        # policycoreutils \
        # selinux-utils \
        # libreadline-dev \
        # libxml2-dev \
        # python3-dev \
        # libmacaroons-dev \
        # libjson-c-dev \
        # uuid-dev \
        # libssl-dev \
        # libcurl4-openssl-dev \
        # libfuse-dev \
        # fuse \
        git \
        # cmake \
        # make \
        # gcc \
        # g++ \
        # gdb \
        # autoconf \
        # automake \
        curl &&\
        # swig && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy the project files to the container and install
COPY pyproject.toml poetry.lock config.yaml.example logging.ini.example /app/

# Builder stage: install dependencies
FROM base AS builder

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

# # Install dependancies using poetry
# RUN poetry install --no-root

# Development stage: set up development environment
FROM builder AS dev

# Set environment variables
ENV ENVIRONMENT="DEV"

# Install development dependencies
RUN poetry install --with=dev --no-root

# Copy the configuration files
COPY config.yaml.example logging.ini.example /app/
RUN touch hostkey.pem && \
    touch hostcert.pem && \
    cp config.yaml.example config.yaml && \
    cp logging.ini.example logging.ini


# Copy the rest of the application code
COPY . /app

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["poetry","run","uvicorn", "--host=0.0.0.0", "--port=8000", "--log-config=logging.ini", "--reload", "datastore_api.main:app"]

# Production stage: set up production environment
FROM base AS prod

# Set environment variables
ENV ENVIRONMENT="PROD"

COPY --from=builder /root/.local /root/.local

RUN poetry env use python

# # Install production dependencies
# RUN poetry install --without=dev --no-root

# Copy the rest of the application code
COPY . /app

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["poetry","run","uvicorn", "--host=0.0.0.0", "--port=8000", "--log-config=logging.ini", "--reload", "datastore_api.main:app"]