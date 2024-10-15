# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Set the working directory in the container
WORKDIR /app

# Install system dependencies for building M2Crypto and install Poetry
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    swig \
    curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


# Copy the project files to the container
COPY pyproject.toml poetry.lock /app/

# Install the project dependencies
RUN poetry install --no-dev

# Copy the rest of the application code
COPY . /app

# Expose the port the app will run on
EXPOSE 8000

# Run FastAPI server
CMD ["poetry","run","uvicorn", "--host=0.0.0.0", "--port=8000", "--log-config=logging.ini", "--reload", "datastore_api.main:app"]