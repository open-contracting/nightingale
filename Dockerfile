# Use the official Python base image
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY pyproject.toml .

# Install the dependencies
RUN pip install --upgrade pip

# Copy the rest of the application code into the container
COPY . .

# Install the nightigale package
RUN pip install -e .

# Set the entrypoint command
ENTRYPOINT ["nightingale"]
