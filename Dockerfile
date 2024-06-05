# Use the official Python base image
FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Install the nightigale package
RUN pip install -e .

# Set the entrypoint command
ENTRYPOINT ["nightingale"]
