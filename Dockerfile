# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies required for ML libraries
RUN apt-get update && apt-get install -y build-essential

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code and models into the container
COPY src/ ./src/
COPY models/ ./models/

# Set Python to run in unbuffered mode so logs print instantly
ENV PYTHONUNBUFFERED=1
