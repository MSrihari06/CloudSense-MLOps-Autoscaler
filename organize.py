import os
import shutil

# Define the target folder structure
folders = ["src", "models", "k8s", "data"]

# Define which files go into which folders
files_to_move = {
    "producer.py": "src",
    "processor.py": "src",
    "feature_engineer.py": "src",
    "metric_server.py": "src",
    "scaler_model.np": "models",
    "scaler_model.pkl": "models",  # Moving the pickle file too just in case
    "ai-scaler.yaml": "k8s",
    "app-deployment.yaml": "k8s"
}

print("🚀 Starting project reorganization...\n")

# 1. Create the Folders
for folder in folders:
    os.makedirs(folder, exist_ok=True)
    print(f"📁 Ensured directory exists: {folder}/")

# 2. Move the Files
for file, dest_folder in files_to_move.items():
    if os.path.exists(file):
        dest_path = os.path.join(dest_folder, file)
        shutil.move(file, dest_path)
        print(f"📦 Moved {file} -> {dest_path}")
    else:
        print(f"⚠️ Could not find {file} (it might already be moved)")

# 3. Generate the .env file
env_content = """# Kafka Configuration
KAFKA_BROKER=kafka:29092
INPUT_TOPIC=social-buzz
OUTPUT_TOPIC=analyzed-buzz

# Pipeline Tuning
BATCH_SIZE=5
POLL_TIMEOUT=1.0

# AI & Kubernetes Settings
CAPACITY_PER_POD=100
SAFE_BASELINE_PODS=5
"""
with open(".env", "w") as f:
    f.write(env_content)
print("\n📄 Generated .env file")

# 4. Generate requirements.txt
req_content = """confluent-kafka==2.3.0
polars==0.20.10
pandas==2.2.0
transformers==4.38.2
torch==2.6.0
neuralprophet==0.7.1
prometheus_client==0.20.0
python-dotenv==1.0.1
"""
with open("requirements.txt", "w") as f:
    f.write(req_content)
print("📄 Generated requirements.txt file")

# 5. Generate the Dockerfile
docker_content = """# Use a lightweight Python base image
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
"""
with open("Dockerfile", "w") as f:
    f.write(docker_content)
print("📄 Generated Dockerfile")

print("\n✅ Project reorganization complete! Your repository is now fully structured.")