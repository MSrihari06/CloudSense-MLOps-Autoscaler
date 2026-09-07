# ☁️ CloudSense: Predictive MLOps Autoscaler 🚀

An end-to-end, AI-driven streaming pipeline that predicts viral server load in real-time and proactively pre-warms Kubernetes infrastructure before traffic bottlenecks occur.

## 🏗️ Architecture & Data Flow
* **Ingestion:** Real-time social media APIs stream live JSON events into a local **Redpanda (Kafka)** broker.
* **Processing:** **Transformers (RoBERTa)** analyzes text sentiment, while **Polars/Pandas** aggregates the stream into 10-second tumbling time windows.
* **Intelligence:** A **NeuralProphet (PyTorch)** neural network reads the continuous tensors to forecast imminent traffic volume.
* **Action:** **Prometheus** scrapes the AI's metrics, triggering **KEDA** to elastically scale **Kubernetes** pods.

## ⚙️ Key Engineering Features
* **Proactive Scale-Out:** Transitions cloud infrastructure from reactive crash-recovery to predictive pre-warming.
* **Continuous Tensor Healing:** Utilizes forward/backward filling (`ffill`/`bfill`) and microscopic variance injection to prevent PyTorch matrix dimension collapses during time-series data gaps.
* **Timezone Drift Correction:** Aligns UTC timestamps to local system clocks to prevent the autoscaler from misidentifying fresh streams as stale data.
* **Fully Containerized:** The entire multi-node streaming and ML inference architecture launches locally via a unified Docker Compose network.

## 🚀 Quickstart Guide

**1. Install Python dependencies:**
```bash
pip install -r requirements.txt

2. Configure Environment:
Update your .env file with your Kafka broker and KEDA baseline targets.

3. Launch the containerized pipeline:

Bash
docker-compose up --build -d
4. Watch the AI autonomously scale the cluster:

Bash
kubectl get pods -l app=web-server -w
