import pandas as pd
import numpy as np
from neuralprophet import NeuralProphet
import matplotlib.pyplot as plt
import pickle

# --- 1. Load and Prepare Data ---
print("Loading training data...")
df = pd.read_csv('training_features.csv')

# Round timestamps to the nearest 10s to ensure perfect alignment
df['ds'] = pd.to_datetime(df['timestamp'], unit='s').dt.round('10s')
df['y'] = df['actual_server_requests']

# Filter to keep only the necessary columns
df_train = df[['ds', 'y', 'total_mention_velocity', 'avg_sentiment_score']].copy()

# Add microscopic noise to all numerical columns to prevent zero-variance crashes
for col in ['y', 'total_mention_velocity', 'avg_sentiment_score']:
    df_train[col] = df_train[col] + np.random.normal(0, 0.001, size=len(df_train))

print(f"Data loaded: {len(df_train)} rows.")

# --- 2. Configure the Neural Network ---
# --- 2. Configure the Neural Network ---
m = NeuralProphet(
    n_forecasts=3,
    n_lags=5,
    epochs=100,
    learning_rate=0.01,
    drop_missing=True  # <--- Added here
)

m.add_lagged_regressor("total_mention_velocity")
m.add_lagged_regressor("avg_sentiment_score")

# --- 3. Train the Model ---
print("🚀 Training the forecasting model...")
metrics = m.fit(df_train)  # <--- Removed from here

print("Training complete!")

# --- 4. Visualize the Predictions ---
forecast = m.predict(df_train)
fig = m.plot(forecast)
plt.title("Actual Server Traffic vs. AI Predicted Traffic")
plt.xlabel("Time")
plt.ylabel("Server Requests")
plt.savefig("forecast_plot.png")
print("📈 Saved forecast visualization to 'forecast_plot.png'")

# --- 5. Export Model for Production ---
from neuralprophet import save
save(m, "scaler_model.np")
print("💾 Model saved natively as 'scaler_model.np'")