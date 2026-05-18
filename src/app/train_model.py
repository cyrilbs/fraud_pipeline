# train_model.py
from sklearn.ensemble import RandomForestClassifier
import numpy as np
import joblib

# fake training data
X = np.array([
    [100, 0.1],
    [250, 0.8],
    [80, 0.2],
    [500, 0.9],
])
y = [0, 1, 0, 1]  # 0 = legit, 1 = fraud

model = RandomForestClassifier()
model.fit(X, y)

# save model
joblib.dump(model, "models/fraud_model.pkl")