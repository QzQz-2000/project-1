import os
import joblib
import pickle
from pathlib import Path
from collections import OrderedDict

import xgboost as xgb
import lightgbm as lgb
import torch
import tensorflow as tf
from tensorflow import keras


class ModelLoader:
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = self._load_model(model_path)

    def _load_model(self, path):
        ext = Path(path).suffix.lower()
        
        if ext in ['.pkl', '.joblib']:
            return self._load_pickle_model(path)
        elif ext in ['.model', '.json', '.bin']:
            return self._load_xgboost(path)
        elif ext in ['.txt', '.lgb']:
            return self._load_lightgbm(path)
        elif ext in ['.h5', '.keras']:
            return keras.models.load_model(path)
        elif ext in ['.pt', '.pth']:
            return torch.load(path, map_location=torch.device('cpu'))
        elif os.path.isdir(path):
            return tf.keras.models.load_model(path)
        else:
            raise ValueError(f"Unsupported model format: {ext or 'directory'}")

    def _load_pickle_model(self, path):
        try:
            return joblib.load(path)
        except:
            with open(path, 'rb') as f:
                return pickle.load(f)

    def _load_xgboost(self, path):
        model = xgb.Booster()
        model.load_model(path)
        return model

    def _load_lightgbm(self, path):
        return lgb.Booster(model_file=path)

    def predict(self, X):
        if hasattr(self.model, 'predict'):
            return self.model.predict(X)
        elif isinstance(self.model, xgb.Booster):
            return self.model.predict(xgb.DMatrix(X))
        elif isinstance(self.model, lgb.Booster):
            return self.model.predict(X)
        elif isinstance(self.model, torch.nn.Module):
            self.model.eval()
            with torch.no_grad():
                return self.model(torch.tensor(X).float()).numpy()
        else:
            raise NotImplementedError("Predict not implemented for this model type.")


class ModelCache:
    def __init__(self, max_size=5):
        self.cache = OrderedDict()
        self.max_size = max_size

    def get_model(self, path):
        if path in self.cache:
            self.cache.move_to_end(path)
            return self.cache[path]
        else:
            loader = ModelLoader(path)
            self.cache[path] = loader
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)
            return loader
