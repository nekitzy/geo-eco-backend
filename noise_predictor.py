import os
import numpy as np
from datetime import datetime
from typing import Dict, Optional
import joblib

class NoisePredictionModel:
    """Загрузка и использование нейросети для предсказания шума"""
    
    def __init__(self):
        self.model = None
        self.scaler_X = None
        self.scaler_y = None
        self.metadata = None
        self.model_path = os.path.join(os.path.dirname(__file__), 'models')
        self.load_model()
    
    def load_model(self):
        """Загрузка модели с обработкой ошибок совместимости"""
        try:
            from tensorflow import keras
            
            model_files = [
                'noise_prediction_model.keras',      
                'noise_prediction_model.h5',         
                'model_architecture.json'            
            ]
            
            model_loaded = False
            
            for model_file in model_files:
                filepath = os.path.join(self.model_path, model_file)
                if os.path.exists(filepath):
                    print(f"Найдена модель: {model_file}")
                    
                    try:
                        if model_file.endswith('.keras'):
                            self.model = keras.models.load_model(filepath)
                            model_loaded = True
                            break
                        
                        elif model_file.endswith('.h5'):
                            self.model = keras.models.load_model(
                                filepath,
                                compile=False
                            )

                            
                            self.model.compile(
                                optimizer=keras.optimizers.Adam(learning_rate=0.001),
                                loss='mse',
                                metrics=['mae']
                            )
                            print("Модель скомпилирована вручную")
                            model_loaded = True
                            break
                        
                        elif model_file.endswith('.json'):
                            with open(filepath, 'r', encoding='utf-8') as f:
                                model_architecture = f.read()
                            self.model = keras.models.model_from_json(model_architecture)
                            
                            weights_file = os.path.join(self.model_path, 'noise_model_weights.weights.h5')
                            if os.path.exists(weights_file):
                                self.model.load_weights(weights_file)
                                print("Модель загружена (архитектура + веса)")
                                
                                self.model.compile(
                                    optimizer=keras.optimizers.Adam(learning_rate=0.001),
                                    loss='mse',
                                    metrics=['mae']
                                )
                                model_loaded = True
                            break
                    
                    except ValueError as e:
                        print(f"Ошибка загрузки {model_file}: {e}")
                        continue
            
            if not model_loaded:
                print("Модель не найдена или не загружена")
                return False
            
            scaler_X_file = os.path.join(self.model_path, 'scaler_X.pkl')
            if os.path.exists(scaler_X_file):
                self.scaler_X = joblib.load(scaler_X_file)
                print("scaler_X загружен")
            
            scaler_y_file = os.path.join(self.model_path, 'scaler_y.pkl')
            if os.path.exists(scaler_y_file):
                self.scaler_y = joblib.load(scaler_y_file)
                print("scaler_y загружен")
            
            metadata_file = os.path.join(self.model_path, 'model_metadata.pkl')
            if os.path.exists(metadata_file):
                self.metadata = joblib.load(metadata_file)
                print("Метаданные загружены")
            
            return True
            
        except Exception as e:
            print(f"Ошибка загрузки модели: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def calculate_features(self, lat: float, lng: float) -> Dict[str, float]:
        """Расчёт признаков для координат"""
        from math import radians, sin, cos, sqrt, atan2
        
        def haversine(lat1, lng1, lat2, lng2):
            R = 6371
            dlat = radians(lat2 - lat1)
            dlng = radians(lng2 - lng1)
            a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            return R * c
        
        ROADS = [(54.842500, 83.090000), (54.846700, 83.106700), (54.840000, 83.100000)]
        PARKS = [(54.850000, 83.110000)]
        SCHOOLS = [(54.845000, 83.095000)]
        COMMERCIAL = [(54.840000, 83.100000)]
        RESIDENTIAL = [(54.838000, 83.095000)]
        
        features = {}
        features['road_distance'] = min([haversine(lat, lng, r[0], r[1]) for r in ROADS])
        features['park_distance'] = min([haversine(lat, lng, p[0], p[1]) for p in PARKS])
        features['school_distance'] = min([haversine(lat, lng, s[0], s[1]) for s in SCHOOLS])
        features['commercial_distance'] = min([haversine(lat, lng, c[0], c[1]) for c in COMMERCIAL])
        features['residential_distance'] = min([haversine(lat, lng, r[0], r[1]) for r in RESIDENTIAL])
        
        return features
    
    def predict(self, lat: float, lng: float, epochs: int = 10) -> Optional[Dict]:
        """Предсказание уровня шума"""
        if not self.model:
            print("⚠️ Модель не загружена, используем симуляцию")
            return self._predict_with_rules(lat, lng)
        
        try:
            features = self.calculate_features(lat, lng)
            
            X = np.array([[
                lat, lng,
                features['road_distance'],
                features['park_distance'],
                features['school_distance'],
                features['commercial_distance'],
                features['residential_distance']
            ]])
            
            print(f"Входные данные: {X}")
            
            if self.scaler_X:
                X = self.scaler_X.transform(X)
            
            prediction = self.model.predict(X, verbose=0)
            print(f"Предсказание (нормализованное): {prediction}")
            
            if self.scaler_y:
                prediction = self.scaler_y.inverse_transform(prediction)
            
            print(f"Предсказание (реальное): {prediction}")
            
            noise_level = float(prediction[0][0])
            
            return {
                'noise_level': round(noise_level, 1),
                'violations': int((noise_level - 50) * 2) if noise_level > 50 else 0,
                'features': features,
                'confidence': float(self.metadata['accuracy']) if self.metadata else 0.87,
                'model_used': True
            }
            
        except Exception as e:
            print(f"Ошибка предсказания: {type(e).__name__}: {e}")
            return self._predict_with_rules(lat, lng)
    
    def _predict_with_rules(self, lat: float, lng: float) -> dict:
        """Предсказание на основе правил (фоллбэк)"""
        features = self.calculate_features(lat, lng)
        base_noise = 45.0
        
        if features['road_distance'] < 0.3:
            base_noise += 25
        elif features['road_distance'] < 0.5:
            base_noise += 15
        
        if features['school_distance'] < 0.2:
            base_noise += 15
        
        if features['park_distance'] < 0.3:
            base_noise -= 15
        
        if features['commercial_distance'] < 0.3:
            base_noise += 20
        
        base_noise += np.random.uniform(-3, 3)
        noise_level = max(30, min(95, base_noise))
        
        return {
            'noise_level': round(noise_level, 1),
            'violations': int((noise_level - 50) * 2) if noise_level > 50 else 0,
            'features': features,
            'confidence': 0.75,
            'model_used': False
        }

predictor = NoisePredictionModel()
