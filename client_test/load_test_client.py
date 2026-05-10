"""
Load Test Client для геоэкологического мониторинга
Тестирует производительность REST API и WebSocket
"""
import asyncio
import time
import statistics
from datetime import datetime
from typing import List, Dict
import requests
from websockets.sync.client import connect
import threading

class LoadTester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.results = {
            "rest": [],
            "websocket": []
        }
        self.token = None
        
    def login(self, login: str = "viewer", password: str = "viewer123"):
        """Получить JWT токен"""
        response = requests.post(
            f"{self.base_url}/api/auth/login",
            json={"login": login, "password": password}
        )
        if response.status_code == 200:
            self.token = response.json()["access_token"]
            print(f"✅ Авторизация успешна")
            return True
        return False
    
    def test_rest_endpoint(self, endpoint: str, method: str = "GET") -> float:
        """Тестирование одного REST endpoint"""
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        
        start = time.time()
        try:
            if method == "GET":
                response = requests.get(f"{self.base_url}{endpoint}", headers=headers, timeout=5)
            elif method == "POST":
                response = requests.post(f"{self.base_url}{endpoint}", headers=headers, json={}, timeout=5)
            
            elapsed = (time.time() - start) * 1000  # мс
            
            if response.status_code == 200:
                self.results["rest"].append(elapsed)
                return elapsed
            else:
                print(f"⚠️ {endpoint} → {response.status_code}")
                return -1
                
        except Exception as e:
            print(f"❌ Ошибка {endpoint}: {e}")
            return -1
    
    def test_websocket_connection(self, duration: int = 5):
        """Тестирование WebSocket подключения"""
        try:
            start = time.time()
            with connect(f"ws://localhost:8000/ws") as websocket:
                # Отправляем ping
                websocket.send("ping")
                
                # Ждём pong
                response = websocket.recv()
                elapsed = (time.time() - start) * 1000
                
                if "pong" in response:
                    self.results["websocket"].append(elapsed)
                    print(f"✅ WebSocket: {elapsed:.1f} мс")
                    return elapsed
                else:
                    print(f"⚠️ WebSocket: неожиданный ответ")
                    return -1
                    
        except Exception as e:
            print(f"❌ WebSocket ошибка: {e}")
            return -1
    
    def run_concurrent_rest_test(self, endpoint: str, num_requests: int = 50, num_workers: int = 5):
        """Запуск параллельных REST запросов"""
        print(f"\n🚀 Тест: {endpoint}")
        print(f"   Запросов: {num_requests}, Параллельно: {num_workers}")
        
        threads = []
        
        def worker():
            for _ in range(num_requests // num_workers):
                self.test_rest_endpoint(endpoint)
        
        start_time = time.time()
        
        # Запускаем потоки
        for _ in range(num_workers):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()
        
        # Ждём завершения
        for t in threads:
            t.join()
        
        total_time = time.time() - start_time
        
        # Статистика
        if self.results["rest"]:
            self._print_stats("REST", total_time)
    
    def run_websocket_test(self, num_connections: int = 10):
        """Тестирование множественных WebSocket подключений"""
        print(f"\n🚀 WebSocket тест: {num_connections} подключений")
        
        def ws_client():
            self.test_websocket_connection()
        
        threads = []
        start_time = time.time()
        
        for _ in range(num_connections):
            t = threading.Thread(target=ws_client)
            threads.append(t)
            t.start()
            time.sleep(0.1)  # Небольшая задержка между подключениями
        
        for t in threads:
            t.join()
        
        total_time = time.time() - start_time
        
        if self.results["websocket"]:
            self._print_stats("WebSocket", total_time)
    
    def _print_stats(self, test_type: str, total_time: float):
        """Вывод статистики"""
        results = self.results[test_type.lower()]
        
        if not results:
            print("❌ Нет результатов")
            return
        
        print(f"\n📊 Статистика {test_type}:")
        print(f"   Всего запросов: {len(results)}")
        print(f"   Общее время: {total_time:.2f} с")
        print(f"   RPS (запросов/сек): {len(results)/total_time:.1f}")
        print(f"   Среднее время: {statistics.mean(results):.1f} мс")
        print(f"   Медиана: {statistics.median(results):.1f} мс")
        print(f"   Min: {min(results):.1f} мс")
        print(f"   Max: {max(results):.1f} мс")
        
        if len(results) > 1:
            print(f"   Std Dev: {statistics.stdev(results):.1f} мс")
        
        # Проццентили
        sorted_results = sorted(results)
        p95_idx = int(len(sorted_results) * 0.95)
        p99_idx = int(len(sorted_results) * 0.99)
        
        print(f"   P95 (95% <): {sorted_results[p95_idx]:.1f} мс")
        print(f"   P99 (99% <): {sorted_results[p99_idx]:.1f} мс")


def main():
    print("="*60)
    print("🔥 LOAD TEST - Геоэкологический мониторинг")
    print("="*60)
    
    tester = LoadTester("http://localhost:8000")
    
    # Авторизация
    print("\n🔑 Авторизация...")
    if not tester.login():
        print("❌ Не удалось авторизоваться. Запустите сервер!")
        return
    
    # Тест 1: REST API - получение датчиков
    tester.run_concurrent_rest_test(
        endpoint="/api/sensors",
        num_requests=50,
        num_workers=5
    )
    
    # Тест 2: REST API - последние измерения
    tester.run_concurrent_rest_test(
        endpoint="/api/measurements/latest",
        num_requests=50,
        num_workers=5
    )
    
    # Тест 3: REST API - статистика
    tester.run_concurrent_rest_test(
        endpoint="/api/measurements/stats?hours=24",
        num_requests=30,
        num_workers=3
    )
    
    # Тест 4: WebSocket подключения
    tester.run_websocket_test(num_connections=10)
    
    print("\n" + "="*60)
    print("✅ Тестирование завершено!")
    print("="*60)


if __name__ == "__main__":
    main()