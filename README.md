\# Система геоэкологического мониторинга Академгородка



Серверная часть для сбора, хранения и анализа данных с датчиков шума и вибрации (Байкал-8).



\## Технологии



\- \*\*FastAPI\*\* — веб-фреймворк

\- \*\*PostgreSQL\*\* — база данных

\- \*\*SQLAlchemy\*\* — ORM

\- \*\*ObsPy\*\* — работа с miniSEED файлами

\- \*\*SeedLink\*\* — подключение к Байкал-8 в реальном времени

\- \*\*JWT\*\* — авторизация

\- \*\*WebSocket\*\* — real-time уведомления



\## Установка и запуск



\### 1. Клонировать репозиторий



```bash

git clone <url>

cd project/backend

### 2. Установить зависимости
pip install -r requirements.txt



\### 3. Настроить .env
Скопировать шаблон и заполнить свои данные:

cp .env.example .env



Обязательно сгенерировать JWT ключ:

python -c "import secrets; print(secrets.token\_urlsafe(32))"



И вставить в .env → JWT\_SECRET\_KEY=...







\### 4. Запустить сервер

python main.py

Сервер будет доступен на http://localhost:8000





\### 5. Вход в систему

Логин	Пароль		Роль

admin	admin123	Администратор

viewer	viewer123	Просмотр

Документация API: http://localhost:8000/docs



Структура проекта

backend/

├── main.py                    	# FastAPI приложение

├── config.py                  	# Настройки (из .env)

├── models.py                  	# Модели БД

├── mseed\_generator.py         	# Генератор miniSEED

├── mseed\_monitor.py           	# Монитор miniSEED → БД

├── baikal\_vibration\_monitor.py 	# Монитор вибрации Байкал-8

├── baikal\_seedlink\_collector.py 	# SeedLink сборщик

├── seismic\_data\_handler.py    	# Обработчик сейсмических данных

├── .env.example               	# Шаблон настроек

├── requirements.txt           	# Зависимости

└── data/                      	# Данные (miniSEED, архив)



Основные API-эндпоинты
Метод	Путь				Описание

POST	/api/auth/login			Вход в систему

GET	/api/sensors			Список датчиков

GET	/api/sensors/{id}/measurements	Измерения датчика

GET	/api/measurements/latest	Последние измерения

GET	/api/measurements/stats		Статистика

GET	/api/anomalies/recent		Последние аномалии

WS	/ws				Real-time уведомления

WS	/ws/mseed			Поток miniSEED

WS	/ws/vibration			Поток вибрации



## 🧪 Запуск тестов
1. Запусти сервер: `cd backend && python main.py`
2. В новом терминале: `cd client_test && python api_client_full.py`
3. Нагрузочный тест: `python load_test_client.py`
4. WebSocket демо: `python ws_client_demo.py`


Автор:

backend: @nekit4ch





