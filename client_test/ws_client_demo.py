"""
WebSocket-клиент который висит и принимает сообщения бесконечно
Прервать: Ctrl+C (чистый выход)
"""
import asyncio
import websockets
import json
from datetime import datetime

async def main():
    url = "ws://localhost:8000/ws"
    
    print("=" * 55)
    print("  WebSocket-клиент — канал /ws")
    print("=" * 55)
    print(f"  URL: {url}")
    print(f"  Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    msg_count = 0
    
    try:
        async with websockets.connect(url) as ws:
            print("  [OK] Соединение установлено")
            print("  Ожидаю сообщения (Ctrl+C для выхода)...\n")
            
            while True:
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(message)
                    msg_count += 1
                    print(f"  [{msg_count}] type={data.get('type')} | time={data.get('timestamp')}")
                except asyncio.TimeoutError:
                    await ws.send("ping")
                    
    except KeyboardInterrupt:
        print(f"\n  [OK] Клиент отключён.")
        print(f"  Принято сообщений: {msg_count}")
    except websockets.exceptions.ConnectionClosed:
        print(f"\n  [OK] Соединение закрыто.")
        print(f"  Принято сообщений: {msg_count}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass