import asyncio, websockets, json

async def m():
    async with websockets.connect("ws://localhost:8765") as ws:
        msg = await ws.recv()
        print("收到推送:")
        print(json.dumps(json.loads(msg), ensure_ascii=False, indent=2))

asyncio.run(m())
