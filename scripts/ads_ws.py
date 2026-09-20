#!/usr/bin/env python3
# ADS 实时大屏 WebSocket 服务：每 2s 轮询 MySQL ads_city_realtime，推给所有浏览器
import asyncio
import json
import pymysql
import websockets

DB = dict(host="localhost", port=3306, user="cdc",
          password="Cdc@123456", database="traffic", charset="utf8mb4")

clients = set()


def fetch():
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT city_name, dt, gps_points, taxi_cnt, avg_speed, load_cnt,"
                " DATE_FORMAT(update_time,'%H:%i:%s') AS update_time"
                " FROM ads_city_realtime ORDER BY city_name")
            return cur.fetchall()
    finally:
        conn.close()


async def handler(ws):
    clients.add(ws)
    try:
        # 连上先推一次
        await ws.send(json.dumps(fetch(), default=str, ensure_ascii=False))
        async for _ in ws:
            pass
    except Exception:
        pass
    finally:
        clients.discard(ws)


async def pusher():
    while True:
        try:
            msg = json.dumps(fetch(), default=str, ensure_ascii=False)
            for ws in list(clients):
                try:
                    await ws.send(msg)
                except Exception:
                    clients.discard(ws)
        except Exception as e:
            print("[warn]", e)
        await asyncio.sleep(2)


async def main():
    print("ADS WebSocket 服务启动: ws://0.0.0.0:8765")
    async with websockets.serve(handler, "0.0.0.0", 8765):
        await pusher()


if __name__ == "__main__":
    asyncio.run(main())
