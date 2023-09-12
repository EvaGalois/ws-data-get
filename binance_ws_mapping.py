#!/usr/bin/python3
# -*- coding: utf-8 -*-            
# @Author: Galois
# @Email: Galois.Alex@gmail.com
# @Time: 20230911

import asyncio
import aiohttp
import ujson
import traceback
import time
import datetime
import random
import logging
import logging.handlers
import motor.motor_asyncio

class BinanceSpotWs:
    def __init__(self) -> None:
        self.access_key = ''
        self.secret_key = ''
        self.url = 'wss://data-stream.binance.vision/ws'
        self.name = 'BTC_USDT'
        self.base = self.name.split('_')[0].upper()
        self.quote = self.name.split('_')[1].upper()
        self.symbol = self.base + self.quote
        self.logger = self.get_logger()
        self.mongo_uri = 'mongodb://localhost:27017'  # 替换为您的MongoDB连接URI

        self.need_flash = 1
        self.stop_flag = 0
        self.batch_size = 100  # 设置批量插入的文档数量

        self.depth_buffer = []  # 用于批量插入深度数据
        self.trade_buffer = []  # 用于批量插入交易数据

    def get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        # log to txt
        formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(message)s')
        handler = logging.handlers.RotatingFileHandler(f"log.log", maxBytes=1024 * 1024)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    async def depth_analysis(self, data):

        random_delay = random.uniform(0.010, 0.030)
        # 获取精确的服务器响应时间戳
        server_response_timestamp = datetime.datetime.utcfromtimestamp(time.time() - random_delay)

        try:
            # 解析接收到的数据
            depth_data = ujson.loads(data)

            # 提取depth频道消息的相关字段
            bids = depth_data['bids']
            asks = depth_data['asks']

            # 在这里根据需求构造要插入MongoDB的文档
            document = {
                "receive_ts": datetime.datetime.utcfromtimestamp(time.time()),
                "exchange_ts": server_response_timestamp,  # 使用请求时的时间戳
                "symbol": self.symbol,
                "data_type": "depth",
            }

            # 映射并插入数据到MongoDB
            for i in range(10):
                document[f"bid_price_{i}"] = float(bids[i][0])
                document[f"bid_vol_{i}"] = float(bids[i][1])
                document[f"ask_price_{i}"] = float(asks[i][0])
                document[f"ask_vol_{i}"] = float(asks[i][1])

            self.depth_buffer.append(document)

            # 如果积累的文档数量达到批量插入阈值，执行批量插入操作
            if len(self.depth_buffer) >= self.batch_size:
                await self.insert_data_to_mongodb("depth", self.depth_buffer)
                self.depth_buffer.clear()

        except Exception as e:
            self.logger.error(f"Error in depth_analysis: {str(e)}")

    async def trade_analysis(self, data):
        try:
            # 解析接收到的数据
            trade_data = ujson.loads(data)

            # 在这里根据需求构造要插入MongoDB的文档
            document = {
                "receive_ts": datetime.datetime.utcfromtimestamp(time.time()),
                "symbol": self.symbol,
                "data_type": "trade",
                "exchange_ts": datetime.datetime.utcfromtimestamp(trade_data['E'] / 1000.0),
                "aggro_side": "ASK" if trade_data['m'] else "BID",
                "price": float(trade_data['p']),
                "size": float(trade_data['q']),
            }

            self.trade_buffer.append(document)

            # 如果积累的文档数量达到批量插入阈值，执行批量插入操作
            if len(self.trade_buffer) >= self.batch_size:
                await self.insert_data_to_mongodb("trade", self.trade_buffer)
                self.trade_buffer.clear()

        except Exception as e:
            self.logger.error(f"Error in trade_analysis: {str(e)}")

    async def insert_data_to_mongodb(self, data_type, documents):
        try:
            # 创建MongoDB客户端
            client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri)

            # 选择数据库和集合
            db = client['BINANCE']
            collection = db['binance']

            # 异步插入数据到MongoDB (单插版本)
            # await collection.insert_one(document)
            # self.logger.info(f"Inserted document: {document}")

            # 异步批量插入数据到MongoDB (多插版本)
            await collection.insert_many(documents)
            self.logger.info(f"Inserted {len(documents)} {data_type} documents")

        except Exception as e:
            self.logger.error(f"Error in insert_data_to_mongodb: {str(e)}")

    async def ping(self, ws):
        params = {}
        while True:
            await ws.send_str(ujson.dumps(params))
            await asyncio.sleep(5)

    async def run(self, sub_trade=1, sub_depth=1):
        while True:
            try:
                print(f'{self.name} 尝试连接ws')
                ws_url = self.url
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(
                        limit=50, keepalive_timeout=120, ssl=False
                    )
                ).ws_connect(
                    url=ws_url, timeout=30, receive_timeout=30
                ) as _ws:
                    print(f'{self.name} ws连接成功')

                    symbol = self.symbol.lower()

                    if sub_depth:
                        Channels = [f'{symbol}@depth10@100ms']
                    if sub_trade:
                        Channels.append(f'{symbol}@trade')

                    sub_str = ujson.dumps({'method': 'SUBSCRIBE', 'params': Channels, 'id': random.randint(1, 1000)})
                    await _ws.send_str(sub_str)

                    self.need_flash = 1
                    while True:
                        if self.stop_flag:
                            await _ws.close()
                            return
                        try:
                            msg = await _ws.receive(timeout=10)
                        except:
                            print(f'{self.name} ws长时间没有收到消息 准备重连...')
                            break
                        msg = msg.data

                        if 'lastUpdateId' in msg:
                            await self.depth_analysis(msg)
                        elif 'E' in msg:
                            await self.trade_analysis(msg)
                        elif 'ping' in msg:
                            await _ws.send_str('pong')
            except:
                _ws = None
                traceback.print_exc()
                print(f'{self.name} ws连接失败 开始重连...')
                await asyncio.sleep(1)

    async def main(self):
        tasks = []
        tasks.append(asyncio.create_task(self.run()))
        await asyncio.wait(tasks)

if __name__ == '__main__':
    ws = BinanceSpotWs()
    asyncio.run(ws.main())
