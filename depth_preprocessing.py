#!/usr/bin/python3
# -*- coding: utf-8 -*-            
# @Author: Galois
# @Email: Galois.Alex@gmail.com
# @Time: 20230911

from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017')

db = client['BINANCE']
collection = db['binance']

# 执行查询并返回游标
cursor = collection.find({"data_type": "depth"})
# cursor = collection.find({})

# # 遍历游标并访问文档
# for document in cursor:
#     print(document)

# 指定CSV文件名
csv_file = 'btc_depth.csv'

# 打开CSV文件并创建CSV写入器
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)

    # 写入CSV文件头部
    header = ["exchange_ts", "receive_ts",
              "bid_price_0", "bid_vol_0",
              "ask_price_0", "ask_vol_0",
              "bid_price_1", "bid_vol_1",
              "ask_price_1", "ask_vol_1",
              "bid_price_2", "bid_vol_2",
              "ask_price_2", "ask_vol_2",
              "bid_price_3", "bid_vol_3",
              "ask_price_3", "ask_vol_3",
              "bid_price_4", "bid_vol_4",
              "ask_price_4", "ask_vol_4",
              "bid_price_5", "bid_vol_5",
              "ask_price_5", "ask_vol_5",
              "bid_price_6", "bid_vol_6",
              "ask_price_6", "ask_vol_6",
              "bid_price_7", "bid_vol_7",
              "ask_price_7", "ask_vol_7",
              "bid_price_8", "bid_vol_8",
              "ask_price_8", "ask_vol_8",
              "bid_price_9", "bid_vol_9",
              "ask_price_9", "ask_vol_9",]
              
    writer.writerow(header)

    # 遍历游标并写入数据
    for document in cursor:
        # 替换字段名称以匹配文档的键
        row = [document["exchange_ts"], document["receive_ts"],
               document["bid_price_0"], document["bid_vol_0"],
               document["ask_price_0"], document["ask_vol_0"],
               document["bid_price_1"], document["bid_vol_1"],
               document["ask_price_1"], document["ask_vol_1"],
               document["bid_price_2"], document["bid_vol_2"],
               document["ask_price_2"], document["ask_vol_2"],
               document["bid_price_3"], document["bid_vol_3"],
               document["ask_price_3"], document["ask_vol_3"],
               document["bid_price_4"], document["bid_vol_4"],
               document["ask_price_4"], document["ask_vol_4"],
               document["bid_price_5"], document["bid_vol_5"],
               document["ask_price_5"], document["ask_vol_5"],
               document["bid_price_6"], document["bid_vol_6"],
               document["ask_price_6"], document["ask_vol_6"],
               document["bid_price_7"], document["bid_vol_7"],
               document["ask_price_7"], document["ask_vol_7"],
               document["bid_price_8"], document["bid_vol_8"],
               document["ask_price_8"], document["ask_vol_8"],
               document["bid_price_9"], document["bid_vol_9"],
               document["ask_price_9"], document["ask_vol_9"]]
        writer.writerow(row)

print(f"Data has been written to {csv_file}")


