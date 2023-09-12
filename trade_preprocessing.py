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
cursor = collection.find({"data_type": "trade"})
# cursor = collection.find({})

# # 遍历游标并访问文档
# for document in cursor:
#     print(document)

# 指定CSV文件名
csv_file = 'btc_trade.csv'

# 打开CSV文件并创建CSV写入器
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)

    # 写入CSV文件头部
    header = ["exchange_ts", "receive_ts", "aggro_side", "price", "size"]  # 替换为字段名称
    writer.writerow(header)

    # 遍历游标并写入数据
    for document in cursor:
        # 替换字段名称以匹配文档的键
        row = [document["exchange_ts"], document["receive_ts"], document["aggro_side"], document["price"], document["size"]]
        writer.writerow(row)

print(f"Data has been written to {csv_file}")


