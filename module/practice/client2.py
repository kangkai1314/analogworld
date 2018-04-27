#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Janice Cheng

import socket
ip_port = ('127.0.0.1',9999) # 这是一个元组

# 买手机
s = socket.socket()

# 利用创建出来的对象来绑定: 买手机卡
# 因为这个对象是已经封装好 TCP 协议的
s.bind(ip_port)

# 开机
s.listen(5) # 最大接受挂线的数目

# 等待电话
# conn 服务端跟客户端连接的通讯
conn, addr = s.accept() # 每次听电话只能跟一个人通信中、然后另外条线会挂着

while True:
    # 收消息
    recv_data = conn.recv(1024)
    print("--------",type(recv_data))
    if str(recv_data, encoding='utf8') == 'exit':
        break

    # 发消息
    send_data = recv_data.upper()
    print(send_data)
    conn.send(send_data)

# 挂电话
conn.close()

