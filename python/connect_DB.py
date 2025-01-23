import os
from configparser import ConfigParser

import psycopg2

# 動態取得 config.ini 的完整路徑
base_path = os.path.dirname(os.getcwd())  # 找到當前工作目錄的父目錄
config_path = os.path.join(base_path, "pm", "config.ini")
config = ConfigParser()
config.read(config_path)

def official_DB_connection():
    db_config = {
        'user': config.get('official', 'user'),
        'password': config.get('official', 'password'),
        'host': config.get('official', 'host'),
        'port': config.get('official', 'port'),
        'database': config.get('official', 'database')
    }
    return psycopg2.connect(**db_config)

def test_DB_connection():
    db_config = {
        'user': config.get('testing', 'user'),
        'password': config.get('testing', 'password'),
        'host': config.get('testing', 'host'),
        'port': config.get('testing', 'port'),
        'database': config.get('testing', 'database')
    }
    return psycopg2.connect(**db_config)


















