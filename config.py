import os
from dotenv import load_dotenv

load_dotenv()

# 数据库配置 - MySQL
DATABASE_CONFIG = {
    'dialect': os.getenv('DB_DIALECT', 'mysql'),
    'driver': os.getenv('DB_DRIVER', 'pymysql'),
    'username': os.getenv('DB_USERNAME', 'root'),
    'password': os.getenv('DB_PASSWORD', '123456'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '3306'),
    'database': os.getenv('DB_NAME', 'example_an')
}

# 阿里百炼API配置
DASHSCOPE_CONFIG = {
    'api_key': os.getenv('DASHSCOPE_API_KEY', 'sk-9688b2480fd943c0b3f8f7022536e78d'),
    'model': os.getenv('DASHSCOPE_MODEL', 'qwen-plus'),
}

# 系统配置
SYSTEM_CONFIG = {
    'max_rows': int(os.getenv('MAX_ROWS', '10000')),
    'cache_timeout': int(os.getenv('CACHE_TIMEOUT', '3600')),
    'debug': os.getenv('DEBUG', 'False').lower() == 'true'
}