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
    'database': os.getenv('DB_NAME', 'demo')
}

# Ollama配置
OLLAMA_CONFIG = {
    'base_url': os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434'),
    'model': os.getenv('OLLAMA_MODEL', 'llama3.2:3b'),
    'timeout': int(os.getenv('OLLAMA_TIMEOUT', '120'))
}

# 系统配置
SYSTEM_CONFIG = {
    'max_rows': int(os.getenv('MAX_ROWS', '10000')),
    'cache_timeout': int(os.getenv('CACHE_TIMEOUT', '3600')),
    'debug': os.getenv('DEBUG', 'False').lower() == 'true'
}