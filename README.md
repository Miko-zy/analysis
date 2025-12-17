# 智能数据分析系统 Demo

这是一个基于阿里百炼大模型API的数据分析系统Demo，换用了阿里百炼模型替代Ollama,实现了超过 /AItrymcptool版本 的功能(基本功能全部实现），并可通过语音指令调用，未实现操控页面。

## 功能特性

- 📊 数据库连接与管理
- 🔍 自然语言转SQL查询
- 📈 数据可视化
- 🤖 AI数据分析与洞察
- 🎙️ 语音指令支持
- ⚙️ MCP服务集成

## 文件结构
demo/ ├── analysis.py # 数据分析模块 ├── config.py # 配置文件 ├── database.py # 数据库管理模块 ├── llm_integration.py # 大语言模型集成模块 ├── mcp_service.py # MCP服务实现 ├── sql_validator.py # SQL验证模块 ├── mcp_config.json # MCP配置文件 ├── .env # 环境变量配置 └── pyproject.toml # 项目依赖配置


## 环境要求

- Python 3.11+
- MySQL数据库
- 阿里百炼API Key

## 安装依赖

```bash
pip install -e .
或直接安装依赖包：

bash
pip install fastmcp pandas numpy sqlalchemy dashscope python-dotenv PyMySQL
配置说明
在 .env 文件中配置以下环境变量：

env
# 数据库配置
DB_DIALECT=mysql
DB_DRIVER=pymysql
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database

# 阿里百炼API配置
DASHSCOPE_API_KEY=your_api_key
DASHSCOPE_MODEL=qwen-plus
使用方法
启动MCP服务
bash
python mcp_service.py
通过MCP配置文件启动
bash
# 确保mcp_config.json配置正确
fastmcp run
核心功能
1. 数据库操作
列出所有数据表
获取表结构信息
预览表数据
2. 自然语言查询
将自然语言转换为SQL查询语句
执行SQL查询并返回结果
3. 数据分析
生成数据统计摘要
创建可视化图表规范
4. AI洞察
使用大模型分析数据并提供业务洞察
MCP工具接口
该系统提供以下MCP工具接口：

list_database_tables - 列出数据库中的所有表
get_table_preview - 获取表的数据预览
get_table_structure - 获取表结构信息
natural_language_to_sql - 将自然语言转换为SQL
execute_sql_and_get_results - 执行SQL并获取结果
create_data_visualization - 创建数据可视化
analyze_data_with_insights - 使用AI分析数据并提供洞察
get_data_statistics - 获取数据统计信息
开发说明
该项目遵循与 /AItrymcptool 相似的架构设计，便于统一管理和扩展。通过MCP协议，可以方便地与其他系统集成。