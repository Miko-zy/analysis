import requests
import json
import re
import time
import logging
from config import OLLAMA_CONFIG

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LLMAnalyst:
    def __init__(self):
        self.base_url = OLLAMA_CONFIG['base_url']
        self.model = OLLAMA_CONFIG['model']
        self.timeout = OLLAMA_CONFIG['timeout']
        self.max_retries = 3
        self.retry_delay = 2

    def _call_ollama(self, prompt, system_prompt=None, temperature=0.1):
        """调用Ollama API，支持重试机制"""
        for attempt in range(self.max_retries):
            try:
                # 构建消息列表
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})

                payload = {
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "top_p": 0.9,
                        "top_k": 40,
                        "num_predict": 4000  # 限制输出长度
                    }
                }

                # 发送请求到Ollama
                response = requests.post(
                    f"{self.base_url}/api/chat",
                    json=payload,
                    timeout=self.timeout
                )

                # 检查响应状态
                if response.status_code != 200:
                    error_msg = f"Ollama API错误: 状态码 {response.status_code}, 响应: {response.text}"
                    logger.error(error_msg)
                    if attempt == self.max_retries - 1:
                        return error_msg
                    time.sleep(self.retry_delay)
                    continue

                result = response.json()

                # 检查响应结构
                if 'message' in result and 'content' in result['message']:
                    content = result['message']['content'].strip()
                    logger.info(f"Ollama响应成功，长度: {len(content)} 字符")
                    return content
                else:
                    error_msg = f"响应格式错误: {result}"
                    logger.error(error_msg)
                    return error_msg

            except requests.exceptions.ConnectionError:
                error_msg = f"无法连接到Ollama服务，请确保Ollama正在运行在 {self.base_url}"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

            except requests.exceptions.Timeout:
                error_msg = "请求超时，请检查Ollama服务状态"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

            except requests.exceptions.RequestException as e:
                error_msg = f"网络请求错误: {str(e)}"
                logger.error(error_msg)
                if attempt == self.max_retries - 1:
                    return error_msg
                time.sleep(self.retry_delay)

            except KeyError as e:
                error_msg = f"响应解析错误: 缺少键 {str(e)}"
                logger.error(error_msg)
                return error_msg

            except Exception as e:
                error_msg = f"未知错误: {str(e)}"
                logger.error(error_msg)
                return error_msg

        return "所有重试尝试都失败了"

    def generate_sql_query(self, natural_language_query, table_schemas):
        """根据自然语言生成SQL查询"""
        if not natural_language_query.strip():
            return "请输入查询问题"

        try:
            # 构建详细的schema信息
            schema_info = ""
            for table, schema in table_schemas.items():
                columns_info = []
                for col in schema:
                    col_info = f"{col['name']} ({col['type']})"
                    if col.get('comment'):
                        col_info += f" - {col['comment']}"
                    columns_info.append(col_info)
                schema_info += f"表: {table}\n列: {', '.join(columns_info)}\n\n"

            # 获取当前数据库类型
            from config import DATABASE_CONFIG
            db_dialect = DATABASE_CONFIG['dialect']

            prompt = f"""
    你是一个专业的SQL专家。请根据下面的数据库表结构和用户的问题，生成合适的SQL查询语句。

    数据库表结构:
    {schema_info}

    用户问题: {natural_language_query}

    要求:
    1. 只返回SQL查询语句，不要有其他解释或markdown格式
    2. 使用标准的SQL语法，适用于{db_dialect}数据库
    3. 如果涉及多个表，请使用JOIN
    4. 如果用户的问题不明确，做出合理的假设
    5. 确保查询语法正确
    6. 优先使用现有的表和字段
    7. 如果问题涉及统计，使用合适的聚合函数
    8. 注意：表名和列名可能包含中文，请使用反引号包裹

    请直接生成SQL查询语句:
    """

            system_prompt = """你是一个专业的SQL查询生成器。请严格遵守以下规则：
    1. 只返回SQL语句，不包含任何解释、注释或额外文本
    2. 使用标准的SQL语法
    3. 确保查询能够正确执行
    4. 如果问题不明确，选择最合理的表进行查询
    5. 不要使用不存在的表或字段
    6. 对于中文表名和列名，使用反引号包裹"""

            logger.info(f"正在为问题生成SQL: {natural_language_query}")
            sql_query = self._call_ollama(prompt, system_prompt, temperature=0.1)

            # 检查是否返回错误信息
            if any(error in sql_query for error in
                   ["无法连接", "请求超时", "网络请求错误", "Ollama API错误", "响应格式错误", "未知错误",
                    "所有重试尝试都失败了"]):
                return sql_query

            # 清理响应，提取SQL语句
            sql_query = sql_query.strip()

            # 移除可能的代码块标记
            sql_query = re.sub(r'```sql|```', '', sql_query).strip()

            # 提取SQL语句（从SELECT开始到分号或结尾）
            sql_match = re.search(r'(SELECT.*?)(?=;|$)', sql_query, re.IGNORECASE | re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1).strip()

            # 确保以SELECT开头
            if not sql_query.upper().startswith('SELECT'):
                # 尝试找到SELECT语句
                select_match = re.search(r'SELECT.*', sql_query, re.IGNORECASE | re.DOTALL)
                if select_match:
                    sql_query = select_match.group(0).strip()
                else:
                    # 如果找不到SELECT，返回原始响应但添加说明
                    sql_query = f"-- 生成的查询可能需要调整:\n{sql_query}"

            logger.info(f"生成的SQL: {sql_query}")
            return sql_query

        except Exception as e:
            error_msg = f"生成SQL时出现异常: {str(e)}"
            logger.error(error_msg)
            return error_msg

    def analyze_data_insights(self, analysis_prompt, data_description):
        """使用大模型分析数据洞察"""
        if not analysis_prompt.strip():
            return "请输入分析需求"

        if not data_description.strip():
            return "数据描述为空"

        prompt = f"""
你是一个专业的数据分析师。请分析以下数据并提供深入的业务洞察：

分析需求: {analysis_prompt}

数据描述: 
{data_description}

请用中文提供以下分析：
1. 主要发现和趋势（3-4个关键点）
2. 数据中的异常值或有趣模式
3. 业务建议和行动计划
4. 进一步分析的建议

请用清晰的结构化格式回答，使用适当的标题和项目符号。
确保分析基于提供的数据，不要虚构不存在的信息。
"""

        system_prompt = """你是一个专业的数据分析师，擅长从数据中发现洞察并提供实用的业务建议。
请用中文回答，结构清晰，内容实用，基于实际数据进行分析。"""

        logger.info("正在进行AI数据分析...")
        insights = self._call_ollama(prompt, system_prompt, temperature=0.7)
        return insights

    def check_ollama_connection(self):
        """检查Ollama连接和模型可用性"""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'models' in data:
                    models = data['models']
                    available_models = [model['name'] for model in models]
                    logger.info(f"Ollama连接成功，可用模型: {available_models}")
                    return True, available_models
                else:
                    error_msg = "API响应格式异常"
                    logger.error(error_msg)
                    return False, [error_msg]
            else:
                error_msg = f"HTTP错误: {response.status_code}"
                logger.error(error_msg)
                return False, [error_msg]
        except requests.exceptions.ConnectionError:
            error_msg = "无法连接到Ollama服务"
            logger.error(error_msg)
            return False, [error_msg]
        except requests.exceptions.Timeout:
            error_msg = "连接超时"
            logger.error(error_msg)
            return False, [error_msg]
        except Exception as e:
            error_msg = f"连接错误: {str(e)}"
            logger.error(error_msg)
            return False, [error_msg]

    def validate_sql_query(self, sql_query):
        """验证SQL查询的合理性（基础验证）"""
        if not sql_query or not isinstance(sql_query, str):
            return False, "SQL查询不能为空"

        sql_upper = sql_query.upper().strip()

        # 基础安全检查
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for keyword in dangerous_keywords:
            if keyword in sql_upper and f' {keyword} ' in f' {sql_upper} ':
                return False, f"检测到不允许的操作: {keyword}"

        # 必须是以SELECT开头
        if not sql_upper.startswith('SELECT'):
            return False, "只支持SELECT查询"

        return True, "SQL查询格式正确"