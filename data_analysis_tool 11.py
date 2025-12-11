# data_analysis_tool.py
from fastmcp import FastMCP
import sys
import logging
import pandas as pd
import numpy as np

# 添加当前目录到Python路径
sys.path.append('.')

# 导入系统模块
from database import DatabaseManager
from analysis import DataAnalyzer
from llm_integration import LLMAnalyst

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('DataAnalysisTool')

# 创建MCP服务器
mcp = FastMCP("智能数据分析工具")

# 初始化系统组件
db_manager = DatabaseManager()
data_analyzer = DataAnalyzer()
llm_analyst = LLMAnalyst()


@mcp.tool()
def list_database_tables() -> dict:
    """列出数据库中所有可用的数据表，用于数据浏览功能的第一步"""
    try:
        tables = db_manager.get_tables()
        return {"success": True, "tables": tables}
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_table_preview(table_name: str, limit: int = 50) -> dict:
    """获取指定表的数据预览，用于查看表的前几行数据，默认显示50行"""
    try:
        # 获取表的前N行数据（默认50行，比原来10行更多）
        df = db_manager.get_table_data(table_name, limit=limit)

        if not df.empty and 'error' not in df.columns:
            preview_data = {
                "table_name": table_name,
                "data": df.to_dict('records'),
                "columns": list(df.columns),
                "row_count": len(df),
                "limit": limit
            }
            return {"success": True, "preview": preview_data}
        else:
            error_msg = df['error'].iloc[0] if 'error' in df.columns else "获取表预览失败"
            return {"success": False, "error": error_msg}
    except Exception as e:
        logger.error(f"获取表预览失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_table_structure(table_name: str) -> dict:
    """获取指定表的结构信息，包括字段名、数据类型等"""
    try:
        # 首先检查表是否存在
        tables = db_manager.get_tables()
        if table_name not in tables:
            return {"success": False, "error": f"表 '{table_name}' 不存在。可用的表: {', '.join(tables[:10])}"}

        schema = db_manager.get_table_schema(table_name)

        # 检查是否有返回结构信息
        if not schema:
            # 尝试另一种方式获取表信息
            table_info = db_manager.get_table_info(table_name)
            if 'error' in table_info:
                return {"success": False, "error": f"获取表结构失败: {table_info['error']}"}
            schema = table_info.get('schema', [])

        if schema:
            return {"success": True, "structure": schema}
        else:
            return {"success": False, "error": f"未能获取表 '{table_name}' 的结构信息"}
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def natural_language_to_sql(nl_query: str, timeout: int = 180) -> dict:
    """将自然语言查询转换为SQL语句，用于智能查询功能的第一步，增加超时时间"""
    try:
        # 检查Ollama连接
        status, models = llm_analyst.check_ollama_connection()
        if not status:
            return {"success": False, "error": f"Ollama服务连接失败: {models}"}

        # 获取所有表及其结构
        tables = db_manager.get_tables()
        if not tables:
            return {"success": False, "error": "数据库中没有找到任何表"}

        table_schemas = {}
        for table in tables[:10]:  # 限制表数量以减小提示词大小
            schema = db_manager.get_table_schema(table)
            # 限制每张表的列数以减小提示词大小
            table_schemas[table] = schema[:20] if schema else []

        # 临时增加超时时间
        original_timeout = llm_analyst.timeout
        llm_analyst.timeout = timeout

        try:
            sql_query = llm_analyst.generate_sql_query(nl_query, table_schemas)
            return {"success": True, "sql_query": sql_query}
        finally:
            # 恢复原始超时时间
            llm_analyst.timeout = original_timeout

    except Exception as e:
        logger.error(f"生成SQL失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def execute_sql_and_get_results(sql_query: str, limit: int = 200) -> dict:
    """执行SQL查询并返回结果，用于智能查询功能的第二步，限制返回行数"""
    try:
        # 验证SQL查询
        is_valid, validation_msg = llm_analyst.validate_sql_query(sql_query)
        if not is_valid:
            return {"success": False, "error": f"SQL验证失败: {validation_msg}"}

        # 在SQL查询中添加LIMIT子句以防止返回过多数据
        if "LIMIT" not in sql_query.upper():
            sql_query = f"{sql_query.rstrip(';')} LIMIT {limit}"

        df = db_manager.execute_query(sql_query)

        if not df.empty and 'error' not in df.columns:
            # 限制返回的行数以避免超出MCP限制
            limited_df = df.head(min(limit, 100))  # 最多返回100行

            result = {
                "success": True,
                "data": limited_df.to_dict('records'),
                "columns": list(limited_df.columns),
                "total_rows": len(df),
                "returned_rows": len(limited_df),
                "limit_applied": True
            }
            return result
        else:
            error_msg = df['error'].iloc[0] if 'error' in df.columns else "查询执行成功但无数据返回"
            return {"success": False, "error": error_msg}
    except Exception as e:
        logger.error(f"执行SQL查询失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_data_visualization(data: dict, chart_type: str, x_column: str, y_column: str = None,
                              group_by: str = None) -> dict:
    """创建数据可视化图表，根据提供的数据和图表配置生成图表"""
    try:
        # 检查数据是否为空
        if not data or 'data' not in data or not data['data']:
            return {"success": False, "error": "提供的数据为空"}

        # 从字典重建DataFrame
        df = pd.DataFrame(data['data'])

        # 检查必要的列是否存在
        if x_column not in df.columns:
            return {"success": False, "error": f"列 '{x_column}' 不存在于数据中"}

        if y_column and y_column not in df.columns:
            return {"success": False, "error": f"列 '{y_column}' 不存在于数据中"}

        # 限制数据行数以提高性能
        if len(df) > 1000:
            df = df.sample(1000)
            logger.info(f"数据量过大，已抽样至1000行用于可视化")

        # 创建可视化
        fig = data_analyzer.create_visualization(df, chart_type, x_column, y_column, group_by)

        # 返回成功消息（实际图表将在前端渲染）
        return {
            "success": True,
            "message": f"成功创建{chart_type}图表",
            "chart_spec": {
                "type": chart_type,
                "x_column": x_column,
                "y_column": y_column,
                "group_by": group_by
            }
        }
    except Exception as e:
        logger.error(f"创建可视化失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def analyze_data_with_insights(analysis_request: str, data_sample: dict) -> dict:
    """使用AI分析数据并提供洞察和业务建议"""
    try:
        # 检查数据是否为空
        if not data_sample or 'data' not in data_sample or not data_sample['data']:
            return {"success": False, "error": "提供的数据为空"}

        # 生成数据描述
        df = pd.DataFrame(data_sample['data'])
        data_description = f"""
        数据概况:
        - 数据形状: {df.shape}
        - 列名: {list(df.columns)}

        前5行数据预览:
        {df.head().to_string()}
        """

        # 添加基本统计信息（如果是数值型数据）
        numeric_cols = df.select_dtypes(include=[np.number])
        if not numeric_cols.empty:
            data_description += f"\n基本统计信息:\n{numeric_cols.describe().to_string()}"

        insights = llm_analyst.analyze_data_insights(analysis_request, data_description)
        return {"success": True, "insights": insights}
    except Exception as e:
        logger.error(f"AI分析失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_data_statistics(data: dict) -> dict:
    """生成数据摘要统计信息，包括数据类型、缺失值等基本信息"""
    try:
        # 检查数据是否为空
        if not data or 'data' not in data or not data['data']:
            return {"success": False, "error": "提供的数据为空"}

        df = pd.DataFrame(data['data'])
        # 限制数据行数以提高性能
        if len(df) > 5000:
            df = df.sample(5000)
            logger.info(f"数据量过大，已抽样至5000行用于统计分析")

        summary = data_analyzer.generate_summary_statistics(df)
        return {"success": True, "statistics": summary}
    except Exception as e:
        logger.error(f"生成摘要统计失败: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="stdio")