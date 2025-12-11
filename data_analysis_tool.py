# data_analysis_tool.py
from fastmcp import FastMCP
import sys
import logging
import pandas as pd
import numpy as np

# Fix UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stderr.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')

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
    """列出数据库中的所有表名，用于数据浏览功能"""
    try:
        tables = db_manager.get_tables()
        return {"success": True, "tables": tables}
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_table_preview(table_name: str) -> dict:
    """获取指定表的数据预览，显示前10行数据"""
    try:
        df = db_manager.get_table_data(table_name, limit=10)

        if not df.empty and 'error' not in df.columns:
            preview_data = {
                "table_name": table_name,
                "data": df.to_dict('records'),
                "columns": list(df.columns),
                "row_count": len(df)
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
    """获取指定表的结构信息，包括字段名和数据类型"""
    try:
        schema = db_manager.get_table_schema(table_name)
        if schema:
            return {"success": True, "structure": schema}
        else:
            return {"success": False, "error": f"未能获取表 '{table_name}' 的结构信息"}
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def natural_language_to_sql(nl_query: str) -> dict:
    """将自然语言查询转换为SQL语句"""
    try:
        tables = db_manager.get_tables()
        table_schemas = {}
        for table in tables:
            table_schemas[table] = db_manager.get_table_schema(table)

        sql_query = llm_analyst.generate_sql_query(nl_query, table_schemas)
        return {"success": True, "sql_query": sql_query}
    except Exception as e:
        logger.error(f"生成SQL失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def execute_sql_and_get_results(sql_query: str) -> dict:
    """执行SQL查询并返回结果"""
    try:
        is_valid, validation_msg = llm_analyst.validate_sql_query(sql_query)
        if not is_valid:
            return {"success": False, "error": f"SQL验证失败: {validation_msg}"}

        df = db_manager.execute_query(sql_query)

        if not df.empty and 'error' not in df.columns:
            # 限制返回的行数以避免超出MCP限制
            limited_df = df.head(100)

            result = {
                "success": True,
                "data": limited_df.to_dict('records'),
                "columns": list(limited_df.columns),
                "row_count": len(df)
            }
            return result
        else:
            error_msg = df['error'].iloc[0] if 'error' in df.columns else "查询执行成功但无数据返回"
            return {"success": False, "error": error_msg}
    except Exception as e:
        logger.error(f"执行SQL查询失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_data_visualization(chart_data: dict, chart_type: str, x_col: str, y_col: str = None,
                              group_col: str = None) -> dict:
    """创建数据可视化图表"""
    try:
        df = pd.DataFrame(chart_data)
        fig = data_analyzer.create_visualization(df, chart_type, x_col, y_col, group_col)

        return {
            "success": True,
            "message": f"成功创建{chart_type}图表",
            "chart_info": {
                "type": chart_type,
                "x_col": x_col,
                "y_col": y_col,
                "group_col": group_col
            }
        }
    except Exception as e:
        logger.error(f"创建可视化失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def analyze_data_with_ai(analysis_prompt: str, data_description: dict) -> dict:
    """使用AI分析数据并提供洞察"""
    try:
        insights = llm_analyst.analyze_data_insights(analysis_prompt, str(data_description))
        return {"success": True, "insights": insights}
    except Exception as e:
        logger.error(f"AI分析失败: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="stdio")