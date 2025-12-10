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
def get_database_tables() -> dict:
    """获取数据库中所有表的名称"""
    try:
        tables = db_manager.get_tables()
        return {"success": True, "tables": tables}
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_table_schema(table_name: str) -> dict:
    """获取指定表的结构信息"""
    try:
        schema = db_manager.get_table_schema(table_name)
        return {"success": True, "schema": schema}
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def execute_sql_query(sql_query: str) -> dict:
    """执行SQL查询并返回结果"""
    try:
        # 验证SQL查询
        is_valid, validation_msg = llm_analyst.validate_sql_query(sql_query)
        if not is_valid:
            return {"success": False, "error": f"SQL验证失败: {validation_msg}"}

        df = db_manager.execute_query(sql_query)

        if not df.empty and 'error' not in df.columns:
            # 将DataFrame转换为字典格式
            result = {
                "success": True,
                "data": df.to_dict('records'),
                "columns": list(df.columns),
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
def generate_sql_from_natural_language(nl_query: str) -> dict:
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
def create_visualization(chart_data: dict, chart_type: str, x_col: str, y_col: str = None,
                         group_col: str = None) -> dict:
    """创建数据可视化图表"""
    try:
        # 从字典重建DataFrame
        df = pd.DataFrame(chart_data)

        # 创建可视化
        fig = data_analyzer.create_visualization(df, chart_type, x_col, y_col, group_col)

        # 返回成功消息（实际图表将在前端渲染）
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


@mcp.tool()
def get_data_summary_statistics(data: dict) -> dict:
    """生成数据摘要统计信息"""
    try:
        df = pd.DataFrame(data)
        summary = data_analyzer.generate_summary_statistics(df)
        return {"success": True, "summary": summary}
    except Exception as e:
        logger.error(f"生成摘要统计失败: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="stdio")