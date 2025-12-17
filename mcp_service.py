# mcp_service.py
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
logger = logging.getLogger('DataAnalysisMCP')

# 创建MCP服务器
mcp = FastMCP("智能数据分析MCP服务")

# 初始化系统组件
db_manager = DatabaseManager()
data_analyzer = DataAnalyzer()
llm_analyst = LLMAnalyst()


@mcp.tool()
def list_database_tables() -> dict:
    """列出数据库中所有可用的数据表"""
    try:
        tables = db_manager.get_tables()
        return {"success": True, "tables": tables}
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_table_preview(table_name: str, limit: int = 50) -> dict:
    """获取指定表的数据预览，默认显示50行"""
    try:
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
        schema = db_manager.get_table_schema(table_name)
        return {"success": True, "structure": schema}
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return {"success": False, "error": str(e)}


@mcp.tool()
def natural_language_to_sql(nl_query: str) -> dict:
    """将自然语言查询转换为SQL语句"""
    try:
        # 获取所有表及其结构
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
        # 验证SQL查询
        is_valid, validation_msg = llm_analyst.validate_sql_query(sql_query)
        if not is_valid:
            return {"success": False, "error": f"SQL验证失败: {validation_msg}"}

        df = db_manager.execute_query(sql_query)

        if not df.empty and 'error' not in df.columns:
            # 限制返回的行数以避免超出MCP限制
            limited_df = df.head(100)  # 限制最多返回100行

            result = {
                "success": True,
                "data": limited_df.to_dict('records'),
                "columns": list(limited_df.columns),
                "total_rows": len(df),
                "returned_rows": len(limited_df)
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
    """创建数据可视化图表"""
    try:
        # 从字典重建DataFrame
        df = pd.DataFrame(data['data']) if 'data' in data else pd.DataFrame(data)

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
        if not data_sample or ('data' in data_sample and not data_sample['data']):
            return {"success": False, "error": "提供的数据为空"}

        # 从字典重建DataFrame
        df = pd.DataFrame(data_sample['data']) if 'data' in data_sample else pd.DataFrame(data_sample)

        # 生成数据描述
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
        if not data or ('data' in data and not data['data']):
            return {"success": False, "error": "提供的数据为空"}

        # 从字典重建DataFrame
        df = pd.DataFrame(data['data']) if 'data' in data else pd.DataFrame(data)

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