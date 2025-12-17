try:
    import compatibility_patch

    print("✅ 兼容性补丁已应用")
except ImportError:
    print("⚠️  兼容性补丁未找到，继续运行...")

import gradio as gr
import pandas as pd
import numpy as np
import sys
import os
import traceback
import logging
import re
from typing import List, Dict, Any, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from database import DatabaseManager
    from llm_integration import LLMAnalyst
    from analysis import DataAnalyzer
    from config import SYSTEM_CONFIG
except ImportError as e:
    logger.error(f"❌ 导入错误: {e}")
    print(f"❌ 导入错误: {e}")
    print("请确保以下文件存在: database.py, llm_integration.py, analysis.py, config.py")
    sys.exit(1)


class DataAnalysisSystem:
    def __init__(self):
        logger.info("🚀 初始化数据分析系统...")
        try:
            self.db_manager = DatabaseManager()
            self.llm_analyst = LLMAnalyst()
            self.data_analyzer = DataAnalyzer()

            self.table_data = None
            self.current_table_name = None
            self.query_result_data = None
            self.is_query_result = False

            self.check_system_status()
        except Exception as e:
            logger.error(f"❌ 系统初始化失败: {e}")
            print(f"❌ 系统初始化失败: {e}")
            traceback.print_exc()
            sys.exit(1)

    def create_interface(self):
        """创建Gradio界面"""
        with gr.Blocks(title="智能数据分析系统", theme=gr.themes.Soft(),
                       css="""
                       .gradio-container {max-width: 1200px !important}
                       .success {color: green; font-weight: bold;}
                       .error {color: red; font-weight: bold;}
                       .warning {color: orange; font-weight: bold;}
                       .chart-explanation {
                           background-color: #f8f9fa;
                           padding: 15px;
                           border-radius: 8px;
                           border-left: 4px solid #4CAF50;
                           margin-top: 10px;
                           font-size: 14px;
                       }
                       .data-source-info {
                           background-color: #e3f2fd;
                           padding: 10px;
                           border-radius: 5px;
                           margin-bottom: 10px;
                           font-size: 13px;
                       }
                       .preset-btn {
                           padding: 8px 12px;
                           margin: 2px;
                           border-radius: 6px;
                           font-size: 12px;
                       }
                       """) as demo:
            gr.Markdown("""
            # 🚀 智能数据分析系统
            **基于本地Ollama大模型的数据分析平台**

            *支持自然语言查询、数据可视化、AI深度分析*
            """)

            # 数据源状态显示
            with gr.Row():
                data_source_info = gr.Markdown(
                    "### 📊 当前数据源: 未选择",
                    elem_classes="data-source-info"
                )

            with gr.Tab("📊 数据浏览"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 数据选择")
                        table_dropdown = gr.Dropdown(
                            choices=self.db_manager.get_tables(),
                            label="选择数据表",
                            value=self.db_manager.get_tables()[0] if self.db_manager.get_tables() else None
                        )
                        with gr.Row():
                            refresh_btn = gr.Button("🔄 刷新")
                            load_btn = gr.Button("📥 加载表数据", variant="primary")

                        gr.Markdown("### 表结构")
                        schema_output = gr.JSON(label="", show_label=False)

                        gr.Markdown("### 📋 数据智能分析")
                        data_summary_btn = gr.Button("🔍 分析数据结构", variant="secondary")
                        data_summary_output = gr.JSON(label="数据摘要", show_label=False)

                    with gr.Column(scale=2):
                        gr.Markdown("### 表数据预览")
                        table_data_info = gr.Textbox(label="表数据信息", interactive=False)
                        table_data_display = gr.Dataframe(
                            label="数据表",
                            interactive=False,
                            height=400,
                            wrap=True
                        )

            with gr.Tab("🔍 智能查询"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 自然语言查询")
                        nl_query = gr.Textbox(
                            label="用中文描述你的查询需求",
                            placeholder="例如：查询销售额最高的5个产品，按地区分组显示",
                            lines=3
                        )
                        with gr.Row():
                            gen_sql_btn = gr.Button("🎯 生成SQL", variant="primary")
                            clear_nl_btn = gr.Button("🗑️ 清空")

                        gr.Markdown("### SQL查询")
                        sql_query = gr.Textbox(
                            label="SQL查询语句",
                            placeholder="生成的SQL语句将显示在这里...",
                            lines=4
                        )
                        with gr.Row():
                            exec_sql_btn = gr.Button("⚡ 执行查询", variant="primary")
                            clear_sql_btn = gr.Button("🗑️ 清空SQL")

                    with gr.Column(scale=2):
                        gr.Markdown("### 查询结果")
                        query_result_info = gr.Textbox(label="查询结果信息", interactive=False)
                        query_result_display = gr.Dataframe(
                            label="",
                            interactive=False,
                            height=400,
                            wrap=True
                        )

            with gr.Tab("📈 数据分析"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 图表设置")

                        chart_type_info = gr.Markdown("### 📊 选择图表类型")
                        available_charts = self.data_analyzer.get_available_chart_types()
                        chart_choices = [(f"{chart['label']}", chart['value']) for chart in available_charts]

                        chart_type = gr.Dropdown(
                            choices=chart_choices,
                            label="图表类型",
                            value='bar',
                            info="选择最适合您数据的图表类型"
                        )

                        chart_logic = gr.Markdown("", elem_classes="chart-explanation")

                        with gr.Row():
                            x_axis = gr.Dropdown(
                                label="X轴字段",
                                interactive=True,
                                info="通常为分类、时间或自变量"
                            )
                            y_axis = gr.Dropdown(
                                label="Y轴字段",
                                interactive=True,
                                info="通常为数值指标或因变量"
                            )

                        with gr.Row():
                            smart_recommend_btn = gr.Button("🤖 智能推荐字段", variant="secondary", size="sm")
                            validate_btn = gr.Button("🔍 验证字段选择", variant="secondary", size="sm")

                        group_by = gr.Dropdown(
                            label="分组字段（可选）",
                            interactive=True,
                            allow_custom_value=True,
                            info="按此字段分组显示数据"
                        )

                        validation_output = gr.Markdown("", elem_classes="chart-explanation")

                        create_chart_btn = gr.Button("📊 生成图表", variant="primary")

                    with gr.Column(scale=2):
                        gr.Markdown("### 可视化结果")
                        chart_output = gr.Plot(
                            label="",
                            show_label=False
                        )

                        chart_explanation = gr.Markdown("", elem_classes="chart-explanation")

            with gr.Tab("🤖 AI分析"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### AI分析设置")

                        gr.Markdown("#### 📋 预设问题（一键填充）")
                        with gr.Row():
                            preset_btn1 = gr.Button("📊 数据概况分析", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                            preset_btn2 = gr.Button("📈 趋势分析", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                        with gr.Row():
                            preset_btn3 = gr.Button("🔍 异常值检测", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                            preset_btn4 = gr.Button("🔗 相关性分析", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                        with gr.Row():
                            preset_btn5 = gr.Button("🎯 业务洞察", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                            preset_btn6 = gr.Button("📋 数据质量检查", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                        with gr.Row():
                            preset_btn7 = gr.Button("💰 财务分析", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")
                            preset_btn8 = gr.Button("👥 客户分析", variant="secondary", size="sm",
                                                    elem_classes="preset-btn")

                        analysis_prompt = gr.Textbox(
                            label="分析需求描述",
                            placeholder="例如：分析销售数据的季节性趋势，找出表现最好的产品和地区，提供业务建议",
                            lines=4
                        )
                        with gr.Row():
                            analyze_btn = gr.Button("🧠 开始分析", variant="primary")
                            clear_analysis_btn = gr.Button("🗑️ 清空")

                    with gr.Column(scale=2):
                        gr.Markdown("### AI分析结果")
                        analysis_output = gr.Markdown(
                            label="",
                            show_label=False
                        )

            # 事件处理
            refresh_btn.click(
                fn=self.refresh_tables,
                outputs=table_dropdown
            )

            table_dropdown.change(
                fn=self.show_table_schema,
                inputs=table_dropdown,
                outputs=schema_output
            )

            load_btn.click(
                fn=self.load_table_data,
                inputs=[table_dropdown],
                outputs=[table_data_info, table_data_display, x_axis, y_axis, group_by, data_source_info]
            )

            data_summary_btn.click(
                fn=self.generate_data_summary,
                inputs=[table_dropdown],
                outputs=data_summary_output
            )

            chart_type.change(
                fn=self.update_chart_logic,
                inputs=chart_type,
                outputs=chart_logic
            )

            smart_recommend_btn.click(
                fn=self.smart_recommend_fields,
                inputs=[chart_type],
                outputs=[x_axis, y_axis, validation_output]
            )

            validate_btn.click(
                fn=self.validate_chart_fields,
                inputs=[chart_type, x_axis, y_axis],
                outputs=validation_output
            )

            gen_sql_btn.click(
                fn=self.generate_sql_from_nl,
                inputs=[nl_query, table_dropdown],
                outputs=sql_query
            )

            clear_nl_btn.click(
                fn=lambda: "",
                outputs=nl_query
            )

            exec_sql_btn.click(
                fn=self.execute_custom_query,
                inputs=sql_query,
                outputs=[query_result_info, query_result_display, x_axis, y_axis, group_by, data_source_info]
            )

            clear_sql_btn.click(
                fn=lambda: "",
                outputs=sql_query
            )

            create_chart_btn.click(
                fn=self.create_visualization,
                inputs=[chart_type, x_axis, y_axis, group_by],
                outputs=[chart_output, chart_explanation]
            )

            analyze_btn.click(
                fn=self.perform_ai_analysis,
                inputs=analysis_prompt,
                outputs=analysis_output
            )

            clear_analysis_btn.click(
                fn=lambda: "",
                outputs=analysis_output
            )

            # 预设按钮事件处理
            preset_btn1.click(
                fn=lambda: "请分析数据的基本情况，包括数据分布、缺失值、异常值、主要趋势等。",
                outputs=analysis_prompt
            )

            preset_btn2.click(
                fn=lambda: "请分析数据的时间趋势，包括季节性变化、增长趋势、周期性规律等。",
                outputs=analysis_prompt
            )

            preset_btn3.click(
                fn=lambda: "请检测数据中的异常值，识别潜在的数据质量问题，分析异常值的原因和影响。",
                outputs=analysis_prompt
            )

            preset_btn4.click(
                fn=lambda: "请分析各变量之间的相关性，找出强相关和弱相关的变量，提供关联性洞察。",
                outputs=analysis_prompt
            )

            preset_btn5.click(
                fn=lambda: "从业务角度分析数据，提供可行的商业建议和行动计划，识别增长机会。",
                outputs=analysis_prompt
            )

            preset_btn6.click(
                fn=lambda: "检查数据的完整性、一致性、准确性，评估数据质量并提供改进建议。",
                outputs=analysis_prompt
            )

            preset_btn7.click(
                fn=lambda: "分析财务数据，包括收入、成本、利润、投资回报率等，提供财务洞察。",
                outputs=analysis_prompt
            )

            preset_btn8.click(
                fn=lambda: "分析客户数据，包括客户细分、行为模式、满意度、流失率等客户洞察。",
                outputs=analysis_prompt
            )

        return demo

    def refresh_tables(self):
        """刷新表列表"""
        try:
            tables = self.db_manager.get_tables()
            logger.info(f"刷新表列表，发现 {len(tables)} 个表")
            return gr.Dropdown(choices=tables, value=tables[0] if tables else None)
        except Exception as e:
            logger.error(f"刷新表列表失败: {e}")
            return gr.Dropdown(choices=[], value=None)

    def show_table_schema(self, table_name):
        """显示表结构"""
        if table_name:
            schema = self.db_manager.get_table_schema(table_name)
            logger.info(f"显示表结构: {table_name}, 列数: {len(schema)}")
            return schema
        return {}

    def generate_data_summary(self, table_name):
        """生成数据摘要"""
        if not table_name:
            return {"error": "请先选择数据表"}

        try:
            df = self.db_manager.get_table_data(table_name, 100)
            if df.empty or ('error' in df.columns and len(df) == 1):
                return {"error": "无法加载数据"}

            summary = self.data_analyzer.get_data_summary(df)
            return summary
        except Exception as e:
            logger.error(f"生成数据摘要失败: {e}")
            return {"error": f"生成摘要失败: {str(e)}"}

    def load_table_data(self, table_name):
        """加载表数据"""
        if table_name:
            try:
                df = self.db_manager.get_table_data(table_name, 100)
                total_count = self.db_manager.get_table_count(table_name)

                self.table_data = df
                self.current_table_name = table_name
                self.query_result_data = None
                self.is_query_result = False

                if 'error' in df.columns and len(df) == 1:
                    error_msg = df['error'].iloc[0]
                    logger.error(f"加载表数据失败: {error_msg}")
                    return f"错误: {error_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

                columns = list(df.columns) if not df.empty else []

                if total_count > len(df):
                    info = f"表: {table_name} | 总行数: {total_count} | 预览: {len(df)} 行 | 列数: {len(columns)}"
                else:
                    info = f"表: {table_name} | 行数: {len(df)} | 列数: {len(columns)}"

                logger.info(f"成功加载表数据: {info}")
                return info, df, gr.Dropdown(choices=columns), gr.Dropdown(choices=columns), gr.Dropdown(
                    choices=columns), self.update_data_source_display()

            except Exception as e:
                error_msg = f"加载表数据时出错: {str(e)}"
                logger.error(error_msg)
                return error_msg, pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()
        return "请选择数据表", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

    def update_chart_logic(self, chart_type_value):
        """更新图表逻辑说明"""
        if '|' in str(chart_type_value):
            chart_type = chart_type_value.split('|')[-1]
        else:
            chart_type = chart_type_value

        available_charts = self.data_analyzer.get_available_chart_types()

        for chart in available_charts:
            if chart['value'] == chart_type:
                return f"### 📝 {chart['title']}\n\n**用途**: {chart['description']}\n\n**逻辑**: {chart['logic']}"

        return "### 📝 图表说明\n\n选择图表类型后，将显示详细说明"

    def smart_recommend_fields(self, chart_type_value):
        """智能推荐字段"""
        current_data = self.get_current_data_for_analysis()
        if current_data is None or current_data.empty:
            return gr.Dropdown(), gr.Dropdown(), "❌ 请先加载数据或执行查询"

        if '|' in str(chart_type_value):
            chart_type = chart_type_value.split('|')[-1]
        else:
            chart_type = chart_type_value

        try:
            recommendations = self.data_analyzer.get_smart_field_recommendations(current_data, chart_type)

            x_choices = recommendations.get('x_axis', [])
            y_choices = recommendations.get('y_axis', [])

            x_value = x_choices[0] if x_choices else None
            y_value = y_choices[0] if y_choices else None

            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            explanation = f"### 🤖 智能推荐 ({data_source})\n\n"
            explanation += f"**图表类型**: {chart_type}\n\n"
            explanation += f"**推荐X轴**: {x_value or '无合适字段'}\n"
            explanation += f"**推荐Y轴**: {y_value or '无合适字段'}\n\n"
            explanation += f"**说明**: 系统根据字段类型和数据特征智能推荐"

            return gr.Dropdown(choices=x_choices, value=x_value), \
                gr.Dropdown(choices=y_choices, value=y_value), \
                explanation

        except Exception as e:
            logger.error(f"智能推荐字段失败: {e}")
            return gr.Dropdown(), gr.Dropdown(), f"❌ 推荐失败: {str(e)}"

    def validate_chart_fields(self, chart_type_value, x_col, y_col):
        """验证图表字段选择"""
        current_data = self.get_current_data_for_analysis()
        if current_data is None or current_data.empty:
            return "❌ 请先加载数据或执行查询"

        if not x_col:
            return "❌ 请选择X轴字段"

        if '|' in str(chart_type_value):
            chart_type = chart_type_value.split('|')[-1]
        else:
            chart_type = chart_type_value

        try:
            validation = self.data_analyzer.validate_chart_fields(current_data, chart_type, x_col, y_col)

            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            result = f"### 🔍 字段验证结果 ({data_source})\n\n"

            if validation['is_valid']:
                result += "✅ **字段选择合理**\n\n"
            else:
                result += "⚠️ **字段选择需要调整**\n\n"

            if validation['warnings']:
                result += "**警告**:\n"
                for warning in validation['warnings']:
                    result += f"• {warning}\n"
                result += "\n"

            if validation['suggestions']:
                result += "**建议**:\n"
                for suggestion in validation['suggestions']:
                    result += f"• {suggestion}\n"
                result += "\n"

            if validation['recommended_x'] or validation['recommended_y']:
                result += "**推荐调整**:\n"
                if validation['recommended_x']:
                    result += f"• X轴: {validation['recommended_x']}\n"
                if validation['recommended_y']:
                    result += f"• Y轴: {validation['recommended_y']}\n"
                result += "\n"

            logic = self.data_analyzer.get_chart_logic_explanation(current_data, chart_type, x_col, y_col)
            if logic:
                result += "**图表逻辑**:\n" + logic

            return result

        except Exception as e:
            logger.error(f"验证字段失败: {e}")
            return f"❌ 验证失败: {str(e)}"

    def generate_sql_from_nl(self, nl_query, current_table):
        """从自然语言生成SQL"""
        if not nl_query.strip():
            return "请输入查询问题"

        try:
            tables = self.db_manager.get_tables()
            table_schemas = {}
            for table in tables:
                table_schemas[table] = self.db_manager.get_table_schema(table)

            logger.info(f"为自然语言查询生成SQL: {nl_query}")
            return self.llm_analyst.generate_sql_query(nl_query, table_schemas)
        except Exception as e:
            error_msg = f"生成SQL时出错: {str(e)}"
            logger.error(error_msg)
            return error_msg

    def execute_custom_query(self, sql_query: str):
        """执行自定义SQL查询"""
        if not sql_query.strip():
            return "请输入SQL查询", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

        try:
            # 验证SQL查询
            is_valid, validation_msg = self.llm_analyst.validate_sql_query(sql_query)
            if not is_valid:
                return f"SQL验证失败: {validation_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

            # 执行查询
            df = self.db_manager.execute_query(sql_query)

            # 检查是否有错误
            if not df.empty and 'error' in df.columns and len(df) == 1:
                error_msg = df['error'].iloc[0]
                logger.error(f"SQL执行错误: {error_msg}")

                # 智能错误处理
                fixed_sql = self._handle_sql_error(sql_query, error_msg)

                if fixed_sql != sql_query:
                    logger.info(f"执行修正后的SQL: {fixed_sql}")
                    df_fixed = self.db_manager.execute_query(fixed_sql)

                    if not df_fixed.empty and 'error' not in df_fixed.columns:
                        self.query_result_data = df_fixed
                        self.is_query_result = True

                        columns = list(df_fixed.columns)
                        info = f"查询成功 (自动修正) | 行数: {len(df_fixed)} | 列数: {len(columns)}"
                        return info, df_fixed, gr.Dropdown(choices=columns), gr.Dropdown(
                            choices=columns), gr.Dropdown(choices=columns), self.update_data_source_display()
                    else:
                        return f"自动修正后仍失败: {df_fixed['error'].iloc[0] if not df_fixed.empty and 'error' in df_fixed.columns else '未知错误'}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()
                else:
                    return f"SQL执行错误: {error_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

            self.query_result_data = df
            self.is_query_result = True

            if not df.empty and 'error' not in df.columns:
                columns = list(df.columns)
                info = f"查询成功 | 行数: {len(df)} | 列数: {len(columns)}"
                logger.info(f"SQL查询执行成功: {info}")
                return info, df, gr.Dropdown(choices=columns), gr.Dropdown(choices=columns), gr.Dropdown(
                    choices=columns), self.update_data_source_display()
            else:
                if df.empty:
                    info = "查询执行成功但无数据返回"
                else:
                    info = "查询结果可能包含错误"
                logger.warning(f"SQL查询执行情况: {info}")
                return info, df, gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()
        except Exception as e:
            error_msg = f"执行查询时出错: {str(e)}"
            logger.error(error_msg)
            return error_msg, pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown(), self.update_data_source_display()

    def _handle_sql_error(self, sql_query: str, error_msg: str) -> str:
        """智能处理SQL错误"""
        tables = self.db_manager.get_tables()
        table_schemas = {}
        for table in tables:
            table_schemas[table] = self.db_manager.get_table_schema(table)

        # 如果是列名错误，尝试自动修正
        if "Unknown column" in error_msg:
            logger.info("尝试自动修正列名错误")
            try:
                from sql_validator import SQLValidator
                validator = SQLValidator()

                table_names = validator.extract_tables_from_sql(sql_query)

                for table_name in table_names:
                    if table_name in table_schemas:
                        available_columns = [col['name'] for col in table_schemas[table_name]]
                        fixed_sql, corrections = validator.fix_column_names(sql_query, available_columns, table_name)

                        if corrections:
                            logger.info(f"自动修正了列名: {corrections}")
                            return fixed_sql
            except Exception as e:
                logger.error(f"自动修正列名失败: {e}")

        # 如果是表名错误，尝试使用第一个表
        if "Table" in error_msg and "doesn't exist" in error_msg:
            logger.info("尝试修正表名错误")
            if tables:
                first_table = tables[0]
                fixed_sql = re.sub(r'`?(\w+)`?', f'`{first_table}`', sql_query, count=1)
                return fixed_sql

        # 其他情况使用大模型改进
        try:
            improved_sql = self.llm_analyst.improve_sql_with_feedback(
                sql_query, error_msg, table_schemas
            )
            return improved_sql
        except Exception as e:
            logger.error(f"大模型改进失败: {e}")
            return sql_query

    def get_current_data_for_analysis(self):
        """获取用于分析的数据"""
        if self.is_query_result and self.query_result_data is not None:
            logger.info(f"使用查询结果数据进行图表分析，行数: {len(self.query_result_data)}")
            return self.query_result_data
        elif self.table_data is not None:
            logger.info(f"使用表数据 ({self.current_table_name}) 进行图表分析，行数: {len(self.table_data)}")
            return self.table_data
        else:
            logger.warning("没有可用数据进行分析")
            return None

    def create_visualization(self, chart_type_value, x_col, y_col, group_col):
        """创建可视化图表"""
        current_data = self.get_current_data_for_analysis()

        if current_data is None or current_data.empty:
            return self.data_analyzer._create_error_plot("请先加载数据或执行查询"), ""

        if not x_col:
            return self.data_analyzer._create_error_plot("请选择X轴字段"), ""

        if '|' in str(chart_type_value):
            chart_type = chart_type_value.split('|')[-1]
        else:
            chart_type = chart_type_value

        data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
        logger.info(f"创建可视化图表: {chart_type}, 数据来源: {data_source}, X: {x_col}, Y: {y_col}, 分组: {group_col}")

        chart = self.data_analyzer.create_visualization(
            current_data, chart_type, x_col, y_col,
            group_col if group_col else None
        )

        explanation = self.data_analyzer.get_chart_logic_explanation(
            current_data, chart_type, x_col, y_col
        )

        if explanation:
            explanation = f"### 📊 图表解读 ({data_source})\n\n{explanation}"
        else:
            explanation = f"### 📊 图表解读 ({data_source})\n\n图表生成成功，但无法生成详细解读。"

        return chart, explanation

    def perform_ai_analysis(self, analysis_prompt):
        """执行AI分析"""
        current_data = self.get_current_data_for_analysis()

        if current_data is None or current_data.empty:
            return "**❌ 请先加载数据或执行查询**"

        if not analysis_prompt.strip():
            return "**❌ 请输入分析需求**"

        try:
            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            data_description = f"""
            **数据来源:** {data_source}
            **数据概况:**
            - 数据形状: {current_data.shape}
            - 列名: {list(current_data.columns)}

            **前5行数据预览:**
            ```
            {current_data.head().to_string()}
            ```
            """

            numeric_cols = current_data.select_dtypes(include=[np.number])
            if not numeric_cols.empty:
                data_description += f"\n**基本统计信息:**\n```\n{numeric_cols.describe().to_string()}\n```"

            logger.info(f"执行AI分析: {analysis_prompt}")
            insights = self.llm_analyst.analyze_data_insights(analysis_prompt, data_description)
            return f"## 🤖 AI分析结果 ({data_source})\n\n{insights}"

        except Exception as e:
            error_msg = f"**❌ 分析过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg

    def check_system_status(self):
        """检查系统状态"""
        logger.info("🔍 检查系统组件...")

        # 检查数据库
        try:
            success, message = self.db_manager.test_connection()
            if success:
                tables = self.db_manager.get_tables()
                logger.info(f"✅ 数据库连接成功: {message}")
                print(f"✅ 数据库连接成功: {message}")
                print(f"📊 发现表: {tables}")
            else:
                logger.error(f"❌ 数据库连接失败: {message}")
                print(f"❌ 数据库连接失败: {message}")
        except Exception as e:
            logger.error(f"❌ 数据库检查失败: {e}")
            print(f"❌ 数据库检查失败: {e}")

        # 检查Ollama
        try:
            status, models = self.llm_analyst.check_ollama_connection()
            if status:
                logger.info(f"✅ Ollama连接成功，可用模型: {models}")
                print(f"✅ Ollama连接成功，可用模型: {models}")
            else:
                logger.warning(f"⚠️  Ollama连接失败: {models}")
                print(f"⚠️  Ollama连接失败: {models}")
        except Exception as e:
            logger.error(f"❌ Ollama检查失败: {e}")
            print(f"❌ Ollama检查失败: {e}")

    def update_data_source_display(self):
        """更新数据来源显示"""
        if self.is_query_result:
            if self.query_result_data is not None:
                source_text = f"### 📊 当前数据源: 查询结果 ({len(self.query_result_data)} 行, {len(self.query_result_data.columns)} 列)"
            else:
                source_text = "### 📊 当前数据源: 查询结果"
        elif self.current_table_name:
            if self.table_data is not None:
                total_count = self.db_manager.get_table_count(self.current_table_name)
                source_text = f"### 📊 当前数据源: 表: {self.current_table_name} ({total_count} 行, {len(self.table_data.columns)} 列)"
            else:
                source_text = f"### 📊 当前数据源: 表: {self.current_table_name}"
        else:
            source_text = "### 📊 当前数据源: 未选择"

        return source_text


def main():
    """主函数"""
    print("=" * 50)
    print("🚀 智能数据分析系统启动中...")
    print("=" * 50)

    try:
        system = DataAnalysisSystem()
        demo = system.create_interface()

        print("\n✅ 系统启动成功！")
        print("🌐 访问地址: http://localhost:7860")
        print("⏹️  按 Ctrl+C 停止服务\n")

        demo.launch(
            server_name="127.0.0.1",
            server_port=7860,
            share=False,
            show_error=True,
            inbrowser=True
        )

    except KeyboardInterrupt:
        print("\n👋 系统已停止")
    except Exception as e:
        print(f"\n❌ 系统启动失败: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()