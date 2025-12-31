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
                       .gradio-container {max-width: 1400px !important}
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
                       /* 分析等级选择器样式 */
                       .analysis-level-selector {
                           background-color: #f0f7ff;
                           padding: 15px;
                           border-radius: 8px;
                           margin-bottom: 15px;
                           border-left: 4px solid #1890ff;
                       }
                       .level-btn {
                           margin: 5px;
                           border-radius: 20px;
                       }
                       .level-basic { 
                           background-color: #e6f7ff !important; 
                           border-color: #91d5ff !important;
                       }
                       .level-standard { 
                           background-color: #bae7ff !important;
                           border-color: #69c0ff !important;
                       }
                       .level-advanced { 
                           background-color: #91d5ff !important;
                           border-color: #40a9ff !important;
                       }
                       .level-expert { 
                           background-color: #69c0ff !important;
                           border-color: #1890ff !important;
                       }
                       .dimension-tag {
                           display: inline-block;
                           background-color: #f0f0f0;
                           padding: 4px 8px;
                           margin: 2px;
                           border-radius: 4px;
                           font-size: 12px;
                       }
                       .analysis-result-tabs {
                           margin-top: 20px;
                       }
                       .analysis-progress {
                           background-color: #f0f7ff;
                           padding: 10px;
                           border-radius: 5px;
                           margin-bottom: 10px;
                           border-left: 4px solid #52c41a;
                       }
                       .executive-summary {
                           background-color: #f6ffed;
                           padding: 15px;
                           border-radius: 8px;
                           border-left: 4px solid #52c41a;
                           margin-top: 10px;
                       }
                       .key-metrics {
                           background-color: #fff7e6;
                           padding: 10px;
                           border-radius: 5px;
                           margin: 5px 0;
                       }
                       .action-plan {
                           background-color: #f9f0ff;
                           padding: 10px;
                           border-radius: 5px;
                           margin: 5px 0;
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

                        # 分析等级选择
                        gr.Markdown("#### 📊 分析深度等级")
                        with gr.Row():
                            analysis_level = gr.Radio(
                                choices=[
                                    ("基础分析 (快速概览)", "basic"),
                                    ("标准分析 (推荐)", "standard"),
                                    ("深度分析 (详细洞察)", "advanced"),
                                    ("专家级分析 (全面研究)", "expert")
                                ],
                                value="standard",
                                label="选择分析深度",
                                elem_classes="analysis-level-selector"
                            )

                        # 多维度分析选项
                        gr.Markdown("#### 🎯 多维度分析")
                        with gr.Row():
                            dimension_time = gr.Checkbox(label="时间维度", value=True)
                            dimension_geo = gr.Checkbox(label="地理维度", value=True)
                        with gr.Row():
                            dimension_product = gr.Checkbox(label="产品维度", value=True)
                            dimension_customer = gr.Checkbox(label="客户维度", value=True)
                        with gr.Row():
                            dimension_channel = gr.Checkbox(label="渠道维度", value=True)
                            dimension_custom = gr.Checkbox(label="自定义维度", value=False)

                        custom_dimension = gr.Textbox(
                            label="自定义维度（逗号分隔）",
                            placeholder="例如：年龄段,收入水平,教育程度",
                            visible=False
                        )

                        # 预设问题更新
                        gr.Markdown("#### 📋 预设分析模板")
                        with gr.Row():
                            preset_btn1 = gr.Button("📊 数据概览分析", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-basic"])
                            preset_btn2 = gr.Button("📈 趋势深度分析", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-advanced"])
                        with gr.Row():
                            preset_btn3 = gr.Button("🔍 异常值深度检测", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-advanced"])
                            preset_btn4 = gr.Button("🔗 多维度关联分析", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-expert"])
                        with gr.Row():
                            preset_btn5 = gr.Button("🎯 商业智能洞察", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-standard"])
                            preset_btn6 = gr.Button("📋 数据质量全面评估", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-expert"])
                        with gr.Row():
                            preset_btn7 = gr.Button("💰 财务深度分析", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-advanced"])
                            preset_btn8 = gr.Button("👥 客户细分分析", variant="secondary", size="sm",
                                                    elem_classes=["preset-btn", "level-standard"])

                        # 新的分析类型按钮
                        with gr.Row():
                            multi_dim_btn = gr.Button("🌐 多维度综合分析", variant="primary", size="sm")
                            trend_btn = gr.Button("📈 专项趋势分析", variant="primary", size="sm")

                        gr.Markdown("#### 🎯 高级分析选项")
                        analysis_prompt = gr.Textbox(
                            label="分析需求描述",
                            placeholder="例如：分析销售数据的季节性趋势，找出表现最好的产品和地区，提供业务建议",
                            lines=4
                        )

                        with gr.Row():
                            analyze_btn = gr.Button("🧠 开始智能分析", variant="primary")
                            summary_btn = gr.Button("📋 生成执行摘要", variant="secondary", size="sm")
                            clear_analysis_btn = gr.Button("🗑️ 清空")

                        # 分析进度指示器
                        analysis_progress = gr.Markdown(
                            "",
                            elem_classes="analysis-progress",
                            visible=False
                        )

                    with gr.Column(scale=2):
                        gr.Markdown("### AI分析结果")

                        # 分析结果标签页
                        with gr.Tabs(elem_classes="analysis-result-tabs"):
                            with gr.TabItem("📋 详细分析"):
                                analysis_output = gr.Markdown(
                                    label="详细分析结果",
                                    show_label=False
                                )

                            with gr.TabItem("📊 关键指标"):
                                key_metrics_output = gr.Markdown(
                                    value="### 📊 关键指标\n\n*执行分析后，关键指标将显示在这里*",
                                    label="关键指标提取",
                                    show_label=False,
                                    elem_classes="key-metrics"
                                )

                            with gr.TabItem("🚀 行动计划"):
                                action_plan_output = gr.Markdown(
                                    value="### 🚀 行动计划\n\n*执行分析后，行动计划将显示在这里*",
                                    label="行动计划",
                                    show_label=False,
                                    elem_classes="action-plan"
                                )

                            with gr.TabItem("📈 可视化洞察"):
                                visual_insights_output = gr.Markdown(
                                    value="### 📈 可视化洞察\n\n*执行分析后，可视化建议将显示在这里*",
                                    label="可视化建议",
                                    show_label=False
                                )

                            with gr.TabItem("📋 执行摘要"):
                                executive_summary_output = gr.Markdown(
                                    value="### 📋 执行摘要\n\n*生成执行摘要后将显示在这里*",
                                    label="执行摘要",
                                    show_label=False,
                                    elem_classes="executive-summary"
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

            # 自定义维度显示/隐藏
            dimension_custom.change(
                fn=lambda x: gr.Textbox(visible=x),
                inputs=dimension_custom,
                outputs=custom_dimension
            )

            # 新的分析按钮事件
            multi_dim_btn.click(
                fn=self.perform_multidimensional_analysis,
                inputs=[dimension_time, dimension_geo, dimension_product,
                        dimension_customer, dimension_channel, dimension_custom, custom_dimension],
                outputs=analysis_output
            ).then(
                fn=lambda: gr.Markdown(visible=False),
                outputs=analysis_progress
            )

            trend_btn.click(
                fn=self.perform_trend_analysis,
                outputs=analysis_output
            ).then(
                fn=lambda: gr.Markdown(visible=False),
                outputs=analysis_progress
            )

            analyze_btn.click(
                fn=lambda: gr.Markdown(value="⏳ 正在分析数据，请稍候...", visible=True),
                outputs=analysis_progress
            ).then(
                fn=self.perform_ai_analysis_with_level,
                inputs=[analysis_prompt, analysis_level],
                outputs=analysis_output
            ).then(
                fn=lambda: gr.Markdown(visible=False),
                outputs=analysis_progress
            )

            summary_btn.click(
                fn=self.generate_executive_summary,
                inputs=analysis_output,
                outputs=executive_summary_output
            )

            clear_analysis_btn.click(
                fn=self.clear_analysis_outputs,
                outputs=[analysis_prompt, analysis_output, key_metrics_output,
                         action_plan_output, visual_insights_output, executive_summary_output]
            )

            # 更新预设按钮
            preset_btn1.click(
                fn=lambda: ("请分析数据的基本情况，包括数据分布、缺失值、主要特征等。", "basic"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn2.click(
                fn=lambda: ("请进行深度的趋势分析，包括季节性、周期性、增长趋势和预测模型。", "advanced"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn3.click(
                fn=lambda: ("请深度检测数据中的异常值，使用统计方法和机器学习技术识别异常模式。", "advanced"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn4.click(
                fn=lambda: ("请进行多维度关联分析，探索各变量间的复杂关系和交互效应。", "expert"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn5.click(
                fn=lambda: ("从商业智能角度分析数据，提供实用的业务洞察和决策支持。", "standard"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn6.click(
                fn=lambda: ("进行全面数据质量评估，包括完整性、一致性、准确性、时效性等多维度检查。", "expert"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn7.click(
                fn=lambda: ("进行深度的财务数据分析，包括盈利能力、偿债能力、运营效率等全面分析。", "advanced"),
                outputs=[analysis_prompt, analysis_level]
            )

            preset_btn8.click(
                fn=lambda: ("进行客户细分分析，包括RFM分析、客户生命周期价值、客户行为模式等。", "standard"),
                outputs=[analysis_prompt, analysis_level]
            )

            # 分析完成后更新其他标签页
            analysis_output.change(
                fn=self.extract_key_metrics,
                inputs=analysis_output,
                outputs=key_metrics_output
            ).then(
                fn=self.extract_action_plan,
                inputs=analysis_output,
                outputs=action_plan_output
            ).then(
                fn=self.extract_visual_insights,
                inputs=analysis_output,
                outputs=visual_insights_output
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
            df = self.db_manager.get_table_data(table_name, 50)
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
                df = self.db_manager.get_table_data(table_name, 50)
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

    def perform_ai_analysis_with_level(self, analysis_prompt, analysis_level):
        """执行带等级设定的AI分析"""
        current_data = self.get_current_data_for_analysis()

        if current_data is None or current_data.empty:
            return "**❌ 请先加载数据或执行查询**"

        if not analysis_prompt.strip():
            return "**❌ 请输入分析需求**"

        try:
            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            data_description = self._build_data_description(current_data, data_source)

            logger.info(f"执行{analysis_level}等级分析: {analysis_prompt}")

            # 根据等级调用不同的分析方法
            if analysis_level == "basic":
                insights = self.llm_analyst.analyze_data_insights(
                    analysis_prompt, data_description, "basic"
                )
            elif analysis_level == "advanced":
                insights = self.llm_analyst.analyze_data_insights(
                    analysis_prompt, data_description, "advanced"
                )
            elif analysis_level == "expert":
                insights = self.llm_analyst.analyze_data_insights(
                    analysis_prompt, data_description, "expert"
                )
            else:  # standard
                insights = self.llm_analyst.analyze_data_insights(
                    analysis_prompt, data_description, "standard"
                )

            return insights

        except Exception as e:
            error_msg = f"**❌ 分析过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg

    def perform_multidimensional_analysis(self, time_dim, geo_dim, product_dim,
                                          customer_dim, channel_dim, custom_dim, custom_dim_text):
        """执行多维度分析"""
        current_data = self.get_current_data_for_analysis()

        if current_data is None or current_data.empty:
            return "**❌ 请先加载数据或执行查询**"

        try:
            # 构建维度列表
            dimensions = []
            if time_dim: dimensions.append("时间")
            if geo_dim: dimensions.append("地理")
            if product_dim: dimensions.append("产品")
            if customer_dim: dimensions.append("客户")
            if channel_dim: dimensions.append("渠道")

            # 添加自定义维度
            if custom_dim and custom_dim_text.strip():
                custom_dims = [d.strip() for d in custom_dim_text.split(',') if d.strip()]
                dimensions.extend(custom_dims)

            if not dimensions:
                return "**⚠️ 请至少选择一个分析维度**"

            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            data_description = self._build_data_description(current_data, data_source)

            logger.info(f"执行多维度分析，维度: {dimensions}")

            analysis = self.llm_analyst.analyze_data_multidimensional(
                data_description, dimensions
            )

            return analysis

        except Exception as e:
            error_msg = f"**❌ 多维度分析过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg

    def perform_trend_analysis(self):
        """执行专项趋势分析"""
        current_data = self.get_current_data_for_analysis()

        if current_data is None or current_data.empty:
            return "**❌ 请先加载数据或执行查询**"

        try:
            data_source = "查询结果" if self.is_query_result else f"表: {self.current_table_name}"
            data_description = self._build_data_description(current_data, data_source)

            logger.info("执行专项趋势分析")

            # 自动检测时间列
            time_columns = []
            for col in current_data.columns:
                if any(keyword in col.lower() for keyword in ['date', 'time', 'year', 'month', 'day']):
                    time_columns.append(col)

            time_period = f"基于时间列: {', '.join(time_columns[:3])}" if time_columns else "所有时期"

            analysis = self.llm_analyst.analyze_data_trends(
                data_description, time_period
            )

            return analysis

        except Exception as e:
            error_msg = f"**❌ 趋势分析过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg

    def generate_executive_summary(self, full_analysis):
        """生成执行摘要"""
        if not full_analysis or "❌" in full_analysis or "⚠️" in full_analysis:
            return "**❌ 请先进行完整的分析再生成摘要**"

        try:
            logger.info("生成执行摘要")

            # 提取详细分析内容（去掉标题）
            content = full_analysis.split("\n\n", 1)[1] if "\n\n" in full_analysis else full_analysis

            summary = self.llm_analyst.generate_executive_summary(content)

            return summary

        except Exception as e:
            error_msg = f"**❌ 生成摘要过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg

    def extract_key_metrics(self, analysis_text):
        """从分析结果中提取关键指标"""
        if not analysis_text or "❌" in analysis_text or "⚠️" in analysis_text:
            return "### 📊 关键指标\n\n*等待分析结果...*"

        try:
            # 从分析文本中提取关键指标部分
            import re

            # 查找包含关键指标的章节
            metrics_sections = re.findall(r'(?:关键指标|核心指标|主要指标|KPI).*?(?=\n#|\n##|\Z)',
                                          analysis_text, re.IGNORECASE | re.DOTALL)

            if metrics_sections:
                return f"### 📊 关键指标\n\n{metrics_sections[0]}"
            else:
                # 如果没有找到关键指标章节，尝试提取数字和百分比
                metrics = re.findall(r'([\d.,]+%?|\d+\.\d+%?)\s*(?:增长|下降|提高|降低|占比|达到)', analysis_text)
                if metrics:
                    return f"### 📊 关键指标\n\n**提取的数值指标**:\n" + "\n".join(
                        [f"• {metric}" for metric in set(metrics[:10])])
                else:
                    return "### 📊 关键指标\n\n*分析结果中未明确标识关键指标*"

        except Exception as e:
            logger.error(f"提取关键指标失败: {e}")
            return "### 📊 关键指标\n\n*提取失败，请查看详细分析*"

    def extract_action_plan(self, analysis_text):
        """从分析结果中提取行动计划"""
        if not analysis_text or "❌" in analysis_text or "⚠️" in analysis_text:
            return "### 🚀 行动计划\n\n*等待分析结果...*"

        try:
            # 从分析文本中提取行动计划部分
            import re

            # 查找包含行动计划的章节
            action_sections = re.findall(r'(?:行动计划|行动建议|建议|下一步|措施).*?(?=\n#|\n##|\Z)',
                                         analysis_text, re.IGNORECASE | re.DOTALL)

            if action_sections:
                return f"### 🚀 行动计划\n\n{action_sections[0]}"
            else:
                # 尝试提取包含"建议"、"应该"、"需要"的句子
                suggestions = re.findall(r'[^。！？]*?(?:建议|应该|需要|建议|优先)[^。！？]*[。！？]', analysis_text)
                if suggestions:
                    return f"### 🚀 行动计划\n\n**提取的行动建议**:\n" + "\n".join(
                        [f"• {s.strip()}" for s in set(suggestions[:10])])
                else:
                    return "### 🚀 行动计划\n\n*分析结果中未明确标识行动计划*"

        except Exception as e:
            logger.error(f"提取行动计划失败: {e}")
            return "### 🚀 行动计划\n\n*提取失败，请查看详细分析*"

    def extract_visual_insights(self, analysis_text):
        """从分析结果中提取可视化洞察"""
        if not analysis_text or "❌" in analysis_text or "⚠️" in analysis_text:
            return "### 📈 可视化洞察\n\n*等待分析结果...*"

        try:
            # 从分析文本中提取可视化相关建议
            import re

            # 查找可视化相关的建议
            visual_keywords = ['图表', '可视化', '图形', '展示', '趋势图', '柱状图', '折线图', '散点图', '热力图']
            visual_sentences = []

            for sentence in re.split(r'[。！？]', analysis_text):
                if any(keyword in sentence for keyword in visual_keywords):
                    visual_sentences.append(sentence.strip())

            if visual_sentences:
                return f"### 📈 可视化洞察\n\n**可视化建议**:\n" + "\n".join(
                    [f"• {s}" for s in set(visual_sentences[:10])])
            else:
                return "### 📈 可视化洞察\n\n*分析结果中未包含具体的可视化建议*"

        except Exception as e:
            logger.error(f"提取可视化洞察失败: {e}")
            return "### 📈 可视化洞察\n\n*提取失败，请查看详细分析*"

    def clear_analysis_outputs(self):
        """清空所有分析输出"""
        return "", "", "", "", "", ""

    def _build_data_description(self, current_data, data_source):
        """构建数据描述"""
        data_description = f"""
        **数据来源:** {data_source}
        **数据概况:**
        - 数据形状: {current_data.shape[0]} 行 × {current_data.shape[1]} 列
        - 内存使用: {current_data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB
        - 列名列表: {list(current_data.columns)}

        **数据类型分布:**
        """

        # 数据类型统计
        dtypes = current_data.dtypes
        type_counts = {}
        for dtype in dtypes:
            dtype_str = str(dtype)
            if 'int' in dtype_str or 'float' in dtype_str:
                type_counts['数值型'] = type_counts.get('数值型', 0) + 1
            elif 'object' in dtype_str or 'string' in dtype_str:
                type_counts['文本型'] = type_counts.get('文本型', 0) + 1
            elif 'datetime' in dtype_str:
                type_counts['日期时间型'] = type_counts.get('日期时间型', 0) + 1
            elif 'bool' in dtype_str:
                type_counts['布尔型'] = type_counts.get('布尔型', 0) + 1
            else:
                type_counts['其他类型'] = type_counts.get('其他类型', 0) + 1

        for type_name, count in type_counts.items():
            data_description += f"  - {type_name}: {count} 列\n"

        data_description += f"""
        **缺失值情况:**
        - 总缺失值数量: {current_data.isnull().sum().sum()}
        - 缺失值比例: {current_data.isnull().mean().mean() * 100:.2f}%

        **前5行数据预览:**
        ```
        {current_data.head().to_string()}
        ```
        """

        # 数值列的统计信息
        numeric_cols = current_data.select_dtypes(include=[np.number])
        if not numeric_cols.empty:
            data_description += f"""
            **数值列基本统计信息:**
            ```
            {numeric_cols.describe().to_string()}
            ```
            """

        # 分类列的分布信息
        categorical_cols = current_data.select_dtypes(include=['object', 'category'])
        if not categorical_cols.empty and len(categorical_cols.columns) > 0:
            sample_cat_col = categorical_cols.columns[0]
            if len(categorical_cols[sample_cat_col].unique()) <= 10:
                data_description += f"""
                **分类列 '{sample_cat_col}' 的分布:**
                ```
                {categorical_cols[sample_cat_col].value_counts().to_string()}
                ```
                """

        return data_description

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
        print("📊 系统功能:")
        print("  - 数据浏览与查询")
        print("  - 智能可视化分析")
        print("  - 四级深度AI分析（基础/标准/深度/专家）")
        print("  - 多维度综合分析")
        print("  - 执行摘要生成")
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