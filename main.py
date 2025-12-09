# 在文件最开头应用兼容性补丁
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

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加当前目录到Python路径
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
            self.current_data = None

            # 检查系统状态
            self.check_system_status()
        except Exception as e:
            logger.error(f"❌ 系统初始化失败: {e}")
            print(f"❌ 系统初始化失败: {e}")
            traceback.print_exc()
            sys.exit(1)

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

    def create_interface(self):
        """创建Gradio界面"""
        with gr.Blocks(title="智能数据分析系统", theme=gr.themes.Soft(),
                       css="""
                       .gradio-container {max-width: 1200px !important}
                       .success {color: green; font-weight: bold;}
                       .error {color: red; font-weight: bold;}
                       .warning {color: orange; font-weight: bold;}
                       """) as demo:
            gr.Markdown("""
            # 🚀 智能数据分析系统
            **基于本地Ollama大模型的数据分析平台**

            *支持自然语言查询、数据可视化、AI深度分析*
            """)

            # 系统状态显示
            with gr.Row():
                status_info = gr.Markdown("### 📊 系统状态: 运行中")

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
                            load_btn = gr.Button("📥 加载数据", variant="primary")

                        gr.Markdown("### 表结构")
                        schema_output = gr.JSON(label="", show_label=False)

                    with gr.Column(scale=2):
                        gr.Markdown("### 数据预览")
                        data_info = gr.Textbox(label="数据信息", interactive=False)
                        data_table = gr.Dataframe(
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
                        result_info = gr.Textbox(label="结果信息", interactive=False)
                        query_result = gr.Dataframe(
                            label="",
                            interactive=False,
                            height=400,
                            wrap=True
                        )

            with gr.Tab("📈 数据分析"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 图表设置")
                        chart_type = gr.Dropdown(
                            choices=[
                                ('折线图', 'line'),
                                ('柱状图', 'bar'),
                                ('散点图', 'scatter'),
                                ('直方图', 'histogram'),
                                ('箱线图', 'box'),
                                ('热力图', 'heatmap')
                            ],
                            label="图表类型",
                            value='bar'
                        )
                        with gr.Row():
                            x_axis = gr.Dropdown(label="X轴字段", interactive=True)
                            y_axis = gr.Dropdown(label="Y轴字段", interactive=True)
                        group_by = gr.Dropdown(label="分组字段", interactive=True, allow_custom_value=True)
                        create_chart_btn = gr.Button("📊 生成图表", variant="primary")

                    with gr.Column(scale=2):
                        gr.Markdown("### 可视化结果")
                        chart_output = gr.Plot(
                            label="",
                            show_label=False
                        )

            with gr.Tab("🤖 AI分析"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### AI分析设置")
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
                outputs=[data_info, data_table, x_axis, y_axis, group_by]
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
                outputs=[result_info, query_result, x_axis, y_axis, group_by]
            )

            clear_sql_btn.click(
                fn=lambda: "",
                outputs=sql_query
            )

            create_chart_btn.click(
                fn=self.create_visualization,
                inputs=[chart_type, x_axis, y_axis, group_by],
                outputs=chart_output
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

    def load_table_data(self, table_name):
        """加载表数据"""
        if table_name:
            try:
                # 获取更多数据
                df = self.db_manager.get_table_data(table_name, 2000)  # 改为2000行

                # 获取总行数信息
                total_count = self.db_manager.get_table_count(table_name)

                self.current_data = df

                if 'error' in df.columns and len(df) == 1:
                    error_msg = df['error'].iloc[0]
                    logger.error(f"加载表数据失败: {error_msg}")
                    return f"错误: {error_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()

                columns = list(df.columns) if not df.empty else []

                # 显示更详细的信息
                if total_count > len(df):
                    info = f"表: {table_name} | 总行数: {total_count} | 预览: {len(df)} 行 | 列数: {len(columns)}"
                else:
                    info = f"表: {table_name} | 行数: {len(df)} | 列数: {len(columns)}"

                logger.info(f"成功加载表数据: {info}")
                return info, df, gr.Dropdown(choices=columns), gr.Dropdown(choices=columns), gr.Dropdown(
                    choices=columns)

            except Exception as e:
                error_msg = f"加载表数据时出错: {str(e)}"
                logger.error(error_msg)
                return error_msg, pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()
        return "请选择数据表", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()

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

    def execute_custom_query(self, sql_query):
        """执行自定义SQL查询"""
        if not sql_query.strip():
            return "请输入SQL查询", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()

        try:
            # 验证SQL查询
            is_valid, validation_msg = self.llm_analyst.validate_sql_query(sql_query)
            if not is_valid:
                return f"SQL验证失败: {validation_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()

            df = self.db_manager.execute_query(sql_query)
            self.current_data = df

            if not df.empty and 'error' not in df.columns:
                columns = list(df.columns)
                info = f"查询成功 | 行数: {len(df)} | 列数: {len(columns)}"
                logger.info(f"SQL查询执行成功: {info}")
                return info, df, gr.Dropdown(choices=columns), gr.Dropdown(choices=columns), gr.Dropdown(
                    choices=columns)
            else:
                error_msg = df['error'].iloc[0] if 'error' in df.columns else "查询执行成功但无数据返回"
                logger.warning(f"SQL查询执行问题: {error_msg}")
                return f"注意: {error_msg}", pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()
        except Exception as e:
            error_msg = f"执行查询时出错: {str(e)}"
            logger.error(error_msg)
            return error_msg, pd.DataFrame(), gr.Dropdown(), gr.Dropdown(), gr.Dropdown()

    def create_visualization(self, chart_type, x_col, y_col, group_col):
        """创建可视化图表"""
        if self.current_data is None or self.current_data.empty:
            return self.data_analyzer._create_error_plot("请先加载数据或执行查询")

        if not x_col:
            return self.data_analyzer._create_error_plot("请选择X轴字段")

        logger.info(f"创建可视化图表: {chart_type}, X: {x_col}, Y: {y_col}, 分组: {group_col}")
        return self.data_analyzer.create_visualization(
            self.current_data, chart_type, x_col, y_col,
            group_col if group_col else None
        )

    def perform_ai_analysis(self, analysis_prompt):
        """执行AI分析"""
        if self.current_data is None or self.current_data.empty:
            return "**❌ 请先加载数据或执行查询**"

        if not analysis_prompt.strip():
            return "**❌ 请输入分析需求**"

        try:
            # 生成数据描述
            data_description = f"""
            **数据概况:**
            - 数据形状: {self.current_data.shape}
            - 列名: {list(self.current_data.columns)}

            **前5行数据预览:**
            ```
            {self.current_data.head().to_string()}
            ```
            """

            # 添加基本统计信息
            numeric_cols = self.current_data.select_dtypes(include=[np.number])
            if not numeric_cols.empty:
                data_description += f"\n**基本统计信息:**\n```\n{numeric_cols.describe().to_string()}\n```"

            logger.info(f"执行AI分析: {analysis_prompt}")
            insights = self.llm_analyst.analyze_data_insights(analysis_prompt, data_description)
            return f"## 🤖 AI分析结果\n\n{insights}"

        except Exception as e:
            error_msg = f"**❌ 分析过程中出错:** {str(e)}"
            logger.error(error_msg)
            return error_msg


def main():
    """主函数"""
    print("=" * 50)
    print("🚀 智能数据分析系统启动中...")
    print("=" * 50)

    try:
        # 创建系统实例
        system = DataAnalysisSystem()

        # 创建界面
        demo = system.create_interface()

        # 启动服务
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