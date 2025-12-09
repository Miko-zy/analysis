import pandas as pd
import numpy as np
import warnings
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 在导入任何其他库之前应用兼容性补丁
try:
    import compatibility_patch
except ImportError:
    logger.warning("兼容性补丁未找到，继续运行...")

# 设置环境变量以避免NumPy兼容性问题
import os

os.environ['NPY_PROMOTION_STATE'] = 'weak'

# 尝试导入plotly，如果失败则使用matplotlib作为备选
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    PLOTLY_AVAILABLE = True
    logger.info("Plotly导入成功")
except ImportError as e:
    logger.warning(f"Plotly不可用: {e}，将使用matplotlib作为备选")
    PLOTLY_AVAILABLE = False
    try:
        import matplotlib.pyplot as plt
        import matplotlib

        matplotlib.use('Agg')  # 使用非交互式后端
        MATPLOTLIB_AVAILABLE = True
        logger.info("Matplotlib导入成功")
    except ImportError:
        logger.warning("Matplotlib也不可用，可视化功能将完全禁用")
        MATPLOTLIB_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import LabelEncoder

    SKLEARN_AVAILABLE = True
    logger.info("scikit-learn导入成功")
except ImportError as e:
    logger.warning(f"scikit-learn不可用: {e}，部分分析功能将受限")
    SKLEARN_AVAILABLE = False


class DataAnalyzer:
    def __init__(self):
        if SKLEARN_AVAILABLE:
            self.label_encoder = LabelEncoder()
        else:
            self.label_encoder = None
        logger.info("数据分析器初始化完成")

    def generate_summary_statistics(self, df):
        """生成数据摘要统计"""
        if df.empty:
            logger.warning("尝试为空DataFrame生成摘要统计")
            return {"error": "数据为空"}

        try:
            summary = {
                'shape': df.shape,
                'columns': list(df.columns),
                'data_types': df.dtypes.astype(str).to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'duplicate_rows': df.duplicated().sum()
            }

            # 数值列统计
            numeric_cols = df.select_dtypes(include=[np.number])
            if not numeric_cols.empty:
                summary['numeric_stats'] = numeric_cols.describe().to_dict()
                # 避免在相关性计算中的兼容性问题
                try:
                    summary['correlation_matrix'] = numeric_cols.corr().to_dict()
                except Exception as e:
                    logger.warning(f"计算相关性矩阵时出错: {e}")
                    summary['correlation_matrix'] = {}

            # 分类列统计
            categorical_cols = df.select_dtypes(include=['object', 'category'])
            if not categorical_cols.empty:
                categorical_stats = {}
                for col in categorical_cols.columns:
                    try:
                        value_counts = df[col].value_counts()
                        categorical_stats[col] = {
                            'unique_count': len(value_counts),
                            'top_values': value_counts.head(5).to_dict()
                        }
                    except Exception as e:
                        logger.warning(f"处理分类列 {col} 时出错: {e}")
                        categorical_stats[col] = {'error': str(e)}
                summary['categorical_stats'] = categorical_stats

            logger.info(f"生成摘要统计，数据形状: {df.shape}")
            return summary

        except Exception as e:
            error_msg = f"生成摘要统计时出错: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}

    def create_visualization(self, df, chart_type, x_col, y_col=None, group_col=None):
        """创建可视化图表"""
        if not PLOTLY_AVAILABLE and not MATPLOTLIB_AVAILABLE:
            return self._create_error_plot("可视化库不可用，请安装plotly或matplotlib")

        try:
            if df.empty:
                return self._create_error_plot("数据为空")

            if x_col not in df.columns:
                return self._create_error_plot(f"列 '{x_col}' 不存在")

            # 预处理数据：处理缺失值
            plot_df = df.copy()
            if y_col and y_col in plot_df.columns:
                plot_df = plot_df.dropna(subset=[x_col, y_col])
            else:
                plot_df = plot_df.dropna(subset=[x_col])

            if plot_df.empty:
                return self._create_error_plot("数据清洗后无有效数据")

            # 使用plotly（如果可用）
            if PLOTLY_AVAILABLE:
                return self._create_plotly_visualization(plot_df, chart_type, x_col, y_col, group_col)
            # 备选：使用matplotlib
            elif MATPLOTLIB_AVAILABLE:
                return self._create_matplotlib_visualization(plot_df, chart_type, x_col, y_col, group_col)
            else:
                return self._create_error_plot("没有可用的可视化库")

        except Exception as e:
            error_msg = f"图表生成错误: {str(e)}"
            logger.error(error_msg)
            return self._create_error_plot(error_msg)

    def _create_plotly_visualization(self, df, chart_type, x_col, y_col, group_col):
        """使用Plotly创建可视化"""
        try:
            fig = None

            if chart_type == 'line':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("折线图需要Y轴字段")
                fig = px.line(df, x=x_col, y=y_col, color=group_col,
                              title=f'{y_col} 随 {x_col} 变化趋势')

            elif chart_type == 'bar':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("柱状图需要Y轴字段")
                fig = px.bar(df, x=x_col, y=y_col, color=group_col,
                             title=f'{y_col} 按 {x_col} 分布')

            elif chart_type == 'scatter':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("散点图需要Y轴字段")
                fig = px.scatter(df, x=x_col, y=y_col, color=group_col,
                                 title=f'{y_col} 与 {x_col} 关系')

            elif chart_type == 'histogram':
                fig = px.histogram(df, x=x_col, color=group_col,
                                   title=f'{x_col} 分布直方图')

            elif chart_type == 'box':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("箱线图需要Y轴字段")
                fig = px.box(df, x=group_col if group_col else x_col, y=y_col,
                             title=f'{y_col} 箱线图')

            elif chart_type == 'heatmap':
                numeric_df = df.select_dtypes(include=[np.number])
                if numeric_df.empty:
                    return self._create_error_plot("没有数值列用于热力图")
                if len(numeric_df.columns) < 2:
                    return self._create_error_plot("需要至少两个数值列生成热力图")
                try:
                    corr_matrix = numeric_df.corr()
                    fig = px.imshow(corr_matrix,
                                    title='数值列相关性热力图',
                                    color_continuous_scale='RdBu_r',
                                    aspect="auto")
                except Exception as e:
                    return self._create_error_plot(f"生成热力图时出错: {str(e)}")
            else:
                return self._create_error_plot(f"不支持的图表类型: {chart_type}")

            # 更新布局
            if fig:
                fig.update_layout(
                    font=dict(size=12),
                    showlegend=True,
                    height=500,
                    margin=dict(l=50, r=50, t=50, b=50)
                )
                logger.info(f"成功生成 {chart_type} 图表 (Plotly)")
                return fig
            else:
                return self._create_error_plot("图表生成失败")

        except Exception as e:
            error_msg = f"Plotly图表生成错误: {str(e)}"
            logger.error(error_msg)
            # 尝试使用matplotlib作为备选
            if MATPLOTLIB_AVAILABLE:
                logger.info("尝试使用Matplotlib作为备选")
                return self._create_matplotlib_visualization(df, chart_type, x_col, y_col, group_col)
            return self._create_error_plot(error_msg)

    def _create_matplotlib_visualization(self, df, chart_type, x_col, y_col, group_col):
        """使用Matplotlib创建可视化（备选方案）"""
        try:
            plt.figure(figsize=(10, 6))

            if chart_type == 'line':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("折线图需要Y轴字段")
                if group_col and group_col in df.columns:
                    for group in df[group_col].unique():
                        group_data = df[df[group_col] == group]
                        plt.plot(group_data[x_col], group_data[y_col], label=str(group))
                    plt.legend()
                else:
                    plt.plot(df[x_col], df[y_col])
                plt.title(f'{y_col} 随 {x_col} 变化趋势')

            elif chart_type == 'bar':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("柱状图需要Y轴字段")
                if group_col and group_col in df.columns:
                    # 分组柱状图
                    pivot_df = df.pivot_table(values=y_col, index=x_col, columns=group_col, aggfunc='mean')
                    pivot_df.plot(kind='bar')
                else:
                    plt.bar(df[x_col], df[y_col])
                plt.title(f'{y_col} 按 {x_col} 分布')
                plt.xticks(rotation=45)

            elif chart_type == 'scatter':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("散点图需要Y轴字段")
                plt.scatter(df[x_col], df[y_col])
                plt.title(f'{y_col} 与 {x_col} 关系')

            elif chart_type == 'histogram':
                plt.hist(df[x_col].dropna(), bins=20)
                plt.title(f'{x_col} 分布直方图')

            elif chart_type == 'box':
                if not y_col or y_col not in df.columns:
                    return self._create_error_plot("箱线图需要Y轴字段")
                if group_col and group_col in df.columns:
                    groups = [df[df[group_col] == group][y_col] for group in df[group_col].unique()]
                    plt.boxplot(groups, labels=[str(group) for group in df[group_col].unique()])
                else:
                    plt.boxplot(df[y_col])
                plt.title(f'{y_col} 箱线图')

            elif chart_type == 'heatmap':
                numeric_df = df.select_dtypes(include=[np.number])
                if numeric_df.empty:
                    return self._create_error_plot("没有数值列用于热力图")
                corr_matrix = numeric_df.corr()
                plt.imshow(corr_matrix, cmap='RdBu_r', aspect='auto')
                plt.colorbar()
                plt.xticks(range(len(corr_matrix.columns)), corr_matrix.columns, rotation=45)
                plt.yticks(range(len(corr_matrix.columns)), corr_matrix.columns)
                plt.title('数值列相关性热力图')

            else:
                return self._create_error_plot(f"不支持的图表类型: {chart_type}")

            plt.tight_layout()
            logger.info(f"成功生成 {chart_type} 图表 (Matplotlib)")
            return plt.gcf()

        except Exception as e:
            error_msg = f"Matplotlib图表生成错误: {str(e)}"
            logger.error(error_msg)
            return self._create_error_plot(error_msg)

    def perform_regression_analysis(self, df, x_cols, y_col):
        """执行回归分析"""
        if not SKLEARN_AVAILABLE:
            return {'error': 'scikit-learn不可用，无法执行回归分析'}

        try:
            if df.empty:
                return {'error': '数据为空'}

            # 检查列是否存在
            missing_cols = [col for col in x_cols + [y_col] if col not in df.columns]
            if missing_cols:
                return {'error': f'列不存在: {missing_cols}'}

            # 处理分类变量
            df_encoded = df.copy()
            for col in x_cols:
                if df[col].dtype == 'object':
                    if self.label_encoder is None:
                        return {'error': 'LabelEncoder不可用'}
                    try:
                        df_encoded[col] = self.label_encoder.fit_transform(df[col].astype(str))
                    except Exception as e:
                        return {'error': f'编码列 {col} 时出错: {str(e)}'}

            X = df_encoded[x_cols]
            y = df_encoded[y_col]

            # 移除缺失值
            mask = ~(X.isnull().any(axis=1) | y.isnull())
            X = X[mask]
            y = y[mask]

            if len(X) == 0:
                return {'error': '没有有效数据用于回归分析'}

            if len(X) < len(x_cols) + 1:
                return {'error': '样本数量不足，无法进行回归分析'}

            model = LinearRegression()
            model.fit(X, y)

            results = {
                'coefficients': dict(zip(x_cols, model.coef_)),
                'intercept': model.intercept_,
                'r_squared': model.score(X, y),
                'sample_size': len(X),
                'feature_names': x_cols,
                'target_name': y_col
            }

            logger.info(f"回归分析完成，R²: {results['r_squared']:.4f}")
            return results

        except Exception as e:
            error_msg = f"回归分析错误: {str(e)}"
            logger.error(error_msg)
            return {'error': error_msg}

    def _create_error_plot(self, message):
        """创建错误提示图表"""
        if PLOTLY_AVAILABLE:
            try:
                fig = go.Figure()
                fig.add_annotation(
                    text=message,
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False,
                    font=dict(size=16, color="red")
                )
                fig.update_layout(
                    title="图表生成错误",
                    xaxis=dict(visible=False),
                    yaxis=dict(visible=False),
                    plot_bgcolor='white',
                    height=400
                )
                return fig
            except Exception:
                pass
        elif MATPLOTLIB_AVAILABLE:
            try:
                plt.figure(figsize=(8, 4))
                plt.text(0.5, 0.5, message, ha='center', va='center',
                         transform=plt.gca().transAxes, color='red', fontsize=12)
                plt.axis('off')
                return plt.gcf()
            except Exception:
                pass

        # 如果所有可视化方法都失败，返回文本消息
        return f"错误: {message}"

    def detect_outliers(self, df, column):
        """检测数值列的异常值"""
        if column not in df.columns:
            return {'error': f'列 {column} 不存在'}

        if not pd.api.types.is_numeric_dtype(df[column]):
            return {'error': f'列 {column} 不是数值类型'}

        try:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR

            outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]

            return {
                'outlier_count': len(outliers),
                'total_count': len(df),
                'outlier_percentage': len(outliers) / len(df) * 100,
                'bounds': {'lower': lower_bound, 'upper': upper_bound},
                'outliers': outliers[column].tolist()
            }
        except Exception as e:
            return {'error': f'检测异常值时出错: {str(e)}'}