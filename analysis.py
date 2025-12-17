import pandas as pd
import numpy as np
import warnings
import logging
import traceback
from typing import Optional, Dict, List, Tuple, Any, Set
import math
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COLOR_PALETTES = {
    'categorical': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'],
    'sequential': ['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6',
                   '#4292c6', '#2171b5', '#08519c', '#08306b'],
    'diverging': ['#67001f', '#b2182b', '#d6604d', '#f4a582', '#fddbc7',
                  '#f7f7f7', '#d1e5f0', '#92c5de', '#4393c3', '#2166ac', '#053061']
}

SCIENTIFIC_CONFIG = {
    'dpi': 100,
    'font_size': 10,
    'title_size': 14,
    'label_size': 11,
    'tick_size': 9,
    'line_width': 2,
    'marker_size': 50,
    'grid_alpha': 0.3,
    'figure_ratio': 1.618,
    'max_categories': 10,
}

CHART_CONFIGS = {
    'line': {
        'title': '趋势分析图',
        'description': '展示变量随时间或其他连续变量的变化趋势',
        'requirements': {
            'x_type': ['datetime', 'numeric', 'ordinal'],
            'y_type': ['numeric'],
            'x_role': '自变量（时间/序列）',
            'y_role': '因变量（数值指标）'
        },
        'best_for': ['时间序列', '趋势分析', '周期性变化'],
        'logic': '展示Y变量如何随X变量变化，适合看趋势'
    },
    'bar': {
        'title': '比较分析图',
        'description': '用于比较不同类别之间的数值差异',
        'requirements': {
            'x_type': ['categorical', 'ordinal'],
            'y_type': ['numeric'],
            'x_role': '分类变量（维度）',
            'y_role': '度量指标（数值）'
        },
        'best_for': ['类别比较', '排名分析', '分组对比'],
        'logic': '比较不同分类下数值指标的大小'
    },
    'scatter': {
        'title': '相关分析图',
        'description': '探索两个连续变量之间的关系和相关程度',
        'requirements': {
            'x_type': ['numeric'],
            'y_type': ['numeric'],
            'x_role': '自变量（数值）',
            'y_role': '因变量（数值）'
        },
        'best_for': ['相关性分析', '异常值检测', '聚类观察'],
        'logic': '分析两个数值变量之间的相关关系'
    },
    'histogram': {
        'title': '分布分析图',
        'description': '显示单个变量的分布情况，包括中心趋势和离散程度',
        'requirements': {
            'x_type': ['numeric'],
            'y_type': 'auto',
            'x_role': '数值变量',
            'y_role': '频数/频率'
        },
        'best_for': ['分布形态', '偏度峰度', '数据范围'],
        'logic': '展示单个数值变量的分布情况'
    },
    'box': {
        'title': '统计摘要图',
        'description': '展示数据的五数概括（最小值、Q1、中位数、Q3、最大值）和异常值',
        'requirements': {
            'x_type': ['categorical'],
            'y_type': ['numeric'],
            'x_role': '分组变量（可选）',
            'y_role': '数值变量'
        },
        'best_for': ['分布比较', '异常值识别', '统计摘要'],
        'logic': '展示数值变量的分布特征，可对比不同分组'
    },
    'heatmap': {
        'title': '关联矩阵图',
        'description': '可视化变量之间的相关关系矩阵',
        'requirements': {
            'x_type': 'matrix',
            'y_type': 'matrix',
            'x_role': '数值变量集',
            'y_role': '数值变量集'
        },
        'best_for': ['相关性矩阵', '模式识别', '多变量分析'],
        'logic': '展示多个数值变量之间的相关性'
    },
    'violin': {
        'title': '密度分布图',
        'description': '结合箱线图和核密度估计，展示数据的分布密度',
        'requirements': {
            'x_type': ['categorical'],
            'y_type': ['numeric'],
            'x_role': '分组变量',
            'y_role': '数值变量'
        },
        'best_for': ['密度分布', '多组比较', '分布形态'],
        'logic': '展示不同分组下数值变量的分布密度'
    },
    'area': {
        'title': '面积图',
        'description': '展示不同类别随时间变化的趋势和比例',
        'requirements': {
            'x_type': ['datetime', 'numeric', 'ordinal'],
            'y_type': ['numeric'],
            'x_role': '时间/序列',
            'y_role': '数值指标'
        },
        'best_for': ['趋势比例', '累计变化', '占比分析'],
        'logic': '展示不同类别随时间变化的比例关系'
    }
}

PLOTLY_AVAILABLE = False
MATPLOTLIB_AVAILABLE = False

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
    logger.info("✅ Plotly导入成功")
except ImportError:
    logger.warning("❌ Plotly不可用")

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, MaxNLocator
    MATPLOTLIB_AVAILABLE = True
    logger.info("✅ Matplotlib导入成功")
except ImportError:
    logger.warning("❌ Matplotlib不可用")


class DataAnalyzer:
    def __init__(self):
        """初始化数据分析器"""
        self.current_figure = None
        self.field_analysis_cache = {}
        logger.info("🚀 智能数据分析器初始化完成")

        if not PLOTLY_AVAILABLE and not MATPLOTLIB_AVAILABLE:
            logger.warning("⚠️  没有可用的可视化库，图表功能将不可用")

    def _analyze_column(self, series: pd.Series) -> Dict[str, Any]:
        """深入分析单个字段"""
        col_name = series.name if hasattr(series, 'name') else 'unknown'

        # 基本类型检测
        if pd.api.types.is_numeric_dtype(series):
            col_type = 'numeric'
        elif pd.api.types.is_datetime64_any_dtype(series):
            col_type = 'datetime'
        elif pd.api.types.is_bool_dtype(series):
            col_type = 'boolean'
        else:
            unique_count = series.nunique()
            total_count = len(series)
            unique_ratio = unique_count / total_count if total_count > 0 else 0

            if unique_count <= 10 or unique_ratio < 0.1:
                col_type = 'categorical'
            elif unique_count <= 50:
                col_type = 'ordinal'
            else:
                col_type = 'text'

        analysis = {
            'name': col_name,
            'type': col_type,
            'dtype': str(series.dtype),
            'unique_count': series.nunique(),
            'null_count': series.isnull().sum(),
            'null_percentage': series.isnull().mean() * 100,
        }

        # 数值型字段的详细分析
        if col_type == 'numeric':
            analysis.update({
                'min': series.min(),
                'max': series.max(),
                'mean': series.mean(),
                'median': series.median(),
                'std': series.std(),
                'skewness': series.skew(),
                'kurtosis': series.kurtosis(),
                'range': series.max() - series.min(),
                'iqr': series.quantile(0.75) - series.quantile(0.25),
                'is_percentage': any(x in col_name.lower() for x in ['rate', 'ratio', 'percent', '%']),
                'is_amount': any(x in col_name.lower() for x in ['amount', 'price', 'cost', 'revenue', 'sales']),
                'is_count': any(x in col_name.lower() for x in ['count', 'number', 'quantity', 'qty']),
                'is_id': any(x in col_name.lower() for x in ['id', 'code', 'no', 'num']),
            })

            # 判断是否可能是时间序列
            if series.dropna().between(1900, 2100).all():
                analysis['potential_time'] = True
                analysis['time_unit'] = 'year'
            elif series.dropna().between(1, 12).all():
                analysis['potential_time'] = True
                analysis['time_unit'] = 'month'
            elif series.dropna().between(1, 31).all():
                analysis['potential_time'] = True
                analysis['time_unit'] = 'day'

        # 日期时间字段的详细分析
        elif col_type == 'datetime':
            analysis.update({
                'min_date': series.min(),
                'max_date': series.max(),
                'date_range': (series.max() - series.min()).days,
                'has_time_component': any(series.dropna().apply(lambda x: x.hour != 0 or x.minute != 0)),
            })

        # 分类字段的详细分析
        elif col_type in ['categorical', 'ordinal']:
            value_counts = series.value_counts()
            analysis.update({
                'top_values': value_counts.head(10).to_dict(),
                'value_distribution': (value_counts / len(series)).head(5).to_dict(),
                'is_binary': unique_count == 2,
                'is_gender': any(x in col_name.lower() for x in ['gender', 'sex']),
                'is_status': any(x in col_name.lower() for x in ['status', 'state', 'type']),
                'is_region': any(x in col_name.lower() for x in ['region', 'area', 'city', 'province', 'country']),
                'is_category': any(x in col_name.lower() for x in ['category', 'class', 'type', 'group']),
            })

        return analysis

    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """分析整个DataFrame的结构和特征"""
        analysis = {
            'shape': df.shape,
            'columns': {},
            'column_types': {},
            'suggestions': [],
            'potential_issues': [],
        }

        # 分析每个字段
        for col in df.columns:
            col_analysis = self._analyze_column(df[col])
            analysis['columns'][col] = col_analysis

            # 统计类型分布
            col_type = col_analysis['type']
            if col_type not in analysis['column_types']:
                analysis['column_types'][col_type] = []
            analysis['column_types'][col_type].append(col)

        # 缓存分析结果
        self.field_analysis_cache = analysis

        # 生成建议
        self._generate_suggestions(analysis, df)

        return analysis

    def _generate_suggestions(self, analysis: Dict, df: pd.DataFrame):
        """生成数据分析和可视化建议"""
        suggestions = analysis['suggestions']

        # 检查数据质量问题
        for col_name, col_analysis in analysis['columns'].items():
            if col_analysis['null_percentage'] > 50:
                suggestions.append(f"⚠️  字段 '{col_name}' 缺失值较多 ({col_analysis['null_percentage']:.1f}%)")

        # 识别潜在的主键
        unique_counts = {col: analysis['columns'][col]['unique_count'] for col in df.columns}
        total_rows = len(df)
        for col, count in unique_counts.items():
            if count == total_rows and count > 1:
                suggestions.append(f"🔑  '{col}' 可能是主键（唯一值数量等于总行数）")

        # 识别潜在的时间序列
        datetime_cols = analysis['column_types'].get('datetime', [])
        if datetime_cols:
            suggestions.append(f"📅  发现时间字段: {', '.join(datetime_cols)}，适合进行时间序列分析")

        # 识别潜在的分析指标
        numeric_cols = analysis['column_types'].get('numeric', [])
        if numeric_cols:
            amount_cols = [col for col in numeric_cols if analysis['columns'][col].get('is_amount')]
            count_cols = [col for col in numeric_cols if analysis['columns'][col].get('is_count')]
            rate_cols = [col for col in numeric_cols if analysis['columns'][col].get('is_percentage')]

            if amount_cols:
                suggestions.append(f"💰  金额/数值指标: {', '.join(amount_cols)}")
            if count_cols:
                suggestions.append(f"🔢  计数指标: {', '.join(count_cols)}")
            if rate_cols:
                suggestions.append(f"📊  比率指标: {', '.join(rate_cols)}")

        # 识别分类维度
        categorical_cols = analysis['column_types'].get('categorical', [])
        if categorical_cols:
            suggestions.append(f"🏷️  分类维度: {', '.join(categorical_cols[:5])}")

    def get_smart_field_recommendations(self, df: pd.DataFrame, chart_type: str) -> Dict[str, List[str]]:
        """根据图表类型智能推荐X轴和Y轴字段"""
        if df.empty:
            return {'x_axis': [], 'y_axis': []}

        # 分析数据
        if not self.field_analysis_cache or len(self.field_analysis_cache.get('columns', {})) != len(df.columns):
            self.analyze_dataframe(df)

        analysis = self.field_analysis_cache

        # 获取图表配置
        chart_config = CHART_CONFIGS.get(chart_type, {})
        requirements = chart_config.get('requirements', {})
        x_type_req = requirements.get('x_type', [])
        y_type_req = requirements.get('y_type', [])

        recommendations = {'x_axis': [], 'y_axis': []}

        # 特殊处理热力图
        if chart_type == 'heatmap':
            numeric_cols = analysis['column_types'].get('numeric', [])
            if len(numeric_cols) >= 2:
                recommendations['x_axis'] = numeric_cols
                recommendations['y_axis'] = numeric_cols
            return recommendations

        # 推荐X轴字段
        if isinstance(x_type_req, list):
            for col_type in x_type_req:
                if col_type == 'datetime':
                    datetime_cols = analysis['column_types'].get('datetime', [])
                    recommendations['x_axis'].extend(datetime_cols)

                    if not datetime_cols:
                        for col_name, col_analysis in analysis['columns'].items():
                            if col_analysis.get('type') == 'numeric' and col_analysis.get('potential_time'):
                                recommendations['x_axis'].append(col_name)

                elif col_type == 'numeric':
                    numeric_cols = analysis['column_types'].get('numeric', [])
                    for col in numeric_cols:
                        col_analysis = analysis['columns'][col]
                        if not col_analysis.get('is_id'):
                            recommendations['x_axis'].append(col)

                elif col_type == 'categorical':
                    categorical_cols = analysis['column_types'].get('categorical', [])
                    for col in categorical_cols:
                        col_analysis = analysis['columns'][col]
                        if col_analysis.get('is_region') or col_analysis.get('is_category') or col_analysis.get('is_status'):
                            recommendations['x_axis'].insert(0, col)
                        else:
                            recommendations['x_axis'].append(col)

                elif col_type == 'ordinal':
                    ordinal_cols = analysis['column_types'].get('ordinal', [])
                    recommendations['x_axis'].extend(ordinal_cols)

        # 推荐Y轴字段
        if isinstance(y_type_req, list) and 'numeric' in y_type_req:
            numeric_cols = analysis['column_types'].get('numeric', [])

            for priority in ['is_amount', 'is_percentage', 'is_count']:
                for col in numeric_cols:
                    col_analysis = analysis['columns'][col]
                    if col_analysis.get(priority) and col not in recommendations['y_axis']:
                        recommendations['y_axis'].append(col)

            for col in numeric_cols:
                if col not in recommendations['y_axis']:
                    recommendations['y_axis'].append(col)

        # 对于直方图，Y轴是自动计算的
        if chart_type == 'histogram':
            recommendations['y_axis'] = ['频数 (自动计算)']

        # 去重并限制数量
        recommendations['x_axis'] = list(dict.fromkeys(recommendations['x_axis']))[:20]
        recommendations['y_axis'] = list(dict.fromkeys(recommendations['y_axis']))[:20]

        # 如果没有推荐，返回所有字段
        if not recommendations['x_axis']:
            recommendations['x_axis'] = list(df.columns)[:10]
        if not recommendations['y_axis'] and chart_type not in ['histogram']:
            recommendations['y_axis'] = list(df.columns)[:10]

        return recommendations

    def validate_chart_fields(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str) -> Dict[str, Any]:
        """验证图表字段选择的合理性"""
        result = {
            'is_valid': True,
            'warnings': [],
            'suggestions': [],
            'recommended_x': None,
            'recommended_y': None
        }

        if df.empty:
            result['is_valid'] = False
            result['warnings'].append("数据为空")
            return result

        # 获取字段分析
        if not self.field_analysis_cache:
            self.analyze_dataframe(df)

        analysis = self.field_analysis_cache

        # 检查字段是否存在
        if x_col not in df.columns:
            result['is_valid'] = False
            result['warnings'].append(f"X轴字段 '{x_col}' 不存在")
            return result

        if y_col and y_col != '频数 (自动计算)' and y_col not in df.columns:
            result['is_valid'] = False
            result['warnings'].append(f"Y轴字段 '{y_col}' 不存在")
            return result

        # 获取字段类型
        x_analysis = analysis['columns'].get(x_col, {})
        x_type = x_analysis.get('type', 'unknown')

        y_analysis = analysis['columns'].get(y_col, {}) if y_col and y_col != '频数 (自动计算)' else {}
        y_type = y_analysis.get('type', 'unknown') if y_col and y_col != '频数 (自动计算)' else 'auto'

        # 获取图表要求
        chart_config = CHART_CONFIGS.get(chart_type, {})
        requirements = chart_config.get('requirements', {})
        x_type_req = requirements.get('x_type', [])
        y_type_req = requirements.get('y_type', [])

        # 验证X轴
        if isinstance(x_type_req, list):
            if x_type not in x_type_req:
                result['warnings'].append(f"X轴字段类型 '{x_type}' 可能不适合 {chart_type} 图")
                result['suggestions'].append(f"{chart_type} 图推荐使用 {', '.join(x_type_req)} 类型的字段作为X轴")

                recommendations = self.get_smart_field_recommendations(df, chart_type)
                if recommendations['x_axis']:
                    result['recommended_x'] = recommendations['x_axis'][0]

        # 验证Y轴
        if y_col != '频数 (自动计算)':
            if isinstance(y_type_req, list):
                if y_type not in y_type_req:
                    result['warnings'].append(f"Y轴字段类型 '{y_type}' 可能不适合 {chart_type} 图")
                    result['suggestions'].append(f"{chart_type} 图推荐使用 {', '.join(y_type_req)} 类型的字段作为Y轴")

                    recommendations = self.get_smart_field_recommendations(df, chart_type)
                    if recommendations['y_axis']:
                        result['recommended_y'] = recommendations['y_axis'][0]

        # 特殊验证规则
        if chart_type == 'scatter':
            if x_type != 'numeric':
                result['warnings'].append("散点图的X轴应该是数值型字段")
            if y_type != 'numeric':
                result['warnings'].append("散点图的Y轴应该是数值型字段")

        elif chart_type == 'line':
            if x_type not in ['datetime', 'numeric', 'ordinal']:
                result['warnings'].append("折线图的X轴最好是有序字段（时间、数值或有序分类）")

        elif chart_type == 'bar':
            if x_type not in ['categorical', 'ordinal']:
                result['warnings'].append("柱状图的X轴最好是分类字段")
            if y_type != 'numeric':
                result['warnings'].append("柱状图的Y轴应该是数值型字段")

        elif chart_type == 'histogram':
            if x_type != 'numeric':
                result['warnings'].append("直方图的X轴应该是数值型字段")

        # 检查数据量
        if len(df) < 3 and chart_type in ['line', 'scatter']:
            result['warnings'].append(f"数据点太少 ({len(df)})，{chart_type} 图可能效果不佳")

        # 检查分类数量
        if x_type == 'categorical' and x_analysis.get('unique_count', 0) > 20:
            result['warnings'].append(f"X轴分类过多 ({x_analysis['unique_count']} 个)，图表可能过于拥挤")

        return result

    def get_chart_logic_explanation(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: str) -> str:
        """获取图表逻辑解释"""
        if not self.field_analysis_cache:
            self.analyze_dataframe(df)

        analysis = self.field_analysis_cache
        x_analysis = analysis['columns'].get(x_col, {})
        y_analysis = analysis['columns'].get(y_col, {}) if y_col and y_col != '频数 (自动计算)' else {}

        chart_config = CHART_CONFIGS.get(chart_type, {})
        chart_logic = chart_config.get('logic', '')

        explanations = []

        if chart_type == 'line':
            explanations.append(f"📈 **折线图**: 展示 '{y_col}' 如何随着 '{x_col}' 的变化而变化")
            if x_analysis.get('type') == 'datetime':
                explanations.append(f"   • X轴是时间维度 ({x_col})，适合观察趋势")
            explanations.append(f"   • 可以观察增长/下降趋势、周期性变化")

        elif chart_type == 'bar':
            explanations.append(f"📊 **柱状图**: 比较不同 '{x_col}' 类别下 '{y_col}' 的数值大小")
            explanations.append(f"   • X轴是分类维度 ({x_col})，共有 {x_analysis.get('unique_count', 0)} 个类别")
            explanations.append(f"   • Y轴是数值指标 ({y_col})，平均值为 {y_analysis.get('mean', 0):.2f}")

        elif chart_type == 'scatter':
            explanations.append(f"🔵 **散点图**: 探索 '{x_col}' 和 '{y_col}' 之间的相关关系")
            if x_analysis.get('type') == 'numeric' and y_analysis.get('type') == 'numeric':
                try:
                    correlation = df[x_col].corr(df[y_col])
                    explanations.append(f"   • 相关系数: {correlation:.3f}")
                    if correlation > 0.7:
                        explanations.append(f"   • 💡 强正相关: {x_col} 增加时，{y_col} 也倾向于增加")
                    elif correlation < -0.7:
                        explanations.append(f"   • 💡 强负相关: {x_col} 增加时，{y_col} 倾向于减少")
                    elif abs(correlation) < 0.3:
                        explanations.append(f"   • 💡 弱相关: {x_col} 和 {y_col} 关系不明显")
                except:
                    pass

        elif chart_type == 'histogram':
            explanations.append(f"📋 **直方图**: 展示 '{x_col}' 的分布情况")
            explanations.append(f"   • X轴: {x_col}，数值范围从 {x_analysis.get('min', 0):.2f} 到 {x_analysis.get('max', 0):.2f}")
            explanations.append(f"   • Y轴: 频数，表示每个区间的数据点数量")
            if x_analysis.get('skewness', 0) > 1:
                explanations.append(f"   • ⚠️  数据右偏（偏度: {x_analysis.get('skewness', 0):.2f}）")
            elif x_analysis.get('skewness', 0) < -1:
                explanations.append(f"   • ⚠️  数据左偏（偏度: {x_analysis.get('skewness', 0):.2f}）")

        elif chart_type == 'box':
            explanations.append(f"📦 **箱线图**: 展示 '{y_col}' 的分布特征")
            explanations.append(f"   • 箱体表示中间50%的数据（Q1到Q3）")
            explanations.append(f"   • 中位数: {y_analysis.get('median', 0):.2f}")
            explanations.append(f"   • IQR（四分位距）: {y_analysis.get('iqr', 0):.2f}")
            if x_col:
                explanations.append(f"   • 按 '{x_col}' 分组比较")

        elif chart_type == 'heatmap':
            explanations.append(f"🔥 **热力图**: 展示多个数值变量之间的相关性")
            explanations.append(f"   • 颜色越深表示相关性越强")
            explanations.append(f"   • 红色表示正相关，蓝色表示负相关")

        # 添加数据质量说明
        if x_analysis.get('null_percentage', 0) > 0:
            explanations.append(f"   • ⚠️  X轴有 {x_analysis.get('null_percentage', 0):.1f}% 的缺失值")
        if y_analysis.get('null_percentage', 0) > 0:
            explanations.append(f"   • ⚠️  Y轴有 {y_analysis.get('null_percentage', 0):.1f}% 的缺失值")

        return "\n".join(explanations)

    def create_visualization(self, df: pd.DataFrame, chart_type: str,
                             x_col: str, y_col: str = None, group_col: str = None) -> Any:
        """创建智能可视化图表"""
        try:
            # 1. 检查是否有可视化库可用
            if not PLOTLY_AVAILABLE and not MATPLOTLIB_AVAILABLE:
                return self._create_error_plot("没有可用的可视化库，请安装plotly或matplotlib")

            # 2. 验证字段选择
            validation = self.validate_chart_fields(df, chart_type, x_col, y_col)

            # 3. 准备数据
            df_prepared = self._prepare_data_for_visualization(df, x_col, y_col, group_col)

            if df_prepared.empty:
                return self._create_error_plot("数据清洗后无有效数据")

            # 4. 选择可视化引擎
            if PLOTLY_AVAILABLE:
                try:
                    logger.info(f"使用Plotly创建 {chart_type} 图表")
                    fig = self._create_plotly_chart(df_prepared, chart_type, x_col, y_col, group_col)

                    # 添加逻辑解释
                    if fig:
                        logic_text = self.get_chart_logic_explanation(df, chart_type, x_col, y_col)
                        if logic_text:
                            fig.add_annotation(
                                x=0.02, y=1.05,
                                xref="paper", yref="paper",
                                text=f"📝 图表逻辑",
                                showarrow=False,
                                font=dict(size=10, color="gray"),
                                align="left",
                                bgcolor="rgba(255, 255, 255, 0.8)"
                            )

                    return fig
                except Exception as e:
                    logger.error(f"Plotly图表创建失败: {e}")
                    if MATPLOTLIB_AVAILABLE:
                        logger.info("尝试使用Matplotlib作为备选")
                        return self._create_matplotlib_chart(df_prepared, chart_type, x_col, y_col, group_col)
                    else:
                        return self._create_error_plot(f"Plotly图表创建失败: {str(e)}")
            elif MATPLOTLIB_AVAILABLE:
                logger.info(f"使用Matplotlib创建 {chart_type} 图表")
                return self._create_matplotlib_chart(df_prepared, chart_type, x_col, y_col, group_col)
            else:
                return self._create_error_plot("没有可用的可视化库")

        except Exception as e:
            error_msg = f"图表生成错误: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return self._create_error_plot(error_msg)

    def _prepare_data_for_visualization(self, df: pd.DataFrame, x_col: str,
                                        y_col: str = None, group_col: str = None) -> pd.DataFrame:
        """为可视化准备数据"""
        df_clean = df.copy()

        # 处理缺失值
        if y_col and y_col != '频数 (自动计算)':
            df_clean = df_clean.dropna(subset=[x_col, y_col])
        else:
            df_clean = df_clean.dropna(subset=[x_col])

        # 限制分类数量
        if group_col and group_col in df_clean.columns:
            unique_groups = df_clean[group_col].nunique()
            if unique_groups > SCIENTIFIC_CONFIG['max_categories']:
                top_categories = df_clean[group_col].value_counts().nlargest(
                    SCIENTIFIC_CONFIG['max_categories']
                ).index.tolist()
                df_clean = df_clean[df_clean[group_col].isin(top_categories)]
                logger.warning(
                    f"分组字段 '{group_col}' 有 {unique_groups} 个类别，已限制为前 {SCIENTIFIC_CONFIG['max_categories']} 个")

        return df_clean

    def _get_optimal_bins(self, data: pd.Series) -> int:
        """计算最佳分箱数"""
        n = len(data.dropna())
        if n <= 10:
            return n
        elif n <= 100:
            return int(np.sqrt(n))
        else:
            return min(50, int(1 + 3.322 * np.log10(n)))

    def _create_plotly_chart(self, df: pd.DataFrame, chart_type: str,
                             x_col: str, y_col: str = None, group_col: str = None) -> go.Figure:
        """使用Plotly创建图表"""
        fig = None
        chart_config = CHART_CONFIGS.get(chart_type, {})
        title = chart_config.get('title', chart_type)

        color_discrete_sequence = COLOR_PALETTES['categorical']

        if chart_type == 'line':
            fig = px.line(df, x=x_col, y=y_col, color=group_col,
                          title=f"{title}: {y_col} vs {x_col}",
                          color_discrete_sequence=color_discrete_sequence)

        elif chart_type == 'bar':
            fig = px.bar(df, x=x_col, y=y_col, color=group_col,
                         title=f"{title}: {y_col} by {x_col}",
                         color_discrete_sequence=color_discrete_sequence,
                         barmode='group' if group_col else 'relative')

        elif chart_type == 'scatter':
            fig = px.scatter(df, x=x_col, y=y_col, color=group_col,
                             title=f"{title}: {y_col} vs {x_col}",
                             color_discrete_sequence=color_discrete_sequence,
                             trendline='ols' if not group_col else None,
                             opacity=0.7)

        elif chart_type == 'histogram':
            fig = px.histogram(df, x=x_col, color=group_col,
                               title=f"{title}: {x_col} 分布",
                               color_discrete_sequence=color_discrete_sequence,
                               nbins=self._get_optimal_bins(df[x_col]))

        elif chart_type == 'box':
            fig = px.box(df, x=group_col if group_col else x_col, y=y_col,
                         title=f"{title}: {y_col} 分布",
                         color=group_col if group_col else None,
                         color_discrete_sequence=color_discrete_sequence)

        elif chart_type == 'heatmap':
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if len(numeric_cols) < 2:
                return self._create_error_plot("热力图需要至少2个数值列")

            corr_matrix = df[numeric_cols].corr()
            fig = px.imshow(corr_matrix,
                            title="变量相关性热力图",
                            color_continuous_scale=COLOR_PALETTES['diverging'],
                            labels=dict(color="相关系数"),
                            aspect="auto")

        elif chart_type == 'violin':
            fig = px.violin(df, x=group_col if group_col else x_col, y=y_col,
                            title=f"{title}: {y_col} 密度分布",
                            color=group_col if group_col else None,
                            color_discrete_sequence=color_discrete_sequence,
                            box=True)

        else:
            return self._create_error_plot(f"不支持的图表类型: {chart_type}")

        # 统一美化图表
        if fig:
            fig.update_layout(
                title=dict(
                    text=fig.layout.title.text,
                    x=0.5,
                    xanchor='center',
                    font=dict(size=SCIENTIFIC_CONFIG['title_size'])
                ),
                font=dict(size=SCIENTIFIC_CONFIG['font_size']),
                showlegend=True,
                height=500,
                margin=dict(l=50, r=50, t=80, b=50),
                plot_bgcolor='white',
                paper_bgcolor='white'
            )

            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor='lightgray'
            )

            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor='lightgray'
            )

        return fig

    def _create_matplotlib_chart(self, df: pd.DataFrame, chart_type: str,
                                 x_col: str, y_col: str = None, group_col: str = None) -> plt.Figure:
        """使用Matplotlib创建图表"""
        # 设置样式
        plt.style.use('default')
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.alpha'] = 0.3

        colors = COLOR_PALETTES['categorical']

        if chart_type in ['pair', 'heatmap']:
            fig_width = 10
            fig_height = 8
        else:
            fig_width = 10
            fig_height = 6

        fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=SCIENTIFIC_CONFIG['dpi'])

        chart_config = CHART_CONFIGS.get(chart_type, {})
        title = chart_config.get('title', chart_type)
        if x_col:
            title += f": {x_col}"
        if y_col and y_col != '频数 (自动计算)':
            title += f" vs {y_col}"

        ax.set_title(title, fontsize=SCIENTIFIC_CONFIG['title_size'], pad=20)

        try:
            if chart_type == 'line':
                if group_col and group_col in df.columns:
                    groups = df[group_col].unique()
                    for i, group in enumerate(groups[:len(colors)]):
                        group_data = df[df[group_col] == group]
                        ax.plot(group_data[x_col], group_data[y_col],
                                label=str(group), color=colors[i % len(colors)],
                                linewidth=SCIENTIFIC_CONFIG['line_width'], marker='o', markersize=4)
                    ax.legend()
                else:
                    ax.plot(df[x_col], df[y_col],
                            color=colors[0],
                            linewidth=SCIENTIFIC_CONFIG['line_width'], marker='o', markersize=4)

                ax.set_xlabel(x_col, fontsize=SCIENTIFIC_CONFIG['label_size'])
                ax.set_ylabel(y_col, fontsize=SCIENTIFIC_CONFIG['label_size'])

            elif chart_type == 'bar':
                if group_col and group_col in df.columns:
                    pivot_df = df.pivot_table(values=y_col, index=x_col, columns=group_col, aggfunc='mean')
                    pivot_df.plot(kind='bar', ax=ax, color=colors[:len(pivot_df.columns)], alpha=0.8)
                else:
                    ax.bar(df[x_col], df[y_col], color=colors[0], alpha=0.8)

                ax.set_xlabel(x_col, fontsize=SCIENTIFIC_CONFIG['label_size'])
                ax.set_ylabel(y_col, fontsize=SCIENTIFIC_CONFIG['label_size'])
                plt.xticks(rotation=45, ha='right')

            elif chart_type == 'scatter':
                if group_col and group_col in df.columns:
                    groups = df[group_col].unique()
                    for i, group in enumerate(groups[:len(colors)]):
                        group_data = df[df[group_col] == group]
                        ax.scatter(group_data[x_col], group_data[y_col],
                                   label=str(group), color=colors[i % len(colors)],
                                   s=SCIENTIFIC_CONFIG['marker_size'], alpha=0.6)
                    ax.legend()
                else:
                    ax.scatter(df[x_col], df[y_col],
                               color=colors[0],
                               s=SCIENTIFIC_CONFIG['marker_size'], alpha=0.6)

                ax.set_xlabel(x_col, fontsize=SCIENTIFIC_CONFIG['label_size'])
                ax.set_ylabel(y_col, fontsize=SCIENTIFIC_CONFIG['label_size'])

            elif chart_type == 'histogram':
                ax.hist(df[x_col].dropna(),
                        bins=self._get_optimal_bins(df[x_col]),
                        color=colors[0], alpha=0.7, edgecolor='black')
                ax.set_xlabel(x_col, fontsize=SCIENTIFIC_CONFIG['label_size'])
                ax.set_ylabel('频数', fontsize=SCIENTIFIC_CONFIG['label_size'])

            elif chart_type == 'box':
                if group_col and group_col in df.columns:
                    data = [df[df[group_col] == g][y_col] for g in df[group_col].unique()]
                    labels = [str(g) for g in df[group_col].unique()]
                    ax.boxplot(data, labels=labels, patch_artist=True,
                               boxprops=dict(facecolor=colors[0], alpha=0.7))
                else:
                    ax.boxplot(df[y_col], patch_artist=True,
                               boxprops=dict(facecolor=colors[0], alpha=0.7))
                    ax.set_xticklabels([y_col])

                ax.set_ylabel(y_col if not group_col else '数值', fontsize=SCIENTIFIC_CONFIG['label_size'])

            elif chart_type == 'heatmap':
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                if len(numeric_cols) < 2:
                    return self._create_error_plot("热力图需要至少2个数值列")

                corr_matrix = df[numeric_cols].corr()
                im = ax.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-1, vmax=1)
                ax.set_xticks(range(len(corr_matrix.columns)))
                ax.set_yticks(range(len(corr_matrix.columns)))
                ax.set_xticklabels(corr_matrix.columns, rotation=45, ha='right')
                ax.set_yticklabels(corr_matrix.columns)
                plt.colorbar(im, ax=ax).set_label('相关系数', fontsize=SCIENTIFIC_CONFIG['label_size'])

            else:
                return self._create_error_plot(f"不支持的图表类型: {chart_type}")

        except Exception as e:
            logger.error(f"Matplotlib图表生成错误: {e}")
            return self._create_error_plot(f"图表生成错误: {str(e)}")

        # 设置通用样式
        ax.tick_params(axis='both', which='major', labelsize=SCIENTIFIC_CONFIG['tick_size'])
        ax.grid(True, alpha=SCIENTIFIC_CONFIG['grid_alpha'])

        # 添加数据信息
        info_text = f"数据点: {len(df)}"
        if group_col and group_col in df.columns:
            info_text += f" | 分组数: {df[group_col].nunique()}"

        ax.text(0.02, -0.12, info_text, transform=ax.transAxes,
                fontsize=9, color='gray', verticalalignment='top')

        plt.tight_layout()
        return fig

    def _create_error_plot(self, message: str) -> Any:
        """创建错误提示图表"""
        if PLOTLY_AVAILABLE:
            try:
                fig = go.Figure()
                fig.add_annotation(
                    text=f"❌ {message}",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5,
                    showarrow=False,
                    font=dict(size=16, color="red"),
                    align="center"
                )
                fig.update_layout(
                    title="图表生成错误",
                    xaxis=dict(visible=False),
                    yaxis=dict(visible=False),
                    plot_bgcolor='white',
                    height=400,
                    width=600
                )
                return fig
            except Exception as e:
                logger.error(f"创建Plotly错误图表失败: {e}")

        if MATPLOTLIB_AVAILABLE:
            try:
                fig, ax = plt.subplots(figsize=(8, 4))
                ax.text(0.5, 0.5, f"❌ {message}",
                        ha='center', va='center',
                        transform=ax.transAxes,
                        color='red', fontsize=12)
                ax.axis('off')
                plt.tight_layout()
                return fig
            except Exception as e:
                logger.error(f"创建Matplotlib错误图表失败: {e}")

        # 如果所有可视化方法都失败，返回文本消息
        return f"错误: {message}"

    def get_available_chart_types(self) -> List[Dict[str, str]]:
        """获取可用的图表类型（带描述）"""
        available_charts = []

        for chart_type, config in CHART_CONFIGS.items():
            available_charts.append({
                'value': chart_type,
                'title': config['title'],
                'label': f"{config['title']} - {config['description']}",
                'description': config['description'],
                'logic': config.get('logic', '')
            })

        return available_charts

    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """获取数据摘要"""
        if df.empty:
            return {"error": "数据为空"}

        # 分析数据
        if not self.field_analysis_cache:
            self.analyze_dataframe(df)

        analysis = self.field_analysis_cache

        summary = {
            'basic_info': {
                'rows': df.shape[0],
                'columns': df.shape[1],
                'memory_usage': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
            },
            'column_types': analysis['column_types'],
            'suggestions': analysis.get('suggestions', [])[:5],
            'potential_issues': analysis.get('potential_issues', []),
        }

        # 添加每个字段的简要信息
        field_summary = {}
        for col_name, col_analysis in analysis['columns'].items():
            field_summary[col_name] = {
                'type': col_analysis['type'],
                'unique_values': col_analysis['unique_count'],
                'missing_percentage': f"{col_analysis['null_percentage']:.1f}%",
                'description': self._get_field_description(col_analysis)
            }

        summary['fields'] = field_summary

        return summary

    def _get_field_description(self, col_analysis: Dict) -> str:
        """获取字段描述"""
        col_type = col_analysis['type']
        col_name = col_analysis['name']

        if col_type == 'numeric':
            if col_analysis.get('is_amount'):
                return "金额/数值指标"
            elif col_analysis.get('is_count'):
                return "计数指标"
            elif col_analysis.get('is_percentage'):
                return "比率/百分比"
            elif col_analysis.get('is_id'):
                return "标识符/ID"
            elif col_analysis.get('potential_time'):
                return f"时间数值（{col_analysis.get('time_unit', '单位')}）"
            else:
                return "一般数值"

        elif col_type == 'datetime':
            return "日期时间"

        elif col_type == 'categorical':
            if col_analysis.get('is_gender'):
                return "性别分类"
            elif col_analysis.get('is_status'):
                return "状态分类"
            elif col_analysis.get('is_region'):
                return "地区分类"
            elif col_analysis.get('is_category'):
                return "类别分类"
            else:
                return "分类变量"

        elif col_type == 'boolean':
            return "布尔值"

        elif col_type == 'text':
            return "文本"

        else:
            return "未知类型"