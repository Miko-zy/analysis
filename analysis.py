# analysis.py
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
            'x_role': '分类变量',
            'y_role': '数值指标'
        },
        'best_for': ['分类比较', '排名分析', '占比分析'],
        'logic': '通过柱子高度比较不同类别的数值大小'
    },
    'scatter': {
        'title': '相关性分析图',
        'description': '展示两个数值变量之间的相关关系',
        'requirements': {
            'x_type': ['numeric'],
            'y_type': ['numeric'],
            'x_role': '自变量',
            'y_role': '因变量'
        },
        'best_for': ['相关性分析', '聚类分析', '异常检测'],
        'logic': '通过点的分布观察两个变量间的关系'
    },
    'histogram': {
        'title': '分布分析图',
        'description': '展示单一数值变量的分布情况',
        'requirements': {
            'x_type': ['numeric'],
            'y_type': [],  # 自动生成频率
            'x_role': '数值变量',
            'y_role': '频次/密度'
        },
        'best_for': ['分布分析', '异常值检测', '数据质量检查'],
        'logic': '通过柱子高度表示数值落在各区间的频次'
    },
    'pie': {
        'title': '构成分析图',
        'description': '展示各部分占总体的比例关系',
        'requirements': {
            'x_type': ['categorical'],
            'y_type': ['numeric'],
            'x_role': '组成部分',
            'y_role': '数值指标'
        },
        'best_for': ['占比分析', '构成分析'],
        'logic': '通过扇形面积表示各部分占比'
    },
    'box': {
        'title': '箱线图',
        'description': '展示数据的分布特征和异常值',
        'requirements': {
            'x_type': ['categorical', 'numeric'],
            'y_type': ['numeric'],
            'x_role': '分组变量',
            'y_role': '数值变量'
        },
        'best_for': ['分布比较', '异常值检测', '离散程度分析'],
        'logic': '通过箱子和须线展示数据的四分位数和异常值'
    }
}


class DataAnalyzer:
    def __init__(self):
        """初始化数据分析器"""
        self.current_figure = None
        self.field_analysis_cache = {}
        logger.info("🚀 智能数据分析器初始化完成")

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

        # 文本型字段的详细分析
        elif col_type == 'text':
            lengths = series.dropna().astype(str).str.len()
            analysis.update({
                'avg_length': lengths.mean(),
                'min_length': lengths.min(),
                'max_length': lengths.max(),
                'common_prefix': self._find_common_prefix(series),
            })

        # 分类字段的详细分析
        elif col_type in ['categorical', 'ordinal']:
            value_counts = series.value_counts()
            analysis.update({
                'top_categories': value_counts.head(5).to_dict(),
                'category_distribution': value_counts.to_dict(),
                'category_count': len(value_counts),
            })

        return analysis

    def _find_common_prefix(self, series: pd.Series) -> Optional[str]:
        """查找字符串的公共前缀"""
        non_null_values = series.dropna().astype(str)
        if len(non_null_values) < 2:
            return None

        prefix = non_null_values.iloc[0]
        for value in non_null_values.iloc[1:]:
            while not value.startswith(prefix) and prefix:
                prefix = prefix[:-1]
            if not prefix:
                break
        return prefix if len(prefix) > 1 else None

    def generate_summary_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """生成数据摘要统计"""
        if df.empty:
            return {"error": "数据为空"}

        summary = {
            'basic_info': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum()
            },
            'columns': {}
        }

        # 分析每一列
        for column in df.columns:
            try:
                analysis = self._analyze_column(df[column])
                summary['columns'][column] = analysis
            except Exception as e:
                logger.error(f"分析列 {column} 时出错: {e}")
                summary['columns'][column] = {'error': str(e)}

        return summary

    def create_visualization(self, df: pd.DataFrame, chart_type: str, 
                           x_column: str, y_column: Optional[str] = None,
                           group_by: Optional[str] = None) -> Dict[str, Any]:
        """创建可视化图表规范"""
        try:
            if df.empty:
                return {"error": "数据为空"}

            # 检查所需列是否存在
            required_columns = [x_column]
            if y_column:
                required_columns.append(y_column)
            if group_by:
                required_columns.append(group_by)

            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                return {"error": f"缺少列: {missing_columns}"}

            # 构建可视化规范
            viz_spec = {
                'chart_type': chart_type,
                'x_column': x_column,
                'y_column': y_column,
                'group_by': group_by,
                'data_sample': df.head(100).to_dict('records')  # 限制样本大小
            }

            return {"success": True, "spec": viz_spec}

        except Exception as e:
            logger.error(f"创建可视化失败: {e}")
            return {"error": str(e)}

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

    def _create_error_plot(self, error_message: str):
        """创建错误提示的图表"""
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, error_message,
                ha='center', va='center',
                fontsize=14, color='red')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        plt.tight_layout()
        return fig

    # 添加 get_data_summary 方法（在日志中被调用）
    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """获取数据摘要（兼容性方法）"""
        return self.generate_summary_statistics(df)

    # 添加智能推荐字段的方法
    def get_smart_field_recommendations(self, df: pd.DataFrame, chart_type: str) -> Dict[str, List[str]]:
        """智能推荐图表字段"""
        recommendations = {'x_axis': [], 'y_axis': []}

        for column in df.columns:
            col_info = self._analyze_column(df[column])

            if chart_type == 'bar':
                if col_info['type'] in ['categorical', 'ordinal']:
                    recommendations['x_axis'].append(column)
                elif col_info['type'] == 'numeric':
                    recommendations['y_axis'].append(column)

            elif chart_type == 'line':
                if col_info['type'] in ['datetime', 'numeric', 'ordinal']:
                    recommendations['x_axis'].append(column)
                elif col_info['type'] == 'numeric':
                    recommendations['y_axis'].append(column)

            elif chart_type == 'scatter':
                if col_info['type'] == 'numeric':
                    recommendations['x_axis'].append(column)
                    recommendations['y_axis'].append(column)

            elif chart_type == 'histogram':
                if col_info['type'] == 'numeric':
                    recommendations['x_axis'].append(column)

            elif chart_type == 'pie':
                if col_info['type'] in ['categorical', 'ordinal']:
                    recommendations['x_axis'].append(column)
                elif col_info['type'] == 'numeric':
                    recommendations['y_axis'].append(column)

            elif chart_type == 'box':
                if col_info['type'] in ['categorical', 'ordinal']:
                    recommendations['x_axis'].append(column)
                elif col_info['type'] == 'numeric':
                    recommendations['y_axis'].append(column)

        return recommendations

    # 添加验证字段的方法
    def validate_chart_fields(self, df: pd.DataFrame, chart_type: str, x_col: str, y_col: Optional[str] = None) -> Dict[
        str, Any]:
        """验证图表字段选择的合理性"""
        result = {
            'is_valid': False,
            'warnings': [],
            'suggestions': [],
            'recommended_x': None,
            'recommended_y': None
        }

        if x_col not in df.columns:
            result['warnings'].append(f"X轴字段 '{x_col}' 不存在")
            return result

        x_info = self._analyze_column(df[x_col])

        if chart_type == 'bar':
            if x_info['type'] not in ['categorical', 'ordinal']:
                result['warnings'].append(f"条形图的X轴应该是分类或有序变量，但 '{x_col}' 是 {x_info['type']} 类型")

        elif chart_type == 'line':
            if x_info['type'] not in ['datetime', 'numeric', 'ordinal']:
                result['warnings'].append(f"折线图的X轴应该是时间、数值或有序变量，但 '{x_col}' 是 {x_info['type']} 类型")

        elif chart_type == 'scatter':
            if x_info['type'] != 'numeric':
                result['warnings'].append(f"散点图的X轴应该是数值变量，但 '{x_col}' 是 {x_info['type']} 类型")

        # 获取推荐字段
        recommendations = self.get_smart_field_recommendations(df, chart_type)
        if recommendations['x_axis']:
            result['recommended_x'] = recommendations['x_axis'][0]

        if y_col and recommendations['y_axis']:
            result['recommended_y'] = recommendations['y_axis'][0]

        result['is_valid'] = len(result['warnings']) == 0
        return result

    # 添加图表逻辑解释方法
    def get_chart_logic_explanation(self, df: pd.DataFrame, chart_type: str, x_col: str,
                                    y_col: Optional[str] = None) -> str:
        """获取图表逻辑解释"""
        if chart_type in CHART_CONFIGS:
            config = CHART_CONFIGS[chart_type]
            explanation = f"**图表类型**: {config['title']}\n\n"
            explanation += f"**描述**: {config['description']}\n\n"
            explanation += f"**逻辑**: {config.get('logic', '')}\n\n"

            if x_col in df.columns:
                x_info = self._analyze_column(df[x_col])
                explanation += f"**X轴分析**:\n"
                explanation += f"- 字段: {x_col}\n"
                explanation += f"- 类型: {x_info['type']}\n"
                explanation += f"- 唯一值数: {x_info.get('unique_count', 'N/A')}\n"

            if y_col and y_col in df.columns:
                y_info = self._analyze_column(df[y_col])
                explanation += f"\n**Y轴分析**:\n"
                explanation += f"- 字段: {y_col}\n"
                explanation += f"- 类型: {y_info['type']}\n"
                if y_info['type'] == 'numeric':
                    explanation += f"- 平均值: {y_info.get('mean', 'N/A'):.2f}\n"
                    explanation += f"- 范围: {y_info.get('min', 'N/A'):.2f} ~ {y_info.get('max', 'N/A'):.2f}\n"

            return explanation
        return ""