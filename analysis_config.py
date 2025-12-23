ANALYSIS_CONFIG = {
    # 分析等级配置
    "levels": {
        "basic": {
            "name": "基础分析",
            "description": "快速概览，适合初次了解数据",
            "features": [
                "数据基本统计",
                "主要发现摘要",
                "简要建议"
            ],
            "time_estimate": "1-2分钟",
            "suitable_for": ["数据探索", "快速检查", "初步了解"]
        },
        "standard": {
            "name": "标准分析",
            "description": "详细业务分析，适合常规决策",
            "features": [
                "完整的数据概览",
                "业务洞察发现",
                "实用的行动建议",
                "趋势分析"
            ],
            "time_estimate": "3-5分钟",
            "suitable_for": ["业务分析", "定期报告", "决策支持"]
        },
        "advanced": {
            "name": "深度分析",
            "description": "多维度深度挖掘，适合复杂问题",
            "features": [
                "多维度分析",
                "高级统计分析",
                "模式识别",
                "预测性洞察",
                "风险识别"
            ],
            "time_estimate": "5-10分钟",
            "suitable_for": ["深度研究", "战略规划", "复杂问题解决"]
        },
        "expert": {
            "name": "专家级分析",
            "description": "全面学术级分析，适合研究场景",
            "features": [
                "学术级统计分析",
                "预测建模",
                "因果推断",
                "多方法验证",
                "研究论文格式"
            ],
            "time_estimate": "10-15分钟",
            "suitable_for": ["学术研究", "重大决策", "长期规划"]
        }
    },

    # 分析维度配置
    "dimensions": {
        "time": {
            "name": "时间维度",
            "description": "分析数据随时间的变化",
            "analyses": ["趋势分析", "季节性分析", "周期性分析", "预测分析"]
        },
        "geo": {
            "name": "地理维度",
            "description": "分析不同地理区域的数据差异",
            "analyses": ["区域对比", "地理分布", "区域聚类"]
        },
        "product": {
            "name": "产品维度",
            "description": "分析产品层面的表现",
            "analyses": ["产品对比", "产品组合分析", "产品生命周期"]
        },
        "customer": {
            "name": "客户维度",
            "description": "分析客户特征和行为",
            "analyses": ["客户细分", "RFM分析", "客户生命周期价值"]
        },
        "channel": {
            "name": "渠道维度",
            "description": "分析不同渠道的表现",
            "analyses": ["渠道对比", "渠道效率", "渠道协同效应"]
        }
    },

    # 分析模板
    "templates": {
        "overview": {
            "name": "数据概览分析",
            "prompt": "请全面分析数据的基本情况，包括数据分布、缺失值、异常值、主要特征等。",
            "level": "basic"
        },
        "trend": {
            "name": "趋势深度分析",
            "prompt": "请进行深度的趋势分析，包括季节性、周期性、增长趋势、转折点识别和预测模型。",
            "level": "advanced"
        },
        "correlation": {
            "name": "多维度关联分析",
            "prompt": "请探索各变量间的复杂关系和交互效应，使用相关性分析、回归分析等方法。",
            "level": "expert"
        },
        "business": {
            "name": "商业智能洞察",
            "prompt": "从商业智能角度分析数据，提供实用的业务洞察、KPI分析和决策支持建议。",
            "level": "standard"
        }
    }
}