import re
from typing import Dict, List, Any, Optional, Tuple


class SQLTemplateSystem:
    """SQL模板系统"""

    TEMPLATES = {
        'basic_select': {
            'description': '基础查询',
            'sql_template': "SELECT {columns} FROM `{table}` {where} {group_by} {order_by} {limit}",
            'default_limit': 100
        },
        'summary_stats': {
            'description': '统计汇总',
            'sql_template': """
SELECT 
    COUNT(*) as 总记录数,
    {numeric_columns}
FROM `{table}`
{where}
            """,
            'numeric_columns_template': "AVG(`{column}`) as `{column}_平均值`, MAX(`{column}`) as `{column}_最大值`, MIN(`{column}`) as `{column}_最小值`"
        },
        'group_by_stats': {
            'description': '分组统计',
            'sql_template': """
SELECT 
    `{group_column}` as 分组,
    COUNT(*) as 记录数,
    {agg_columns}
FROM `{table}`
{where}
GROUP BY `{group_column}`
ORDER BY 记录数 DESC
{limit}
            """
        },
        'time_series': {
            'description': '时间序列分析',
            'sql_template': """
SELECT 
    DATE(`{date_column}`) as 日期,
    COUNT(*) as 记录数,
    {agg_columns}
FROM `{table}`
WHERE `{date_column}` IS NOT NULL
GROUP BY DATE(`{date_column}`)
ORDER BY 日期
{limit}
            """
        },
        'ranking': {
            'description': '排名查询',
            'sql_template': """
SELECT 
    `{ranking_column}` as 名称,
    `{value_column}` as 数值
FROM `{table}`
WHERE `{value_column}` IS NOT NULL
ORDER BY `{value_column}` DESC
{limit}
            """
        },
        'related_query': {
            'description': '关联查询',
            'sql_template': """
SELECT 
    t1.`{t1_column}` as 表1字段,
    t2.`{t2_column}` as 表2字段,
    COUNT(*) as 关联数
FROM `{table1}` t1
JOIN `{table2}` t2 ON t1.`{join_key1}` = t2.`{join_key2}`
{where}
GROUP BY t1.`{t1_column}`, t2.`{t2_column}`
ORDER BY 关联数 DESC
{limit}
            """
        }
    }

    @staticmethod
    def classify_query_intent(natural_language_query: str) -> Tuple[str, Dict[str, Any]]:
        """分类查询意图"""
        query_lower = natural_language_query.lower()

        keyword_patterns = {
            'summary_stats': ['总计', '合计', '汇总', '总和', '平均', '最大值', '最小值', '统计', '数量'],
            'group_by_stats': ['分组', '分类', '按.*统计', '各个.*的', '每种', '每类', '各地区'],
            'time_series': ['时间', '日期', '月份', '季度', '年份', '趋势', '每天', '每月', '逐年'],
            'ranking': ['排名', '前.*名', '最高', '最低', '最多', '最少', '最好', '最差', 'top'],
            'related_query': ['关联', '关系', '连接', '涉及', '和.*一起', '同时'],
            'basic_select': ['查询', '查看', '显示', '列出', '找', '搜索']
        }

        matched_intent = 'basic_select'
        confidence = 0.0

        for intent, patterns in keyword_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    if intent != matched_intent:
                        matched_intent = intent
                        confidence = 0.8
                        break

        extracted_params = SQLTemplateSystem._extract_query_params(natural_language_query)

        return matched_intent, {
            'intent': matched_intent,
            'confidence': confidence,
            'extracted_params': extracted_params
        }

    @staticmethod
    def _extract_query_params(query: str) -> Dict[str, Any]:
        """从自然语言查询中提取参数"""
        params = {
            'table_names': [],
            'column_names': [],
            'numeric_columns': [],
            'date_columns': [],
            'text_columns': [],
            'filters': [],
            'sort_order': 'DESC',
            'limit_value': 10
        }

        table_patterns = ['表\s*[："\']?([^"\'，,。\.\s]+)']
        for pattern in table_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                params['table_names'].extend(matches)

        column_patterns = ['字段\s*[："\']?([^"\'，,。\.\s]+)', '列\s*[："\']?([^"\'，,。\.\s]+)']
        for pattern in column_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                params['column_names'].extend(matches)

        limit_matches = re.findall(r'前\s*(\d+)\s*名', query)
        if limit_matches:
            params['limit_value'] = int(limit_matches[0])
        elif re.search(r'前十|前20|前50', query):
            num_match = re.search(r'前(\d+)', query)
            if num_match:
                params['limit_value'] = int(num_match.group(1))

        if re.search(r'最低|最少|最小|降序|倒序', query):
            params['sort_order'] = 'ASC'

        return params

    @staticmethod
    def generate_from_template(
            intent: str,
            table_info: Dict[str, List[Dict]],
            extracted_params: Dict[str, Any]
    ) -> str:
        """根据模板生成SQL"""
        if intent not in SQLTemplateSystem.TEMPLATES:
            intent = 'basic_select'

        template = SQLTemplateSystem.TEMPLATES[intent]
        sql_template = template['sql_template']

        tables = list(table_info.keys())
        if not tables:
            return "SELECT 1"

        selected_table = tables[0]
        columns = table_info[selected_table]

        numeric_cols = [col['name'] for col in columns if col.get('category') in ['integer', 'numeric']]
        text_cols = [col['name'] for col in columns if col.get('category') == 'text']
        date_cols = [col['name'] for col in columns if col.get('category') == 'datetime']

        template_params = {
            'table': selected_table,
            'limit': f"LIMIT {extracted_params.get('limit_value', 100)}"
        }

        if intent == 'basic_select':
            select_cols = []
            for col in columns[:5]:
                col_name = col['name']
                alias = SQLTemplateSystem._get_column_alias(col_name)
                select_cols.append(f"`{col_name}` as `{alias}`")

            template_params['columns'] = ', '.join(select_cols) if select_cols else '*'
            template_params['where'] = ''
            template_params['group_by'] = ''
            template_params['order_by'] = 'ORDER BY 1'

        elif intent == 'summary_stats':
            if numeric_cols:
                numeric_templates = []
                for col in numeric_cols[:3]:
                    template_str = template['numeric_columns_template']
                    numeric_templates.append(template_str.format(column=col))

                template_params['numeric_columns'] = ', '.join(numeric_templates)
            else:
                template_params['numeric_columns'] = 'NULL as 无数值列'

        elif intent == 'group_by_stats':
            if text_cols:
                template_params['group_column'] = text_cols[0]
            elif columns:
                template_params['group_column'] = columns[0]['name']
            else:
                template_params['group_column'] = 'id'

            if numeric_cols:
                agg_columns = []
                for col in numeric_cols[:2]:
                    agg_columns.append(f"SUM(`{col}`) as `{col}_总和`, AVG(`{col}`) as `{col}_平均`")
                template_params['agg_columns'] = ', '.join(agg_columns)
            else:
                template_params['agg_columns'] = 'NULL as 无数值列'

        elif intent == 'time_series':
            if date_cols:
                template_params['date_column'] = date_cols[0]
            else:
                return SQLTemplateSystem.generate_from_template('group_by_stats', table_info, extracted_params)

            if numeric_cols:
                agg_columns = []
                for col in numeric_cols[:2]:
                    agg_columns.append(f"SUM(`{col}`) as `{col}_总计`")
                template_params['agg_columns'] = ', '.join(agg_columns)
            else:
                template_params['agg_columns'] = 'COUNT(*) as 记录数'

        elif intent == 'ranking':
            if numeric_cols and text_cols:
                template_params['ranking_column'] = text_cols[0]
                template_params['value_column'] = numeric_cols[0]
            elif numeric_cols and columns:
                template_params['ranking_column'] = columns[0]['name']
                template_params['value_column'] = numeric_cols[0]
            else:
                return SQLTemplateSystem.generate_from_template('basic_select', table_info, extracted_params)

        elif intent == 'related_query':
            if len(tables) >= 2:
                template_params['table1'] = tables[0]
                template_params['table2'] = tables[1]

                table1_cols = [col['name'] for col in table_info[tables[0]]]
                table2_cols = [col['name'] for col in table_info[tables[1]]]

                join_keys = set(table1_cols).intersection(set(table2_cols))
                if join_keys:
                    join_key = list(join_keys)[0]
                    template_params['join_key1'] = join_key
                    template_params['join_key2'] = join_key
                else:
                    template_params['join_key1'] = table1_cols[0] if table1_cols else 'id'
                    template_params['join_key2'] = table2_cols[0] if table2_cols else 'id'

                template_params['t1_column'] = table1_cols[0] if table1_cols else 'id'
                template_params['t2_column'] = table2_cols[0] if table2_cols else 'id'
            else:
                return SQLTemplateSystem.generate_from_template('group_by_stats', table_info, extracted_params)

        try:
            sql = sql_template.format(**template_params)
            sql = re.sub(r'\n\s*\n', '\n', sql)
            return sql.strip()
        except Exception as e:
            return f"SELECT * FROM `{selected_table}` LIMIT 10"

    @staticmethod
    def _get_column_alias(column_name: str) -> str:
        """获取列的中文别名"""
        col_lower = column_name.lower()

        english_mappings = {
            'id': 'ID', 'name': '名称', 'title': '标题', 'desc': '描述', 'description': '描述',
            'date': '日期', 'time': '时间', 'datetime': '日期时间', 'create_time': '创建时间',
            'update_time': '更新时间', 'amount': '金额', 'price': '价格', 'cost': '成本',
            'fee': '费用', 'money': '金额', 'total': '总计', 'sum': '合计', 'count': '数量',
            'quantity': '数量', 'qty': '数量', 'number': '编号', 'num': '编号', 'status': '状态',
            'state': '状态', 'type': '类型', 'category': '分类', 'class': '类别', 'group': '分组',
            'user': '用户', 'username': '用户名', 'password': '密码', 'email': '邮箱',
            'phone': '电话', 'mobile': '手机', 'address': '地址', 'city': '城市',
            'province': '省份', 'country': '国家', 'region': '区域', 'area': '地区',
            'score': '分数', 'grade': '等级', 'level': '级别', 'rate': '比率', 'ratio': '比例',
            'percent': '百分比', 'percentage': '百分比', 'age': '年龄', 'gender': '性别',
            'sex': '性别', 'birth': '生日', 'birthday': '生日'
        }

        if re.search(r'[\u4e00-\u9fff]', column_name):
            return column_name

        for eng_key, chi_value in english_mappings.items():
            if eng_key == col_lower or f'_{eng_key}' in col_lower or f'{eng_key}_' in col_lower:
                return chi_value

        suffixes = {
            '_id': 'ID', '_name': '名称', '_time': '时间', '_date': '日期',
            '_amount': '金额', '_price': '价格', '_count': '数量', '_total': '总计',
            '_status': '状态', '_type': '类型'
        }

        for suffix, alias in suffixes.items():
            if col_lower.endswith(suffix):
                prefix = col_lower[:-len(suffix)]
                if prefix in english_mappings:
                    return f"{english_mappings[prefix]}{alias}"
                else:
                    return f"{prefix}{alias}"

        return column_name