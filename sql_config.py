# SQL生成策略
SQL_GENERATION_CONFIG = {
    # 验证级别
    'validation_level': 'strict',  # strict, medium, lenient

    # 自动修正
    'auto_fix_columns': True,
    'auto_add_limit': True,
    'default_limit': 50,

    # 复杂性控制
    'max_joins': 2,
    'max_subqueries': 1,
    'max_where_conditions': 3,

    # 回退策略
    'fallback_to_simple': True,
    'simple_query_columns': 5,

    # 错误处理
    'retry_on_error': True,
    'max_retries': 2,

    # 列名相似度阈值
    'similarity_threshold': 0.6,

    # 常见列名映射
    'column_mappings': {
        # 数量相关
        '订购数量': ['数量', 'quantity', 'qty', 'order_quantity'],
        '购买数量': ['数量', 'quantity', 'qty', 'purchase_quantity'],
        '销售数量': ['数量', 'quantity', 'qty', 'sales_quantity'],
        '产品数量': ['数量', 'quantity', 'qty', 'product_quantity'],

        # 金额相关
        '订购金额': ['金额', 'amount', 'total_amount', 'order_amount'],
        '购买金额': ['金额', 'amount', 'purchase_amount'],
        '销售金额': ['金额', 'amount', 'sales_amount', 'revenue'],
        '产品金额': ['金额', 'amount', 'product_amount'],

        # 时间相关
        '订购时间': ['时间', 'date', 'datetime', 'order_date', 'created_at'],
        '购买时间': ['时间', 'date', 'datetime', 'purchase_date'],
        '销售时间': ['时间', 'date', 'datetime', 'sales_date'],
        '创建时间': ['时间', 'date', 'datetime', 'created_at'],

        # 状态相关
        '订单状态': ['状态', 'status', 'order_status'],
        '支付状态': ['状态', 'status', 'payment_status'],
        '发货状态': ['状态', 'status', 'delivery_status'],
        '产品状态': ['状态', 'status', 'product_status'],

        # 用户相关
        '用户ID': ['用户id', 'user_id', 'uid', 'id'],
        '用户名': ['用户名称', 'user_name', 'username', 'name'],
        '用户电话': ['电话', 'phone', 'mobile', 'telephone'],
        '用户邮箱': ['邮箱', 'email', 'mail'],
    }
}

# 安全的SQL模板
SAFE_SQL_TEMPLATES = [
    "SELECT * FROM `{table}` LIMIT {limit}",
    "SELECT id, name FROM `{table}` LIMIT {limit}",
    "SELECT COUNT(*) as count FROM `{table}`",
    "SELECT * FROM `{table}` ORDER BY id DESC LIMIT {limit}",
]