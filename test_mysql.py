import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def test_mysql_connection():
    """测试MySQL连接"""
    try:
        from sqlalchemy import create_engine, text

        # 直接从配置读取
        from config import DATABASE_CONFIG

        username = DATABASE_CONFIG['username']
        password = DATABASE_CONFIG['password']
        host = DATABASE_CONFIG['host']
        port = DATABASE_CONFIG['port']
        database = DATABASE_CONFIG['database']
        driver = DATABASE_CONFIG['driver']

        connection_string = f"mysql+{driver}://{username}:{password}@{host}:{port}/{database}"

        print("🔍 测试MySQL连接...")
        print(f"连接字符串: {connection_string.replace(password, '***')}")

        engine = create_engine(connection_string)

        with engine.connect() as conn:
            # 测试基本连接
            result = conn.execute(text("SELECT 1"))
            print("✅ MySQL基本连接成功！")

            # 获取数据库信息
            result = conn.execute(text("SELECT DATABASE()"))
            db_name = result.scalar()
            print(f"📊 当前数据库: {db_name}")

            # 获取所有表
            result = conn.execute(text("SHOW TABLES"))
            tables = [row[0] for row in result]
            print(f"📋 发现 {len(tables)} 个表: {tables}")

            # 显示表结构示例
            if tables:
                print(f"\n📝 表 '{tables[0]}' 的结构:")
                result = conn.execute(text(f"DESCRIBE {tables[0]}"))
                for row in result:
                    print(f"  - {row[0]} ({row[1]})")

        return True

    except ModuleNotFoundError as e:
        print(f"❌ 缺少MySQL驱动: {e}")
        print("请运行: pip install pymysql")
        return False
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return False


if __name__ == "__main__":
    success = test_mysql_connection()
    if success:
        print("\n🎉 MySQL连接测试通过！可以运行主程序了。")
    else:
        print("\n💡 请检查:")
        print("1. MySQL服务是否启动")
        print("2. 数据库用户名密码是否正确")
        print("3. 数据库是否存在")
        print("4. MySQL驱动是否安装")