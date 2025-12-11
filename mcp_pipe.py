# mcp_pipe.py
import os
import subprocess
import sys

if __name__ == "__main__":
    # 设置环境变量
    os.environ['PYTHONPATH'] = '.'

    # 启动MCP服务
    subprocess.run([sys.executable, "data_analysis_tool10.py"])