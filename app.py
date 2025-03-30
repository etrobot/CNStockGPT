import webbrowser
from factor.multi_factor import calculate_multi_factors
import os

def main():
    # 执行多因子计算并获取结果及HTML路径
    result_df, html_path = calculate_multi_factors()
    
    if html_path and os.path.exists(html_path):
        # 转换文件路径为URL格式
        absolute_path = os.path.abspath(html_path)
        webbrowser.open(f'file://{absolute_path}')
    else:
        print("HTML文件生成失败，请检查日志")

if __name__ == '__main__':
    main()