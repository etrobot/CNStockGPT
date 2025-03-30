#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import re
import pandas as pd
import akshare as ak

logger = logging.getLogger(__name__)

def check_data_structure(symbol):
    """
    检查股票数据结构并输出统计信息，同时检查项目中其他脚本使用的akshare API字段是否正确
    
    参数：
        symbol (str): 股票代码，例如'sh000001'
    """
    try:
        logger.info('开始获取股票数据...')
        
        # 新增：扫描项目文件
        project_root = os.path.dirname(os.path.abspath(__file__))
        akshare_scripts = []
        
        for root, dirs, files in os.walk(project_root):
            if 'factor' not in root and root != project_root:
                continue
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            if re.search(r'^\s*(import|from)\s+akshare', content, flags=re.MULTILINE):
                                akshare_scripts.append(file_path)
                    except Exception as e:
                        logger.warning(f'文件扫描异常: {file_path} - {str(e)}')
        
        print('\n=== 使用akshare的脚本清单 ===')
        for script in akshare_scripts:
            print(f'• {script}')
            
        # 新增：分析脚本中使用的akshare API和字段
        api_usage = {}
        field_usage = {}
        
        for script_path in akshare_scripts:
            try:
                with open(script_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 提取API调用
                    api_calls = re.findall(r'ak\.([a-zA-Z0-9_]+)\(', content)
                    for api in api_calls:
                        if api not in api_usage:
                            api_usage[api] = []
                        api_usage[api].append(os.path.basename(script_path))
                    
                    # 提取字段引用
                    field_refs = re.findall(r'\[[\'\"](\w+)[\'\"]\]', content)
                    for field in field_refs:
                        if field not in field_usage and field not in ['动量', '支撑位', '代码']:
                            field_usage[field] = []
                        if field not in ['动量', '支撑位', '代码']:
                            field_usage[field].append(os.path.basename(script_path))
            except Exception as e:
                logger.warning(f'分析脚本异常: {script_path} - {str(e)}')
        
        # 原有获取数据逻辑
        raw_data = ak.stock_zh_a_spot()
        logger.debug(f'原始数据列名: {raw_data.columns.tolist()}')
        
        # 新增：打印API使用情况
        print("\n=== akshare API使用情况 ===")
        for api, scripts in api_usage.items():
            print(f"• {api}: 被{len(scripts)}个脚本使用 - {', '.join(scripts)}")
            
        # 新增：检查字段使用情况
        print("\n=== 字段使用情况 ===")
        available_fields = set(raw_data.columns)
        for field, scripts in field_usage.items():
            if field in available_fields:
                print(f"• {field}: 正确 - 被{len(scripts)}个脚本使用 - {', '.join(scripts)}")
            else:
                print(f"• \033[31m{field}: 错误 - 字段不存在于当前API返回中\033[0m - 被{len(scripts)}个脚本使用 - {', '.join(scripts)}")
                logger.warning(f"字段'{field}'不存在于当前API返回中，但被以下脚本使用: {', '.join(scripts)}")

        # 打印数据结构
        print("\n=== 数据结构 ===")
        raw_data.info()
        
        # 打印基本统计信息
        print("\n=== 数据统计 ===")
        print(raw_data.describe())

        # 检查必要字段
        required_columns = {'成交额', '最低', '最新价'}
        missing = required_columns - set(raw_data.columns)
        if missing:
            raise KeyError(f'缺少必要字段: {missing}')
            
        # 新增：检查所有使用的字段
        used_fields = set(field_usage.keys())
        missing_fields = used_fields - set(raw_data.columns)
        if missing_fields:
            logger.error(f'项目中使用了不存在的字段: {missing_fields}')
            print(f"\033[31m警告: 项目中使用了不存在的字段: {missing_fields}\033[0m")

        logger.info('数据校验通过')
        return True

    except Exception as e:
        logger.error(f'数据检查失败: {str(e)}')
        print(f"\033[31m错误信息: {str(e)}\033[0m")
        return False

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='股票数据检查工具')
    parser.add_argument('--symbol', type=str, default='sh000001',
                       help='股票代码，例如: sh000001（默认值）')
    args = parser.parse_args()

    # 执行检查
    success = check_data_structure(args.symbol)
    print(f"\n检查结果: {'成功' if success else '失败'}")