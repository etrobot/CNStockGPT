import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def calculate_momentum_factor(data, history_data=None, window=50):
    """
    计算动量因子：(累计高收差-累计缺口)/首日价格
    """
    try:
        logger.info(f'开始计算动量因子，共{len(data)}只股票...')
        
        if data.empty:
            logger.error('输入的股票数据为空')
            return pd.DataFrame()
            
        if not history_data:
            logger.error('历史数据字典为空')
            return pd.DataFrame()

        # 创建结果DataFrame
        result_data = []
        processed_count = 0
        error_count = 0

        # 遍历每只股票，计算动量
        for _, row in data.iterrows():
            stock_code = row['代码']
            
            # 获取股票历史数据
            stock_data = history_data.get(stock_code)
            
            if stock_data is None or stock_data.empty:
                logger.warning(f'股票{stock_code}没有历史数据')
                error_count += 1
                continue

            try:
                if len(stock_data) <= window:
                    logger.warning(f'股票{stock_code}的历史数据不足{window}天 (实际: {len(stock_data)}天)')
                    error_count += 1
                    continue

                # 获取窗口内数据
                window_data = stock_data.tail(window)

                # 计算高收差的累计值
                high_close_diff = (window_data['最高'] - window_data['收盘']).sum()
                
                # 计算缺口大小
                gaps = window_data['开盘'].shift(-1) - window_data['收盘']
                gaps = gaps.abs().sum()

                # 获取首日收盘价
                first_price = window_data['收盘'].iloc[0]

                if first_price <= 0:
                    logger.warning(f'股票{stock_code}的首日价格异常: {first_price}')
                    error_count += 1
                    continue

                # 计算动量值
                momentum_value = (high_close_diff - gaps) / first_price
                
                # 添加到结果列表
                result_data.append({
                    '代码': stock_code,
                    '动量': momentum_value,
                    '股票名称': row.get('名称', '')  # 如果原始数据中有名称列就保留
                })
                
                processed_count += 1
                
            except Exception as e:
                logger.warning(f'计算股票{stock_code}的动量因子失败: {str(e)}')
                error_count += 1

        # 转换为DataFrame
        result_df = pd.DataFrame(result_data)
        
        # 输出详细的统计信息
        logger.info(f'''动量因子计算完成:
        - 总股票数: {len(data)}
        - 成功计算: {processed_count}
        - 计算失败: {error_count}
        ''')

        if result_df.empty:
            logger.error('没有成功计算出任何股票的动量因子')
            return pd.DataFrame()

        # 处理异常值
        result_df['动量'] = result_df['动量'].replace([np.inf, -np.inf], np.nan)
        result_df = result_df.dropna(subset=['动量'])

        logger.info(f'最终得到{len(result_df)}只股票的有效动量因子')
        return result_df

    except Exception as e:
        logger.error(f'动量因子计算过程发生错误: {str(e)}')
        return pd.DataFrame()
