import logging
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def calculate_momentum_factor(data, history_data=None, window=20):
    """
    计算动量因子：N日收益率
    
    参数：
        data (DataFrame): 包含['代码']的股票数据，应该是已经筛选过的前100只股票
        history_data (dict): 以股票代码为键，历史数据DataFrame为值的字典，如果为None则自动获取
        window (int): 计算窗口，默认20天
    
    返回：
        DataFrame: 包含['代码','动量']的因子数据
    """
    try:
        logger.info('开始计算动量因子...')
        
        # 创建结果DataFrame
        result_df = pd.DataFrame()
        
        # 如果没有传入历史数据，则自动获取
        if history_data is None:
            # 获取当前日期
            end_date = datetime.now().strftime('%Y%m%d')
            # 计算开始日期（往前推60天，确保有足够的数据计算窗口）
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            
            # 创建临时历史数据字典
            history_data = {}
            
            # 遍历每只股票，获取日K数据
            for _, row in data.iterrows():
                stock_code = row['代码']
                try:
                    # 使用akshare获取个股日K数据
                    stock_data = ak.stock_zh_a_hist_tx(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
                    # 确保列名统一，如果存在"股票代码"列，将其重命名为"代码"
                    if "股票代码" in stock_data.columns and "代码" not in stock_data.columns:
                        stock_data = stock_data.rename(columns={"股票代码": "代码"})
                    history_data[stock_code] = stock_data
                except Exception as e:
                    logger.warning(f'获取股票{stock_code}的历史数据失败: {str(e)}')
                    history_data[stock_code] = pd.DataFrame()
        
        # 遍历每只股票，计算动量
        for _, row in data.iterrows():
            stock_code = row['代码']
            try:
                # 获取股票历史数据
                stock_data = history_data.get(stock_code, pd.DataFrame())
                
                if not stock_data.empty and len(stock_data) > window:
                    # 计算N日收益率作为动量
                    first_price = stock_data['收盘'].iloc[-window-1] if len(stock_data) > window else stock_data['收盘'].iloc[0]
                    last_price = stock_data['收盘'].iloc[-1]
                    momentum_value = (last_price - first_price) / first_price if first_price > 0 else 0
                    
                    # 添加到结果DataFrame
                    result_df = pd.concat([result_df, pd.DataFrame({'代码': [stock_code], '动量': [momentum_value]})], ignore_index=True)
                    logger.debug(f'成功计算{stock_code}的动量: {momentum_value}')
                else:
                    logger.warning(f'股票{stock_code}的历史数据{len(stock_data)}天，不足{window+1}天，跳过计算')
            except Exception as e:
                logger.warning(f'获取股票{stock_code}的历史数据失败: {str(e)}')
        
        logger.info(f'成功计算{len(result_df)}只股票的{window}日动量')
        return result_df

    except Exception as e:
        logger.error(f'计算动量因子失败: {str(e)}')
        raise
