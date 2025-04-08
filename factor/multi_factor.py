import logging
import pandas as pd
import akshare as ak
import os
import time
import sys
from datetime import datetime, timedelta

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.template import generate_html_table
from factor import AVAILABLE_FACTORS

logger = logging.getLogger(__name__)

def get_default_dates(start_date=None, end_date=None):
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=7*12)
    return start_date, end_date

def get_trading_days(start_date=None, end_date=None):
    start_date, end_date = get_default_dates()
    trading_days = ak.stock_zh_index_daily_em(symbol="sh000001", start_date=start_date.strftime('%Y%m%d'), end_date=end_date.strftime('%Y%m%d'))
    trading_days = pd.to_datetime(trading_days['date']).dt.strftime('%Y%m%d').tolist()
    return trading_days

def get_stock_data():
    """
    获取股票数据，集中处理数据获取逻辑
    
    返回：
        DataFrame: 包含成交额和价格数据的全量股票数据
    """
    try:
        logger.info('开始获取股票数据')
        data = ak.stock_zh_a_spot().sort_values('成交额', ascending=False)
        # 确保列名统一，如果存在"股票代码"列，将其重命名为"代码"
        if "股票代码" in data.columns and "代码" not in data.columns:
            data = data.rename(columns={"股票代码": "代码"})
        logger.info(f'成功获取股票数据，共{len(data)}条记录')
        return data
    except Exception as e:
        logger.error(f'获取股票数据失败: {str(e)}')
        raise

def get_stock_history_data(stock_codes, days=60):
    """
    集中获取多只股票的历史K线数据
    
    参数：
        stock_codes (list): 股票代码列表
        days (int): 需要获取的历史数据天数，默认60天
    
    返回：
        dict: 以股票代码为键，历史数据DataFrame为值的字典
    """
    try:
        logger.info(f'开始获取{len(stock_codes)}只股票的历史数据')
        
        # 获取当前日期
        end_date = datetime.now().strftime('%Y%m%d')
        
        # 获取交易日列表
        trading_days = get_trading_days()
        # 取最近days天的交易日
        start_date = trading_days[-min(days, len(trading_days))]
        
        # 创建结果字典
        history_data = {}
        
        # 遍历每只股票，获取日K数据
        for stock_code in stock_codes:
            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # 使用akshare获取个股日K数据
                    stock_data = ak.stock_zh_a_hist_tx(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
                    if not stock_data.empty:
                        # 添加代码列
                        stock_data['代码'] = stock_code
                        # 确保列名统一
                        column_mapping = {
                            'date': '日期',
                            'open': '开盘',
                            'close': '收盘',
                            'high': '最高',
                            'low': '最低',
                            'amount': '成交量'
                        }
                        stock_data = stock_data.rename(columns=column_mapping)
                        history_data[stock_code] = stock_data
                        logger.debug(f'成功获取{stock_code}的历史数据，共{len(stock_data)}条记录')
                        break
                    else:
                        logger.warning(f'获取股票{stock_code}的历史数据为空，尝试重试 {retry_count + 1}/{max_retries}')
                except Exception as e:
                    logger.warning(f'获取股票{stock_code}的历史数据失败: {str(e)}，尝试重试 {retry_count + 1}/{max_retries}')
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)  # 等待1秒后重试
            
            if retry_count == max_retries:
                logger.error(f'获取股票{stock_code}的历史数据失败，已达到最大重试次数')
                history_data[stock_code] = pd.DataFrame()  # 设置为空DataFrame
        
        logger.info(f'成功获取{len(history_data)}只股票的历史数据')
        return history_data
    except Exception as e:
        logger.error(f'获取股票历史数据失败: {str(e)}')
        raise

def filter_top_stocks(data, top_n=100):
    """
    筛选成交额前N的股票
    
    参数：
        data (DataFrame): 包含成交额的股票数据
        top_n (int): 筛选的股票数量，默认100只
    
    返回：
        DataFrame: 筛选后的股票数据
    """
    try:
        top_stocks = data.nlargest(top_n, '成交额').copy()
        logger.debug(f'筛选出{len(top_stocks)}条成交额前{top_n}数据')
        return top_stocks
    except Exception as e:
        logger.error(f'筛选股票数据失败: {str(e)}')
        raise

def calculate_multi_factors(data=None, selected_factors=None):
    """
    整合多个因子计算
    
    参数：
        data (DataFrame, optional): 包含成交额和价格数据的全量股票数据，如果为None则自动获取
        selected_factors (list, optional): 要计算的因子列表，如果为None则计算所有可用因子
    
    返回：
        DataFrame: 合并后的多因子数据表
    """
    try:
        logger.info('开始多因子计算')
        
        # 如果没有传入数据，则自动获取
        if data is None:
            data = get_stock_data()
        
        # 如果没有指定因子，使用所有可用因子
        if selected_factors is None:
            selected_factors = list(AVAILABLE_FACTORS.keys())
        
        # 筛选成交额前100的股票
        top_100_stocks = filter_top_stocks(data)
        logger.info(f'筛选出成交额前100的股票，共{len(top_100_stocks)}条记录')
        
        # 集中获取所有股票的历史数据
        stock_codes = top_100_stocks['代码'].tolist()
        history_data = get_stock_history_data(stock_codes, days=60)
        logger.info(f'集中获取{len(history_data)}只股票的历史数据完成')
        
        # 初始化结果DataFrame
        merged_df = None
        
        # 依次计算每个因子
        for factor_key in selected_factors:
            if factor_key not in AVAILABLE_FACTORS:
                logger.warning(f'跳过未知因子: {factor_key}')
                continue
                
            factor_info = AVAILABLE_FACTORS[factor_key]
            factor_func = factor_info['func']
            factor_name = factor_info['name']
            
            logger.debug(f'开始计算{factor_name}因子')
            factor_df = factor_func(top_100_stocks, history_data)
            
            # Debug logging
            logger.debug(f"Factor DataFrame columns: {factor_df.columns.tolist()}")
            
            # Check if factor calculation returned valid data
            if factor_df is None or factor_df.empty:
                logger.warning(f'{factor_name}因子计算返回空数据，跳过')
                continue
            
            # 合并因子数据
            if merged_df is None:
                merged_df = factor_df
            else:
                # Standardize column names
                merged_df = merged_df.rename(columns=lambda x: str(x).strip())
                factor_df = factor_df.rename(columns=lambda x: str(x).strip())
                
                # Debug logging
                logger.debug(f"Merged DataFrame columns before merge: {merged_df.columns.tolist()}")
                logger.debug(f"Factor DataFrame columns before merge: {factor_df.columns.tolist()}")
                
                # Merge on 代码 column
                merged_df = pd.merge(merged_df, factor_df, on='代码', how='inner')
            
            # Debug logging
            logger.debug(f"Merged DataFrame columns after merge: {merged_df.columns.tolist()}")
            
            # Find the actual factor column name
            factor_columns = [col for col in merged_df.columns if factor_name in col]
            if not factor_columns:
                logger.error(f'找不到{factor_name}相关的列')
                continue
                
            actual_factor_column = factor_columns[0]
            
            # 标准化处理
            score_column = f'{factor_name}评分'
            try:
                merged_df[score_column] = merged_df[actual_factor_column].rank(ascending=True) / len(merged_df)
                logger.debug(f'{factor_name}因子标准化完成')
            except Exception as e:
                logger.error(f'标准化{factor_name}因子时出错: {str(e)}')
                continue

        if merged_df is None or merged_df.empty:
            raise ValueError('没有成功计算任何因子数据')

        logger.info(f'成功合并因子数据，最终记录数：{len(merged_df)}')

        # 新增新闻数据获取
        def get_stock_news(stock_code):
            """获取单个股票的最近5条新闻标题"""
            try:
                news_df = ak.stock_news_em(symbol=stock_code)
                # 取最新5条新闻并拼接标题
                top_news = news_df.head(5)['新闻标题'].tolist()
                return " | ".join(top_news) if top_news else "无相关新闻"
            except Exception as e:
                logger.warning(f"获取{stock_code}新闻失败: {str(e)}")
                return "新闻获取异常"

        # 添加新闻列（添加延时避免频繁请求）
        merged_df['news'] = merged_df['代码'].apply(
            lambda x: get_stock_news(x)
        )
        logger.info('新闻数据添加完成')

        # 保存结果到CSV文件
        try:
            # 确保result文件夹存在
            result_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'result')
            os.makedirs(result_dir, exist_ok=True)
            
            # 生成文件名，格式为yyMMddhhmm
            current_time = datetime.now().strftime('%y%m%d%H%M')
            file_path = os.path.join(result_dir, f'{current_time}.html')  # 修改文件后缀
            
            # 保存为HTML
            try:
                html_content = generate_html_table(merged_df)  # 替换原有HTML生成代码
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logger.info(f'成功保存多因子结果到: {file_path}')
                return merged_df, file_path
            except Exception as e:
                logger.error(f'保存HTML文件失败: {str(e)}')
                return merged_df, None
        except Exception as e:
            logger.error(f'保存CSV文件失败: {str(e)}')
            return merged_df, None
        
        return merged_df, file_path
    except Exception as e:
        logger.error(f'多因子计算失败: {str(e)}')
        raise

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info('开始调试多因子计算...')
        
        # 获取数据并计算多因子
        result = calculate_multi_factors()
        logger.info(f"调试运行完成，共计算{len(result)}条记录")
        
    except Exception as e:
        logger.exception(f'调试失败: {e}')
        print(f"错误: {e}")