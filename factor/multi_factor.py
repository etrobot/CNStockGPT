import logging
import pandas as pd
import akshare as ak
import os
from datetime import datetime, timedelta
from factor.support_factor import calculate_support_factor  # Changed from relative to absolute import
from factor.momentum_factor import calculate_momentum_factor  # Changed from relative to absolute import
from utils.template import generate_html_table  # 新增导入

logger = logging.getLogger(__name__)

def get_stock_data():
    """
    获取股票数据，集中处理数据获取逻辑
    
    返回：
        DataFrame: 包含成交额和价格数据的全量股票数据
    """
    try:
        logger.info('开始获取股票数据')
        data = ak.stock_zh_a_spot().sort_values('成交额', ascending=False)
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
        # 计算开始日期（往前推指定天数，确保有足够的数据计算窗口）
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        # 创建结果字典
        history_data = {}
        
        # 遍历每只股票，获取日K数据
        for stock_code in stock_codes:
            try:
                # 使用akshare获取个股日K数据
                stock_data = ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
                # 确保列名统一，如果存在"股票代码"列，将其重命名为"代码"
                if "股票代码" in stock_data.columns and "代码" not in stock_data.columns:
                    stock_data = stock_data.rename(columns={"股票代码": "代码"})
                history_data[stock_code] = stock_data
                logger.debug(f'成功获取{stock_code}的历史数据，共{len(stock_data)}条记录')
            except Exception as e:
                logger.warning(f'获取股票{stock_code}的历史数据失败: {str(e)}')
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

def calculate_multi_factors(data=None):
    """
    整合多个因子计算
    
    参数：
        data (DataFrame, optional): 包含成交额和价格数据的全量股票数据，如果为None则自动获取
    
    返回：
        DataFrame: 合并后的多因子数据表
    """
    try:
        logger.info('开始多因子计算')
        
        # 如果没有传入数据，则自动获取
        if data is None:
            data = get_stock_data()
        
        # 筛选成交额前100的股票
        top_100_stocks = filter_top_stocks(data)
        logger.info(f'筛选出成交额前100的股票，共{len(top_100_stocks)}条记录')
        
        # 集中获取所有股票的历史数据
        stock_codes = top_100_stocks['代码'].tolist()
        history_data = get_stock_history_data(stock_codes, days=60)  # 获取60天的历史数据，足够计算各种因子
        logger.info(f'集中获取{len(history_data)}只股票的历史数据完成')
        
        # 计算支撑因子
        support_df = calculate_support_factor(top_100_stocks, history_data)
        logger.debug('支撑因子计算完成')

        # 计算动量因子
        momentum_df = calculate_momentum_factor(top_100_stocks, history_data)
        logger.debug('动量因子计算完成')

        # 合并因子数据
        merged_df = pd.merge(support_df, momentum_df, on='代码', how='inner')
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
        print('\n多因子计算结果:\n', result.head())
        logger.info(f"调试运行完成，共计算{len(result)}条记录")
        
    except Exception as e:
        logger.exception(f'调试失败: {e}')
        print(f"错误: {e}")