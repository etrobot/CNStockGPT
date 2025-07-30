import re,os
import json
import pandas as pd
import requests
import akshare as ak
import time as t

HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'script',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

def parse_jsonp(jsonp_str):
    if not jsonp_str or not isinstance(jsonp_str, str):
        print("错误：输入不是有效的字符串")
        return None
    print("\n原始响应前100个字符:", jsonp_str[:100])
    match = re.match(r'^[a-zA-Z0-9_]+\s*\(\s*(.*)\s*\)\s*;?\s*$', jsonp_str.strip(), re.DOTALL)
    if match:
        json_str = match.group(1)
        data = json.loads(json_str)
        print("JSON解析成功！")
        return data
    else:
        return json.loads(jsonp_str)


def getChanges(concept_df: pd.DataFrame):
    risingConceptsCodes = getRisingConcepts()
    # 先取出在risingConceptsCodes中的部分，按顺序排列
    ordered_df = concept_df[concept_df['板块代码'].isin(risingConceptsCodes)].set_index('板块代码').loc[risingConceptsCodes].reset_index()
    # 剩下的部分
    rest_df = concept_df[~concept_df['板块代码'].isin(risingConceptsCodes)]
    # 拼接
    changedConcepts_df = pd.concat([ordered_df, rest_df], ignore_index=True)
    
    response = requests.get(
        'https://push2ex.eastmoney.com/getAllStockChanges?type=8201,8202,8193,4,32,64,8207,8209,8211,8213,8215,8204,8203,8194,8,16,128,8208,8210,8212,8214,8216&cb=jQuery35108409427522251944_1753773534498&ut=7eea3edcaed734bea9cbfc24409ed989&pageindex=0&pagesize=1000&dpt=wzchanges&_=1753773534514',
        headers={**HEADERS, 'Referer': 'https://quote.eastmoney.com/changes/'},
    )
    
    # 解析JSONP响应
    data = parse_jsonp(response.text)
    
    if data and 'data' in data and 'allstock' in data['data']:
        # 转换为DataFrame
        df = pd.DataFrame(data['data']['allstock'])
        
        # 重命名列名，使其更易读
        column_mapping = {
            'c': '股票代码',
            'n': '股票名称',
            'tm': '时间',
            'm': '市场',
            't': '类型',
            'i': '信息'
        }
        df = df.rename(columns=column_mapping)
        
        # 解析info字段（包含涨跌幅、最新价、涨跌额）
        if '信息' in df.columns:
            info_df = df['信息'].str.split(',', expand=True)
            if len(info_df.columns) >= 3:
                # 清理并转换数据
                info_df[0] = pd.to_numeric(info_df[0], errors='coerce')

                if not df.empty:
                    df['涨跌幅'] = info_df[0]
                
            df = df[(df['涨跌幅'] < 1) & ((df['涨跌幅'] >= 0.05) | (df['涨跌幅'] <= -0.05))]
        
        # 过滤掉负面类型
        negative_types = ['8194', '8', '128', '8208', '8210', '8212', '8214', '8216', '8203', '99', '106']  # 包含大笔卖出、封跌停板、竞价下跌、向下缺口、60日新低、60日大幅下跌、高台跳水、加速下跌等
        df = df[~df['类型'].astype(str).isin(negative_types)]
        
        # 转换为指定的输出格式
        def format_time(tm):
            """将时间戳格式化为 HH:MM"""
            tm_str = str(tm)
            if len(tm_str) < 6:
                tm_str = tm_str.zfill(6)
            return f"{tm_str[:2]}:{tm_str[2:4]}"  # 只返回小时和分钟
        
        # 创建新的DataFrame用于输出
        output_df = pd.DataFrame()
        output_df['股票代码'] = df['股票代码']
        output_df['时间'] = df['时间'].apply(format_time)
        output_df['名称'] = df['股票名称']
        # 直接放原始类型和涨跌幅，便于调试
        output_df['原始类型'] = df['类型']
        output_df['原始涨跌幅'] = df['涨跌幅']
        
        # 类型映射字典
        type_mapping = {
            '8201': '火箭发射',
            '8202': '快速反弹',
            '8193': '大笔买入',
            '4': '封涨停板',
            '32': '打开跌停板',
            '64': '有大买盘',
            '8207': '竞价上涨',
            '8209': '高开5日线',
            '8211': '向上缺口',
            '8213': '60日新高',
            '8215': '60日大幅上涨',
            '8204': '加速下跌',
            '8203': '高台跳水',
            '8194': '大笔卖出',
            '8': '封跌停板',
            '16': '打开涨停板',
            '128': '有大卖盘',
            '8208': '竞价下跌',
            '8210': '低开5日线',
            '8212': '向下缺口',
            '8214': '60日新低',
            '8216': '60日大幅下跌'
        }
        # 增加映射后的类型和相关信息，便于对比
        output_df['类型'] = df['类型'].astype(str).map(type_mapping).fillna('未知类型')
        # 先加一列'四舍五入取整'，在加百分号前处理
        output_df['四舍五入取整'] = df['涨跌幅'].apply(lambda x: int(round(x * 100)) if pd.notnull(x) else None)
        output_df['相关信息'] = df['涨跌幅'].apply(
            lambda x: f"%+.2f" % (x * 100) + "%" if pd.notnull(x) else 'NaN'
        )
        

        first_concept_df = changedConcepts_df.drop_duplicates(subset=['股票代码'], keep='first')
        output_df = pd.merge(output_df, first_concept_df[['股票代码', '板块名称']], on='股票代码', how='left')

        output_df = output_df.sort_values('时间')       

        # 先排序再加上午/下午列
        html_df = output_df[['板块名称', '时间', '名称', '相关信息','类型', '四舍五入取整']].copy()
        def am_pm_col(tm):
            hour = int(tm[:2])
            return '上午' if hour < 12 else '下午'
        html_df['上下午'] = html_df['时间'].apply(am_pm_col)
        # 新增一列用于排序，转为分钟数
        html_df['时间排序'] = html_df['时间'].apply(lambda tm: int(tm[:2])*60 + int(tm[3:5]))
        html_df = html_df.sort_values(['上下午','板块名称', '时间排序'])
        # 确保 static 目录存在
        os.makedirs('static', exist_ok=True)
        # 保存到 static 目录下
        # 保存为csv，追加且去重（股票代码+时间+四舍五入取整）
        csv_file = 'static/changes.csv'
        if os.path.exists(csv_file):
            old_df = pd.read_csv(csv_file)
            combined = pd.concat([old_df, html_df], ignore_index=True)
            # 去重，保留最新一条
            combined = combined.drop_duplicates(subset=['名称', '类型'], keep='last')
            combined.to_csv(csv_file, index=False, encoding='utf-8')
        else:
            html_df.drop_duplicates(subset=['名称', '类型'], keep='last').to_csv(csv_file, index=False, encoding='utf-8')
        print(f"已保存到{csv_file}，当前总行数：", pd.read_csv(csv_file).shape[0])


def getRisingConcepts():
    
    url = "https://79.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "200",
        "po": "1",  # 按涨跌幅排序，1为降序
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",  # 按涨跌幅排序
        "fs": "m:90 t:3 f:!50",
        "fields": "f3,f12,f14,f20",
        "_": "1626075887768",
    }
    response=requests.get(url=url,params=params,headers={**HEADERS, 'Referer': 'https://quote.eastmoney.com/center/gridlist.html'})
    data = parse_jsonp(response.text)['data']['diff']
    bkcodes = [ x['f12'] for x in data if int(x['f20'])<5000000000000 and not '昨日' in x['f14']]
    return bkcodes

def getConcepts():
    concepts=[['板块代码','板块名称','股票代码','股票名称']]
    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    stock_board_concept_name_em_df.sort_values(by='总市值',ascending=True,inplace=True)
    for k,v in stock_board_concept_name_em_df.iterrows():
        if int(v['总市值'])>30000000000000 or '昨日' in v['板块名称']:
            continue
        stock_board_concept_spot_em_df = ak.stock_board_concept_cons_em(symbol=v['板块代码'])
        for k2,v2 in stock_board_concept_spot_em_df.iterrows():
            row = [v['板块代码'],v['板块名称'],v2['代码'],v2['名称']]
            concepts.append(row)
        print(v['板块名称'],len(concepts))
        if len(concepts)%20==0:
            t.sleep(15)
    df = pd.DataFrame(concepts,columns=['板块代码','板块名称','股票代码','股票名称'])
    df.to_csv('static/concepts.csv',index=False)

def watch():
    if not os.path.exists('static/concepts.csv'):
        getConcepts()
    concept_df = pd.read_csv('static/concepts.csv')
    while True:
        now = t.localtime()
        now_minutes = now.tm_hour * 60 + now.tm_min
        # 定义允许的时间段（分钟）
        allowed_periods = [
            (9*60+30, 11*60+30),   # 上午9:30-11:30
            (13*60, 15*60)         # 下午13:00-15:00
        ]
        in_period = any(start <= now_minutes <= end for start, end in allowed_periods)
        if in_period:
            getChanges(concept_df)
        t.sleep(2)

def main():
    watch()

if __name__ == '__main__':
    main()
