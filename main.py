import ast
import os
import re
from copy import deepcopy

import numpy as np
import pandas as pd
import requests
from datetime import *
import time as t
from dotenv import load_dotenv
from revChatGPT.V1 import Chatbot as ChatGPT
import akshare as ak


PROXY='http://127.0.0.1:7890'
load_dotenv(dotenv_path= '.env')

def crawl_data_from_wencai(question:str):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1',
               #   'If-Modified-Since': 'Thu, 11 Jan 2018 07:05:01 GMT',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'}

    headers_wc = deepcopy(headers)
    headers_wc["Referer"] = "http://www.iwencai.com/unifiedwap/unified-wap/result/get-stock-pick"
    headers_wc["Host"] = "www.iwencai.com"
    headers_wc["X-Requested-With"] = "XMLHttpRequest"

    Question_url = "http://www.iwencai.com/unifiedwap/unified-wap/result/get-stock-pick"
    """通过问财接口抓取数据

    Arguments:
        trade_date {[type]} -- [description]
        fields {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    payload = {
        # 查询问句
        "question": question,
        # 返回查询记录总数
        "perpage": 5000,
        "query_type": "stock"
    }

    try:
        response = requests.get(Question_url, params=payload, headers=headers_wc)

        if response.status_code == 200:
            json = response.json()
            df_data = pd.DataFrame(json["data"]["data"])
            # 规范返回的columns，去掉[xxxx]内容,并将重复的命名为.1.2...
            cols = pd.Series([re.sub(r'\[[^)]*\]', '', col) for col in pd.Series(df_data.columns)])
            for dup in cols[cols.duplicated()].unique():
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            df_data.columns=cols
            return df_data
        else:
            print("连接访问接口失败")
    except Exception as e:
        print(e)

def renderHtml(df,filename:str,title:str):
    df.index = np.arange(1, len(df) + 1)
    df.index.name='No.'
    df.reset_index(inplace=True)
    #pd.set_option('colheader_justify', 'center')
    html_string = '<html><head><title>%s</title>{style}</head><body>{table}{tablesort}</body></html>'%title
    html_string = html_string.format(
        table=df.to_html(render_links=True, escape=False, index=False),
        style='<link rel="stylesheet" type="text/css" href="static/table.css"/>',
        tablesort='<script src="static/tablesort.min.js"></script><script src="static/tablesort.number.min.js"></script><script>new Tablesort(document.getElementById("container"));</script>',
    )
    with open(filename, 'w') as f:
        f.write(html_string.replace('<table border="1" class="dataframe">','<table id="container">').replace('<th>','<th role="columnheader">'))

class Bot():
    def __init__(self):
        self.chatgptBot = None
    def chatgpt(self, queryText: str):
        reply_text,convId = None,None
        if self.chatgptBot is None:
            self.chatgptBot =ChatGPT(config={"access_token": os.environ['CHATGPT'],'proxy':PROXY})
        for data in self.chatgptBot.ask(queryText):
            convId=data['conversation_id']
            reply_text = data["message"]
        try:
            t.sleep(2)
            self.chatgptBot.delete_conversation(convId)
        except:
            pass
        return reply_text


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    wencaiPrompt = '上市交易日天数>90，近30日振幅≥20%，总市值<1000亿'
    wdf = crawl_data_from_wencai(wencaiPrompt)
    wdf['区间成交额']=pd.to_numeric(wdf['区间成交额'], errors='coerce')
    wdf=wdf.sort_values('区间成交额',ascending=False)[:20]
    # wdf.to_csv('wencai_o.csv')
    # exit()
    wdf.set_index('股票代码',inplace=True)
    bot=Bot()
    for k,v in wdf.iterrows():
        symbol=k.split('.')[0]
        wdf.at[k,'stock']='<a href="https://xueqiu.com/S/%s">%s<br>%s</a>'%(k[-2:]+symbol,k[-2:]+symbol,v['股票简称'])
        news=ak.stock_news_em(symbol)
        news.drop_duplicates(subset='新闻标题',inplace=True)
        news['发布时间']=pd.to_datetime(news['发布时间'])
        news['新闻标题']=news['发布时间'].dt.strftime('%Y-%m-%d ')+news['新闻标题'].str.replace('%s：'%v['股票简称'],'')
        news = news[~news['新闻标题'].str.contains('股|主力|机构|资金流')]
        news['news']=news['新闻标题'].str.cat(news['新闻内容'].str.split('。').str[0], sep=' ')
        news = news[news['news'].str.contains(v['股票简称'])]
        news.sort_values(by=['发布时间'],ascending=False,inplace=True)
        # news=news[news['发布时间']> datetime.now() - timedelta(days=30)]

        if len(news)<2:
            continue
        newsTitles='\n'.join(news['新闻标题'][:20])[:1200]

        # stock_main_stock_holder_df = ak.stock_main_stock_holder(stock=symbol)
        # holders = ','.join(stock_main_stock_holder_df['股东名称'][:10].tolist())

        prompt="{'%s相关资讯':'''%s''',\n}\n请分析总结机会点和风险点，输出格式为{'机会':[机会点],\n'风险':[风险点],\n'题材标签':[标签1,标签2,标签3...]}"%(v['股票简称'],newsTitles)
        print('Prompt:\n%s'%prompt)
        retry=2
        while retry>0:
            try:
                replyTxt = bot.chatgpt(prompt)
                print('ChatGPT:\n%s'%replyTxt)
                match = re.findall(r'{[^{}]*}', replyTxt)
                content = match[-1]
                parsed = ast.literal_eval(content)
                if isinstance(parsed['机会'], list):
                    chances = '\n'.join( '%s. %s'%(x+1,parsed['机会'][x]) for x in range(len(parsed['机会'])))
                else:
                    chances = parsed['机会']
                if isinstance(parsed['风险'], list):
                    risks = '\n'.join('%s. %s'%(x+1,parsed['风险'][x]) for x in range(len(parsed['风险'])))
                else:
                    risks = parsed['风险']
                wdf.at[k, 'chance'] = chances.replace(v['股票简称'], '').replace('\n', '<br>')
                wdf.at[k, 'risk'] = risks.replace(v['股票简称'], '').replace('\n', '<br>')
                if '\n' in risks:
                    wdf.at[k, 'score'] = len(chances) - len(risks)
                wdf.at[k,'tags'] = '<br>'.join(parsed['题材标签'])
                break
            except Exception as e:
                print(e)
                retry-=1
                prompt+='，请务必保持python dict格式'
                t.sleep(20)
                continue
        t.sleep(20)
    wdf.dropna(subset=['score'],inplace=True)
    wdf.sort_values(by=['score'],ascending=False,inplace=True)
    wdf.to_csv('wencai.csv')
    wdf=wdf[['stock','chance','risk','tags','score']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(wdf,nowTxt+'.html',nowTxt)