import ast
import json
import os
import re
from urllib import request,parse

import feedparser
import numpy as np
import pandas as pd
from datetime import *
import time as t

import requests
from akshare.utils import demjson
from dotenv import load_dotenv
from pocketbase import PocketBase
from revChatGPT.V1 import Chatbot as ChatGPT
from concurrent.futures import ThreadPoolExecutor, as_completed,wait


PROXY='http://127.0.0.1:7890'
load_dotenv(dotenv_path= '.env')

def getActive():
    params = {
        'formatted': 'false',
        'lang': 'en-US',
        'region': 'US',
        'scrIds': 'most_actives',
        'count': '250',
    }
    response = requests.get(
        'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved',
        params=params,
        headers={'user-agent': 'Mozilla'},
        proxies={'https':'http://127.0.0.1:7890'}
    )
    df=pd.DataFrame(response.json()['finance']['result'][0]['quotes'])
    return df

def get_yf_rss(ticker):
    yf_rss_url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%s&region=US&lang=en-US&count=100'
    proxy_support = request.ProxyHandler({ 'https': PROXY})
    opener = request.build_opener(proxy_support)
    request.install_opener(opener)
    feed = feedparser.parse(yf_rss_url % ticker)
    df = pd.json_normalize(feed.entries)
    df['published'] = pd.to_datetime(df["published"],format='mixed').dt.tz_convert('Asia/Shanghai')
    return df

def getUrl(url,cookie=''):
    retryTimes = 0
    while retryTimes < 99:
        try:
            response = requests.get(url,headers={"user-agent": "Mozilla", "cookie": cookie,"Connection":"close"},timeout=5)
            return response.text
        except Exception as e:
            print(e.args)
            print('retrying.....')
            t.sleep(60)
            retryTimes += 1
            continue

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
        if convId is not None:
            try:
                print(convId)
                t.sleep(2)
                self.chatgptBot.delete_conversation(convId)
            except Exception as e:
                print(e)
                pass
        return reply_text

def getK(symbol,period='week'):
    k=tencentK('us',symbol, period)[-61:]
    k1th = k['close'].values[0]
    w=10000
    return [[
        int(x.strftime('%y%m%d')),
        round(y['open'] / k1th*w-w), round(y['close'] / k1th*w - w), round(y['high'] / k1th*w - w), round(y['low'] / k1th*w - w)] for x, y in
     k[-60:].iterrows()]

def tencentK(mkt:str = 'us',symbol: str = "QQQ",period='week') -> pd.DataFrame:
    # symbol=symbol.lower()
    # A股的mkt为''
    if mkt=='us' and '.' not in symbol:
        symbolTxt=requests.get(f"http://smartbox.gtimg.cn/s3/?q={symbol}&t=us").text
        symbol = mkt + symbolTxt.split("~")[1].upper()
    elif mkt=='hk':
        symbol=mkt+symbol
    """
        腾讯证券-获取有股票数据的第一天, 注意这个数据是腾讯证券的历史数据第一天
        http://gu.qq.com/usQQQ.OQ/
        :param symbol: 带市场标识的股票代码
        :type symbol: str
        :return: 开始日期
        :rtype: pandas.DataFrame
        """
    headers = {"user-agent": "Mozilla", "Connection": "close"}
    url = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
    if mkt=='us':
        url = "https://web.ifzq.gtimg.cn/appstock/app/usfqkline/get?"
    temp_df = pd.DataFrame()
    url_list=[]
    params = {
        "_var": f"kline_{period}qfq",
        "param": f"{symbol},{period},,,320,qfq",
        "r": "0.012820108110342066",
    }
    url_list.append(url + parse.urlencode(params))
    # print(url_list)
    with ThreadPoolExecutor(max_workers=10) as executor:  # optimally defined number of threads
        responeses = [executor.submit(getUrl, url) for url in url_list]
        wait(responeses)

    for res in responeses:
        text=res.result()
        try:
            inner_temp_df = pd.DataFrame(
                demjson.decode(text[text.find("={") + 1:])["data"][symbol][period]
            )
        except:
            inner_temp_df = pd.DataFrame(
                demjson.decode(text[text.find("={") + 1:])["data"][symbol]["qfq%s"%period]
            )
        temp_df = pd.concat([temp_df, inner_temp_df],ignore_index=True)

    if temp_df.shape[1] == 6:
        temp_df.columns = ["date", "open", "close", "high", "low", "amount"]
    else:
        temp_df = temp_df.iloc[:, :6]
        temp_df.columns = ["date", "open", "close", "high", "low", "amount"]
    temp_df.index = pd.to_datetime(temp_df["date"])
    del temp_df["date"]
    temp_df = temp_df.astype("float")
    temp_df.drop_duplicates(inplace=True)
    temp_df.rename(columns={'amount':'volume'}, inplace = True)
    # temp_df.to_csv('Quotation/'+symbol+'.csv',encoding='utf-8',index_label='date',date_format='%Y-%m-%d')
    return temp_df

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    ydf = getActive()
    ydf.to_csv('ydf.csv')
    ydf['marketCap'] = round(pd.to_numeric(ydf['marketCap'], errors='coerce') / 100000000, 2)
    capsizes = {"Small": 10, "Middle": 100, "Large": 1000, "Mega": 2000}
    if 'PB' in os.environ.keys():
        client = PocketBase(os.environ['PB'])
        admin_data = client.admins.auth_with_password(os.environ['PBNAME'], os.environ['PBPWD'])
        pbDf = pd.DataFrame([[x.id, x.symbol, x.updated] for x in client.collection("stocks01").get_list(per_page=120,
                                                                                                         query_params={
                                                                                                             "filter": 'market="US"'}).items],
                            columns=['id', 'symbol', 'updated'])
        pbDf.drop_duplicates(subset=['symbol'],inplace=True)
        pbDf.set_index('symbol', inplace=True)
    for k,v in ydf.iterrows():
        symbol=v['symbol']
        if 'PB' in os.environ.keys():
            if symbol in pbDf.index:
                if pbDf.at[symbol, 'updated'].date() == datetime.now().date():
                    continue
        ydf.at[k,'stock']='<a href="https://xueqiu.com/S/%s">%s<br>%s</a>'%(symbol,symbol,v['displayName'])
        try:
            news=get_yf_rss(symbol)
        except:
            continue
        news['summary']=news['published'].dt.strftime('%Y-%m-%d ')+news['summary']
        news.to_csv(symbol+'.csv')
        if len(news)<2:
            continue
        newsTitles='\n'.join(news['summary'].values)[:2900]+'...'

        prompt="{'%s(%s)相关资讯':'''%s''',\n}\n请根据资讯分析总结风险点和机会点，输出中文回答，回答格式为：{'chances':[机会点],'risks':[风险点],'tags':[题材标签]}"%(v['symbol'],v['longName'],newsTitles)
        print('Prompt:\n%s'%prompt)
        retry=2
        while retry>0:
            try:
                bot = Bot()
                replyTxt = bot.chatgpt(prompt)
                print('ChatGPT:\n%s'%replyTxt)
                match = re.findall(r'{[^{}]*}', replyTxt)
                content = match[-1]
                parsed = ast.literal_eval(content)
                if isinstance(parsed['chances'], list):
                    chances = '\n'.join( '%s. %s'%(x+1,parsed['chances'][x]) for x in range(len(parsed['chances'])))
                else:
                    chances = parsed['chances']
                if isinstance(parsed['risks'], list):
                    risks = '\n'.join('%s. %s'%(x+1,parsed['risks'][x]) for x in range(len(parsed['risks'])))
                else:
                    risks = parsed['risks']
                ydf.at[k, 'chances'] = chances.replace('\n', '<br>')
                ydf.at[k, 'risks'] = risks.replace('\n', '<br>')
                if '\n' in risks:
                    ydf.at[k, 'score'] = len(chances) - len(risks)
                    if 'PB' in os.environ.keys():
                        capsize = "Tiny"
                        for kk, vv in capsizes.items():
                            if v['marketCap'] > vv:
                                capsize = kk
                        uploadjson = {
                            "market": "US",
                            "symbol": symbol,
                            "name": v['displayName'],
                            "cap": str(v['marketCap']) + '亿',
                            "capsize": capsize,
                            "chances": chances,
                            "week": json.dumps({"k": getK(symbol, 'week')}),
                            "month": json.dumps({"k": getK(symbol, 'month')}),
                            "risks": risks,
                            "tags": ','.join(parsed['tags']),
                            "score": len(chances) - len(risks)
                        }
                        print(uploadjson)
                        if symbol in pbDf.index:
                            print(symbol,pbDf.at[symbol, 'id'])
                            print(client.collection("stocks01").update(pbDf.at[symbol, 'id'], uploadjson))
                        else:
                            print('create'+symbol)
                            print(client.collection("stocks01").create(uploadjson))
                ydf.at[k,'tags'] = '<br>'.join(parsed['tags'])
                break
            except Exception as e:
                print(e)
                retry-=1
                prompt+='，请务必保持python dict格式'
                t.sleep(20)
                continue
        t.sleep(30)
    ydf.dropna(subset=['score'],inplace=True)
    ydf.sort_values(by=['score'],ascending=False,inplace=True)
    ydf.to_csv('yahooFinance.csv')
    ydf_w=ydf[['stock','chances','risks','tags','score']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(ydf_w,nowTxt+'_us.html',nowTxt)
    ydf_json=ydf[['symbol','chances','risks','tags','score']]
    with open(nowTxt + '.json', 'w', encoding='utf_8_sig') as f:
        json.dump({'columns':ydf_json.columns.tolist(),'data':ydf_json.values.tolist()}, f, ensure_ascii=False, indent=4)