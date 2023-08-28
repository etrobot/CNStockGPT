import ast
import json
import os
import re
from copy import deepcopy
from urllib import parse
import numpy as np
import pandas as pd
import requests
from datetime import *
import time as t
from dotenv import load_dotenv
from revChatGPT.V1 import Chatbot as ChatGPT
# import poe
import openai
import akshare as ak

from pocketbase import PocketBase

PROXY='http://127.0.0.1:7890'
load_dotenv(dotenv_path= '.env')
openai.api_key = os.environ['OPENAI']

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
def cmsK(code:str,type:str='daily'):
    """招商证券A股行情数据"""
    typeNum={'daily':1,'monthly':3,'weekly':2}
    code=code.upper()
    quoFile = 'Quotation/' + code + '.csv'
    if len(code)==8:
        code = code[:2] + ':'+code[2:]
    params = (
        ('funcno', 20050),
        ('version', '1'),
        ('stock_list', code),
        ('count', '10000'),
        ('type', typeNum[type]),
        ('field', '1:2:3:4:5:6:7:8:9:10:11:12:13:14:15:16:18:19'),
        ('date', datetime.now().strftime("%Y%m%d")),
        ('FQType', '2'),#不复权1，前复权2，后复权3
    )
    url='https://hq.cmschina.com/market/json?'+parse.urlencode(params)
    kjson=json.loads(getUrl(url))
    if 'results' not in kjson.keys() or  len(kjson['results'])==0:
        return []
    data = kjson['results'][0]['array']
    df=pd.DataFrame(data=data,columns=['date','open','high','close','low','yesterday','volume','amount','price_chg','percent','turnoverrate','ma5','ma10','ma20','ma30','ma60','afterAmt','afterVol'])
    df.set_index('date',inplace=True)
    df.index=pd.to_datetime(df.index,format='%Y%m%d')
    df=df.apply(pd.to_numeric, errors='coerce').fillna(df)
    if type=='daily':
        df.to_csv(quoFile,encoding='utf-8',index_label='date',date_format='%Y-%m-%d')
    df['percent']=df['percent'].round(4)
    return df

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

def tencentNews(symbol:str):
    params = {
        'page': '1',
        'symbol': symbol,
        'n': '51',
        '_var': 'finance_notice',
        'type': '2',
        # '_': '1690699699389',
    }

    response = requests.get(
        'https://proxy.finance.qq.com/ifzqgtimg/appstock/news/info/search',
        params=params,
        headers={"user-agent": "Mozilla","Connection":"close"}
    )
    df = pd.DataFrame(json.loads(response.text[len('finance_notice='):])['data']['data'])
    df.to_csv('tNews.csv')
    return df

def gpt(symbol:str,name:str):
    print(symbol,name)
    try:
        news = tencentNews(symbol.lower())
        news.drop_duplicates(subset='title', inplace=True)
        news['time'] = pd.to_datetime(news['time'])
        news['title'] = news['time'].dt.strftime('%Y-%m-%d ') + news['title'].str.replace('%s：' % name, '')
        news = news[~news['title'].str.contains('股|主力|机构|资金流|家公司')]
    except Exception as e:
        print(e)
        return
    if len(news) < 2:
        return
    news.sort_values(by=['time'], ascending=False, inplace=True)
    # news=news[news['发布时间']> datetime.now() - timedelta(days=30)]
    newsTitles = '\n'.join(news['title'][:30])[:1800]

    # stock_main_stock_holder_df = ak.stock_main_stock_holder(stock=symbol)
    # holders = ','.join(stock_main_stock_holder_df['股东名称'][:10].tolist())
    prompt = "{'当前日期':%s,'%s最新相关资讯':'''%s''',\n}\n请分析总结机会点和风险点，输出格式为dict:{\n'机会':'''\n1...\n2...\n...''',\n'风险':'''\n1...\n2...\n...''',\n'题材标签':[标签]\n}" % (datetime.now().strftime('%Y-%m-%d'),
    name, newsTitles)

    print('Prompt:\n%s' % prompt)
    retry = 2
    while retry > 0:
        try:
            bot = Bot()
            replyTxt = bot.chatgpt(prompt)
            # replyTxt = ""
            # for t in bot.send_message("a2_2",prompt):
            #     replyTxt = t['text']
            # completion = openai.ChatCompletion.create(model="gpt-3.5-turbo",
            #                                           messages=[{"role": "user", "content": prompt}])
            # replyTxt = completion.choices[0].message.content
            print('ChatGPT:\n%s' % replyTxt)
            match = re.findall(r'{[^{}]*}', replyTxt)
            content = match[-1]
            parsed = ast.literal_eval(content)
            if isinstance(parsed['机会'], list):
                chances = '\n'.join('%s. %s' % (x + 1, parsed['机会'][x]) for x in range(len(parsed['机会'])))
            else:
                chances = parsed['机会']
            if isinstance(parsed['风险'], list):
                risks = '\n'.join('%s. %s' % (x + 1, parsed['风险'][x]) for x in range(len(parsed['风险'])))
            else:
                risks = parsed['风险']
            return {'chances':chances,'risks':risks,'tags':parsed['题材标签']}
            break
        except Exception as e:
            print(e)
            retry -= 1
            prompt += '，请务必保持python dict格式'
            t.sleep(20)
            continue

def getK(symbol,period='weekly'):
    k=cmsK(symbol, period)[-61:]
    k1th = k['close'].values[0]
    w=10000
    return [[
        int(x.strftime('%y%m%d')),
        round(y['open'] / k1th*w-w), round(y['close'] / k1th*w - w), round(y['high'] / k1th*w - w), round(y['low'] / k1th*w - w)] for x, y in
     k[-60:].iterrows()]

def analyze():
    wdf = ak.stock_zh_a_spot_em()
    wdf = wdf.sort_values(by=['总市值'],ascending=False)
    print(wdf.columns)
    wdf.to_csv('wencai_o.csv')
    wdf['总市值'] = round(pd.to_numeric(wdf['总市值'], errors='coerce')/100000000,2)
    capsizes={"Small":10,"Middle":100,"Large":1000,"Mega":2000}
    capsizesCount = {"Tiny": 0,"Small": 0, "Middle": 0, "Large": 0, "Mega": 0}
    wdf.set_index('代码',inplace=True)
    if 'PB' in os.environ.keys():
        client = PocketBase(os.environ['PB'])
        client.admins.auth_with_password(os.environ['PBNAME'], os.environ['PBPWD'])
        pbDf = pd.DataFrame([[x.id, x.symbol,x.updated] for x in client.collection("stocks01").get_list(per_page=120,
                                                                                              query_params={
                                                                                                  "filter": 'market="CN"'}).items],
                            columns=['id', 'symbol', 'updated'])
        pbDf.drop_duplicates(subset=['symbol'], inplace=True)
        pbDf.set_index('symbol',inplace=True)
    for k,v in wdf.iterrows():
        if k.startswith('6'):
            symbol = 'SH'+k
        else:
            symbol = 'SZ'+k
        capsize = "Tiny"
        for kk, vv in capsizes.items():
            if v['总市值'] > vv:
                capsize = kk
        if capsizesCount[capsize] > 10:
            continue
        if 'PB' in os.environ.keys():
            if k in pbDf.index:
                print(capsizesCount)
                if pbDf.at[k, 'updated'].date() == datetime.now().date():
                    capsizesCount[capsize] = capsizesCount[capsize] + 1
                    continue
        wdf.at[k,'symbol']=symbol
        wdf.at[k,'stock']='<a href="https://xueqiu.com/S/%s">%s<br>%s</a>'%(symbol,symbol,v['名称'])
        analysis = gpt(symbol,v['名称'])
        if analysis is not None:
            chances=analysis['chances'].replace(v['名称'], '')
            wdf.at[k, 'chances'] = chances
            risks=analysis['risks'].replace(v['名称'], '')
            wdf.at[k, 'chanceNum'] =  len(analysis['chances'])
            wdf.at[k, 'risks'] = risks
            wdf.at[k, 'riskNum'] = len(risks)
            tags=','.join(analysis['tags'])
            wdf.at[k, 'tags'] = tags
            if '\n' in analysis['risks']:
                wdf.at[k, 'score'] = len(chances) - len(risks)
                if 'PB' in os.environ.keys():
                    uploadjson={
                        "market":"CN",
                        "symbol":k,
                        # "name":v['股票简称'],
                        "name": v['名称'],
                        "cap":str(v['总市值'])+'亿',
                        "capsize":capsize,
                        "chances":chances,
                        "week":json.dumps({"k":getK(symbol,'weekly')}),
                        "month": json.dumps({"k":getK(symbol,'monthly')}),
                        "risks":risks,
                        "tags":tags,
                        "score":len(chances) - len(risks)
                    }
                    print(uploadjson)
                    if k in pbDf.index:
                        print(k, pbDf.at[k, 'id'])
                        print(client.collection("stocks01").update(pbDf.at[k,'id'],uploadjson))
                    else:
                        print('create' + k)
                        print(client.collection("stocks01").create(uploadjson))
            capsizesCount[capsize] = capsizesCount[capsize] + 1
            t.sleep(20)
    if 'score' not in wdf.columns:
        return
    wdf.dropna(subset=['score'],inplace=True)
    wdf.sort_values(by=['score'],ascending=False,inplace=True)
    wdf.to_csv('wencai.csv')
    wdf_w=wdf[['stock','chances','risks','tags','score']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(wdf_w,nowTxt+'.html',nowTxt)
    wdf_json=wdf[['symbol','chanceNum','chances','riskNum','risks','tags','score']]
    with open(nowTxt + '.json', 'w', encoding='utf_8_sig') as f:
        json.dump({'columns':wdf_json.columns.tolist(),'data':wdf_json.values.tolist()}, f, ensure_ascii=False, indent=4)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # gpt('002222','福晶科技')
    # exit()
    # bot = poe.Client(os.environ['POE'])
    analyze()