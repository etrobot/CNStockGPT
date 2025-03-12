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
from dotenv import load_dotenv,find_dotenv
from litellm import completion
import akshare as ak

from pocketbase import PocketBase

PROXY='http://127.0.0.1:7890'
load_dotenv(find_dotenv())

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

def hot(timeType='day',listType='normal'):
    '''
    timeType:day,hour
    listType:normal,skyrocket,tech,value,trend
    '''
    params = {
        'stock_type': 'a',
        'type': timeType,
        'list_type': listType,
    }
    response = requests.get(
        'https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock',
        params=params,
        headers={'User-Agent': 'Mozilla'},
    )
    return pd.DataFrame(response.json()['data']['stock_list'])


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
            replyTxt = completion(model='openai/gpt-3.5-turbo-1106', messages=[{
        "role": "user",
        "content": prompt,
    }], api_key=os.environ['API_KEY'],api_base=os.environ['API_BASE_URL'])["choices"][0]["message"]["content"]
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

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    wdf = hot()
    wdf.to_csv('wencai_o.csv',index=False)
    wdf.set_index('code',inplace=True)
    bot=Bot()
    for k,v in wdf.iterrows():
        symbol={17:'SH',33:'SZ'}[v['market']]+k
        wdf.at[k,'stock']='<a href="https://xueqiu.com/S/%s">%s<br>%s</a>'%(symbol,symbol,v['name'])
        news=ak.stock_news_em(k)
        news.drop_duplicates(subset='新闻标题',inplace=True)
        news['发布时间']=pd.to_datetime(news['发布时间'])
        news['新闻标题']=news['发布时间'].dt.strftime('%Y-%m-%d ')+news['新闻标题'].str.replace('%s：'%v['name'],'')
        news = news[~news['新闻标题'].str.contains('股|主力|机构|资金流')]
        news['news']=news['新闻标题'].str.cat(news['新闻内容'].str.split('。').str[0], sep=' ')
        news = news[news['news'].str.contains(v['name'])]
        news.sort_values(by=['发布时间'],ascending=False,inplace=True)
        # news=news[news['发布时间']> datetime.now() - timedelta(days=30)]

        if len(news)<2:
            continue
        newsTitles='\n'.join(news['新闻标题'][:30])[:1800]

        prompt="{'%s相关资讯':'''%s''',\n}\n请分析总结机会点和风险点，输出格式为{'机会':'''1..\n2..\n...''',\n'风险':'''1..\n2..\n...''',\n'题材标签':[标签]}"%(v['name'],newsTitles)

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
                wdf.at[k, 'chance'] = chances.replace(v['name'], '').replace('\n', '<br>')
                wdf.at[k, 'risk'] = risks.replace(v['name'], '').replace('\n', '<br>')
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
    wdf_w=wdf[['stock','chances','risks','tags','score']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(wdf_w,nowTxt+'.html',nowTxt)
    wdf_json=wdf[['symbol','chanceNum','chances','riskNum','risks','tags','score']]
    with open(nowTxt + '.json', 'w', encoding='utf_8_sig') as f:
        json.dump({'columns':wdf_json.columns.tolist(),'data':wdf_json.values.tolist()}, f, ensure_ascii=False, indent=4)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    analyze()
