import ast
import os
import re
import urllib.request

import feedparser
import numpy as np
import pandas as pd
from datetime import *
import time as t

import requests
from dotenv import load_dotenv
from revChatGPT.V1 import Chatbot as ChatGPT


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
    proxy_support = urllib.request.ProxyHandler({ 'https': PROXY})
    opener = urllib.request.build_opener(proxy_support)
    urllib.request.install_opener(opener)
    feed = feedparser.parse(yf_rss_url % ticker)
    df = pd.json_normalize(feed.entries)
    df['published'] = pd.to_datetime(df["published"],format='mixed').dt.tz_convert('Asia/Shanghai')
    return df

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
    ydf = getActive()
    # ydf.to_csv('ydf.csv')
    bot=Bot()
    for k,v in ydf[:30].iterrows():
        symbol=v['symbol']
        ydf.at[k,'stock']='<a href="https://xueqiu.com/S/%s">%s<br>%s</a>'%(symbol,symbol,v['displayName'])
        news=get_yf_rss(symbol)
        news['summary']=news['published'].dt.strftime('%Y-%m-%d ')+news['summary']
        news.to_csv(symbol+'.csv')
        if len(news)<2:
            continue
        newsTitles='\n'.join(news['summary'].values)[:2900]+'...'

        prompt="{'%s(%s)相关资讯':'''%s''',\n}\n请根据资讯分析总结机会点和风险点，输出中文回答，回答格式为{'机会':[机会点],'风险':[风险点],'题材标签':[标签]}"%(v['symbol'],v['longName'],newsTitles)
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
                ydf.at[k, '机会'] = chances.replace('\n', '<br>')
                ydf.at[k, '风险'] = risks.replace('\n', '<br>')
                if '\n' in risks:
                    ydf.at[k, '得分'] = len(chances) - len(risks)
                ydf.at[k,'标签'] = '<br>'.join(parsed['题材标签'])
                break
            except Exception as e:
                print(e)
                retry-=1
                prompt+='，请务必保持python dict格式'
                t.sleep(20)
                continue
        t.sleep(20)
    ydf.dropna(subset=['得分'],inplace=True)
    ydf.sort_values(by=['得分'],ascending=False,inplace=True)
    ydf.to_csv('yahooFinance.csv')
    ydf=ydf[['stock','机会','风险','标签','得分']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(ydf,nowTxt+'_us.html',nowTxt)