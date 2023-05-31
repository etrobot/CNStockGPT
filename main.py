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
    wdf = hot()
    wdf.to_csv('wencai_o.csv',index=False)
    exit()
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
    wdf=wdf[['stock','chance','risk','tags','score']]
    nowTxt=datetime.now().strftime('%Y-%m-%d')
    renderHtml(wdf,nowTxt+'.html',nowTxt)