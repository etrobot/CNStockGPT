import os
import requests,json,re
from datetime import *
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path= '.env')

def vikaData(id:str):
    headersVika = {
        'Authorization':'Bearer %s'%os.environ['VIKA'],
        'Connection': 'close'
    }
    vikaUrl = 'https://api.vika.cn/fusion/v1/datasheets/dstMiuU9zzihy1LzFX/records?viewId=viwoAJhnS2NMT&fieldKey=name'
    vikajson = requests.get(vikaUrl, headers=headersVika).json()
    print(vikajson)
    return [x['fields']['value'] for x in vikajson['data']['records'] if x['recordId'] == id][0]
def xqStockInfo(mkt, code:str, s, h):  # 雪球股票信息
    code=code.upper()
    data = {
        'code': str(code),
        'size': '30',
        # 'key': '47bce5c74f',
        'market': mkt,
    }
    r = s.get("https://xueqiu.com/stock/p/search.json", headers=h, params=data)
    print(code,r.text)
    stocks = json.loads(r.text)
    stocks = stocks['stocks']
    stock = None
    if len(stocks) > 0:
        for info in stocks:
            if info['code']==code:
                return info
    return stock

class xueqiuPortfolio():
    def __init__(self,mkt,pCode):
        self.pCode=pCode
        self.mkt = mkt
        self.position = dict()
        self.holdnum = 5
        self.session = requests.Session()
        self.session.cookies.update(self.getXueqiuCookie())
        self.p_url = 'https://xueqiu.com/P/'
        self.headers = {
            "Connection": "close",
             "user-agent": "Mozilla",
        }


    def getXueqiuCookie(self):
        sbCookie=os.environ['XQCOOKIE']
        if 'VIKA' in os.environ.keys():
            sbCookie=vikaData('rec0JPNQgmaP1')
        cookie_dict = {}
        for record in sbCookie.split(";"):
            key, value = record.strip().split("=", 1)
            cookie_dict[key] = value
        for item in cookie_dict:
            if 'expiry' in item:
                del item['expiry']
        return cookie_dict

    def trade(self, position_list=None):  # 调仓雪球组合

        portfolio_code = self.pCode
        if position_list is None:
            return
        remain_weight = 100 - sum(i.get('weight') for i in position_list)
        cash = round(remain_weight, 2)
        data = {
            "cash": cash,
            "holdings": str(json.dumps(position_list)),
            "cube_symbol": str(portfolio_code),
            'segment': 'true',
            'comment': ""
        }
        try:
            resp = self.session.post("https://xueqiu.com/cubes/rebalancing/create.json", headers=self.headers, data=data)
        except Exception as e:
            print('调仓失败: %s ' % e)
        else:
            print(resp.text)
            # with open('xueqiu/' + mkt + datetime.now().strftime('%Y%m%d_%H ：%M') + '.json', 'w', encoding='utf-8') as f:
            #     json.dump(json.loads(resp.text), f)

    def getPosition(self):
        if len(self.position)>0:
            return self.position
        resp = self.session.get(self.p_url + self.pCode, headers=self.headers).text.replace('null','0')
        portfolio_info = json.loads(re.search(r'(?<=SNB.cubeInfo = ).*(?=;\n)', resp).group())
        asset_balance = float(portfolio_info['net_value'])
        print(portfolio_info)
        position = portfolio_info['view_rebalancing']
        cash = asset_balance * float(position['cash'])  # 可用资金
        market = asset_balance - cash
        self.position['holding']=position['holdings']
        self.position['cash']=int(cash)
        self.position['last']=portfolio_info['last_success_rebalancing']['holdings']
        self.position['update']=datetime.fromtimestamp(position['updated_at']/1000).date()
        self.position['latest']=portfolio_info['sell_rebalancing']
        self.position['last']=portfolio_info['last_success_rebalancing']
        self.position['monthly_gain']=portfolio_info['monthly_gain']
        self.position['total_gain'] = portfolio_info['total_gain']
        return self.position

    def newPostition(self,mkt,symbol,wgt):
        stock = xqStockInfo(mkt, symbol, self.session, self.headers)
        return {
            "code": stock['code'],
            "name": stock['name'],
            "flag": stock['flag'],
            "current": stock['current'],
            "chg": stock['chg'],
            "stock_id": stock['stock_id'],
            "ind_id": stock['ind_id'],
            "ind_name": stock['ind_name'],
            "ind_color": stock['ind_color'],
            "textname": stock['name'],
            "segment_name": stock['ind_name'],
            "weight": wgt,  # 在这里自定义买入仓位,范围0.01到1
            "url": "/S/" + stock['code'],
            "proactive": True,
            "price": str(stock['current'])
        }

    def getCube(self):
        cubeUrl = 'https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol=' + os.environ['XQP']
        print(cubeUrl)
        response = self.session.get(url=cubeUrl,headers=self.headers)
        return json.loads(response.text)

def updatePortfoio(stockfile:str,pCode,mkt='cn'):
    df = pd.read_csv(stockfile)[:4]
    print(df)
    xueqiuP = xueqiuPortfolio(mkt,pCode)
    xueqiuPp = xueqiuP.getPosition()
    position = xueqiuPp['holding']
    cash = xueqiuPp['cash']
    latest = xueqiuPp['latest']
    stockHeld = [x['stock_symbol'] for x in position]
    for p in position:
        if p['stock_symbol'] not in df['symbol'].values:
            cash += p['weight']
            p['weight'] = 0
            p["proactive"] = True
    for k, v in df.iterrows():
        if v['symbol'] not in stockHeld and v['score'] > 0 and cash >= 24:
            position.append(xueqiuP.newPostition('cn', v['symbol'], 24))
            cash -= 24
    xueqiuP.trade(position)

if __name__ == "__main__":
    updatePortfoio('wencai.csv',os.environ['XQP'])

