利用akshare写一个A股的因子脚本，放在factor文件夹中，供multi_factor.py调用。
确保因子在相同尺度上可比较，都是百分比，都是越大越好。
multi_factor.py获取当日成交额前100以及日k数据交给各因子方法去计算，并返回dataframe表格。
多加log放便定位问题。
给每个脚本都加if __name__ == '__main__': 调试