# CNStockGPT - A股多因子分析系统

## 项目简介
CNStockGPT是一个基于Python的A股多因子分析系统，使用akshare获取实时股票数据，通过计算多种技术因子（如动量因子、支撑因子等）来分析股票表现。

## 功能特性
- 获取A股实时行情数据
- 计算多种技术分析因子
  - 动量因子（N日收益率）
  - 支撑因子
- 自动筛选成交额前100的股票
- 生成可视化HTML报告
  
![image](https://github.com/user-attachments/assets/39574dae-3c14-4130-a03c-656da18f9336)


## 安装指南
1. 确保已安装Python 3.8+环境
2. 克隆本项目
```bash
git clone https://github.com/your-repo/CNStockGPT.git
cd CNStockGPT
```
3. 安装依赖
```bash
uv sync .
```

## 使用说明
1. 运行主程序
```bash
python app.py
```
2. 或者直接运行多因子计算模块
```bash
python -m factor.multi_factor
```

## 项目结构
```
CNStockGPT/
├── factor/            # 因子计算模块
│   ├── momentum_factor.py  # 动量因子计算
│   ├── support_factor.py   # 支撑因子计算
│   └── multi_factor.py    # 多因子整合
├── utils/             # 工具模块
│   ├── template.py    # HTML模板生成
├── result/            # 结果输出目录
└── readme.md          # 项目说明
```

## 许可证
MIT License
