# 快速开始指南

本指南将帮助您快速上手期权波动率交易系统。

## 准备工作

### 1. 环境要求
- Python 3.8 或更高版本
- pip 包管理器

### 2. 安装
```bash
# 克隆仓库
git clone https://github.com/shizhf999/options-volatility-trading-system.git
cd options-volatility-trading-system

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置设置
```bash
# 复制配置模板
cp config.json.example config.json

# 编辑配置文件
notepad config.json  # Windows
# 或
nano config.json     # Linux/Mac
```

## 配置文件说明

编辑 `config.json` 文件，填入您的具体配置：

```json
{
    "data_source": {
        "type": "tushare",
        "token": "您的tushare token"
    },
    "trading": {
        "initial_capital": 1000000,
        "max_position_ratio": 0.3,
        "stop_loss_ratio": 0.05
    },
    "logging": {
        "level": "INFO",
        "file_path": "./logs/trading.log"
    }
}
```

## 基本用法

### 1. 运行系统
```bash
python 期权波动率买方交易系统.py
```

### 2. 主要功能

#### 波动率监控
系统会自动监控期权的隐含波动率变化，识别交易机会。

#### 风险管理
- 自动止损设置
- 仓位管理
- 风险指标监控

#### 策略执行
- 基于波动率的买入策略
- 动态调整仓位
- 自动平仓管理

## 使用示例

### 基础监控
```python
# 系统启动后会自动开始监控
# 查看实时日志了解系统状态
tail -f logs/trading.log
```

### 手动策略测试
系统支持手动触发策略测试，帮助您验证策略逻辑。

### 历史数据分析
利用内置的分析工具，回顾和优化交易策略。

## 常见问题

### Q: 系统无法连接数据源？
A: 请检查网络连接和API配置是否正确。

### Q: 如何调整策略参数？
A: 修改配置文件中的相关参数，重启系统生效。

### Q: 支持哪些期权交易所？
A: 目前主要支持国内主要期权交易所，具体请参考源码。

## 进阶使用

- 自定义技术指标
- 策略回测
- 性能优化
- 多账户管理

更多详细信息请参考完整文档或源码注释。

## 获取帮助

- 提交 Issue：https://github.com/shizhf999/options-volatility-trading-system/issues
- 邮件联系：956551063@qq.com

祝您交易顺利！
