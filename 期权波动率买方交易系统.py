# -*- coding: utf-8 -*-
"""
期权波动率买方交易系统 V1.5.1

【更新内容V1.5.1】:
1. 优化异常处理机制，避免过激的sys.exit
2. 改进数据缓存和性能优化
3. 增强日期解析功能，支持更多格式
"""

"""
期权波动率买方交易系统 - 基于波动率分析的期权买方决策支持系统
=================================================================

版本: 1.8.2
最后更新: 2025年5月24日

【版本历史】
- v1.0.0: 初始版本，基本波动率分析框架
- v1.1.0: 增加期权到期日精确获取功能
- v1.2.0: 改进IV历史计算与IV Rank/Percentile计算
- v1.3.0: 优化决策评分系统，调整权重
- v1.4.0: 细分IV趋势判断，增加3日均线，按趋势强度划分
- v1.5.0: 增加"IV是否高于短期均线"的额外判断，优化推荐合约筛选逻辑
- v1.5.1: 增强期权到期日获取方法(直接从XLS文件获取)，优化推荐合约排序(按品种分组+评分降序)，增加对数字格式日期(如"20250825.0")的解析支持
- v1.5.2: 整合Delta和Theta等希腊字母到评估逻辑中，优化买方决策的风险收益评估；输出结果到Excel并增加详细使用说明。
- v1.5.3: 将IV Rank/Percentile的默认历史回顾周期从252个日历日修改为365个日历日，以更好地覆盖约一整个交易年度的数据。
- v1.5.4: 在输出的Excel使用说明中，补充了当期权历史数据不足指定回顾周期时IV Rank/Percentile计算方式的说明。
- v1.5.5: 调整DTE（剩余到期天数）评分逻辑，对DTE过长（如>120天）的情况也进行适度减分，以平衡时间价值、权利金成本和期权弹性。
- v1.5.6: 进一步优化DTE评分逻辑，将30-60天设为最优区间，对超过120天的期权给予更显著减分，以实现不推荐买入的效果。
- v1.5.7: 根据实盘经验，再次微调DTE评分逻辑，将20-60天设为最优区间。
- v1.5.8: 调整DTE评分，将60 < DTE <= 120天区间的减分从-0.25调整为-0.5。
- v1.5.9: 增加基于当日成交量和持仓量的流动性过滤机制，对流动性不足的合约进行减分，并输出相关数据。
- v1.6.0: 修正流动性不足时"理由"文本的显示错误，使其能正确反映触发条件。
- v1.7.0: 增加波动率偏度和峰度分析功能，评估期权波动率微笑结构，根据市场方向预期优化交易策略，提升决策精确度。
- v1.8.0: 增强推荐条件，加入期权价格趋势要求(价格>昨收且>3日均线)，优化评分提取逻辑以保留完整浮点数评分而非仅整数部分，增加对未达到价格趋势条件的高分期权的日志记录。
- v1.8.1: 增强流动性筛选，自动排除极低流动性合约(成交量<20手或持仓量<50手)，使这类合约无论评分如何都不会被推荐，以降低交易执行风险。
- v1.8.2 (当前版本): 优化价格趋势判断逻辑，由"价格>昨收且>3日均线"调整为"价格>昨收且(>3日均线或>5日均线)"，放宽趋势判断条件以捕捉更多交易机会。当价格趋势条件不满足时，决策调整为"谨慎买入(风险较高)"。

【系统功能】
本系统主要用于分析期权合约的波动率环境，为期权买方提供交易决策支持。核心功能包括：
1. 加载与解析期货/期权历史数据
2. 计算期货历史波动率(HV)和期权隐含波动率(IV)
3. 计算IV Rank、IV Percentile以及IV趋势
4. 基于上述指标进行综合评分，给出期权买入建议
5. 输出评估结果和推荐合约列表

【使用说明】
- 系统需要以下数据文件：
  * 期货历史数据：d:/bb/期货历史/csv格式/
  * 期权历史数据：d:/bb/期权历史/csv格式/
  * 趋势判断数据：d:/bb/xia/模拟商品期权操作.csv
  * 期权剩余天数数据：d:/期权数据/*.xls
- 系统输出结果到：
  * Excel文件：d:/bb/期权买方交易建议/期权交易建议与说明.xlsx
    该文件包含多个Sheet: "交易建议结果", "推荐合约列表", "使用说明"

【注意事项】
- 系统假设趋势已由用户预先判断，决策主要基于波动率条件
- 期权到期日的准确获取对IV计算至关重要
- 建议定期更新期权剩余天数数据文件
"""

import pandas as pd
import numpy as np
import math
import os
import csv
import glob
import sys
import traceback
import json
from datetime import datetime, timedelta

# 自定义异常类
class OptionTradingSystemError(Exception):
    """期权交易系统基础异常类"""
    pass

class DataLoadError(OptionTradingSystemError):
    """数据加载异常"""
    pass

class ConfigurationError(OptionTradingSystemError):
    """配置相关异常"""
    pass

class OptionAnalysisError(OptionTradingSystemError):
    """期权分析异常"""
    pass

class DateParseError(OptionTradingSystemError):
    """日期解析异常"""
    pass

# --- 配置管理 ---
class Config:
    def __init__(self, config_file_path='config.json'):
        self.config_file_path = config_file_path
        self.config = self.load_config()
    
    def load_config(self):
        """加载配置文件"""
        try:
            # 首先尝试当前脚本目录
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(script_dir, self.config_file_path)
            
            if not os.path.exists(config_path):
                # 如果脚本目录没有，尝试当前工作目录
                config_path = self.config_file_path
                
            if not os.path.exists(config_path):
                print(f"警告: 配置文件 {config_path} 不存在，使用默认配置")
                return self.get_default_config()
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"成功加载配置文件: {config_path}")
            return config
        except Exception as e:
            print(f"加载配置文件失败: {e}，使用默认配置")
            return self.get_default_config()
    
    def get_default_config(self):
        """返回默认配置"""
        return {
            "data_paths": {
                "futures_data_dir": "d:/bb/期货历史/csv格式",
                "options_data_dir": "d:/bb/期权历史/csv格式", 
                "trend_file": "d:/bb/xia/模拟商品期权操作.csv",
                "output_dir": "d:/bb/期权买方交易建议",
                "options_expiry_data_dir": "d:/期权数据"
            },
            "parameters": {
                "risk_free_rate": 0.02,
                "batch_size": 500,
                "hv_window": 10,
                "iv_rank_window_days": 365
            }
        }
    
    def get(self, key, default=None):
        """获取配置值"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

# --- 0. 辅助函数 ---
def norm_cdf(x):
    """标准正态分布的累积分布函数 (CDF)"""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def norm_pdf(x):
    """标准正态分布的概率密度函数 (PDF)"""
    return (1 / math.sqrt(2 * math.pi)) * math.exp(-0.5 * x**2)

# 添加辅助函数用于提取评分
def extract_score_from_reason(reason_text):
    """从reason文本中提取评分，返回浮点数"""
    import re
    
    if not reason_text:
        return 0.0
    
    try:
        # 多种评分模式匹配
        patterns = [
            r'评分[：:]\s*(\d+\.?\d*)',      # 评分：8.5
            r'得分[：:]\s*(\d+\.?\d*)',      # 得分: 7.2
            r'评估分数[：:]\s*(\d+\.?\d*)',   # 评估分数：9.1
            r'综合评分[：:]\s*(\d+\.?\d*)',   # 综合评分: 8.0
            r'(\d+\.?\d*)\s*分',            # 8.5分
            r'score[：:]\s*(\d+\.?\d*)',     # score: 8.0
        ]
        
        for pattern in patterns:
            match = re.search(pattern, reason_text)
            if match:
                score = float(match.group(1))
                # 验证评分范围合理性（通常0-10分）
                if 0 <= score <= 10:
                    return score
                elif score > 10:  # 可能是百分制，转换为十分制
                    return min(score / 10, 10.0)
        
    except Exception as e:
        print(f"警告: 评分提取失败 '{reason_text}': {e}")
        
    return 0.0

# --- 1. 数据加载与预处理 ---
class DataLoader:
    def __init__(self, futures_dir, options_dir, cache_limit=100):
        self.futures_dir = futures_dir
        self.options_dir = options_dir
        self.futures_data_cache = {}
        self.options_data_cache = {}
        self.cache_limit = cache_limit  # 缓存限制，防止内存泄漏
        self.cache_access_count = {}  # 访问计数，用于LRU策略
        self.total_cache_access = 0
        
    def _manage_cache_size(self, cache_dict, cache_key):
        """管理缓存大小，使用LRU策略，增强性能监控"""
        if len(cache_dict) >= self.cache_limit:
            # 找到最少使用的缓存项
            if cache_key not in cache_dict:  # 只有在添加新项时才清理
                least_used_key = min(self.cache_access_count.keys(), 
                                   key=lambda k: self.cache_access_count.get(k, 0))
                if least_used_key in cache_dict:
                    del cache_dict[least_used_key]
                    del self.cache_access_count[least_used_key]
                    print(f"缓存清理: 移除最少使用的数据 {least_used_key} (当前缓存大小: {len(cache_dict)})")
        
        # 定期报告缓存状态
        if self.total_cache_access % 100 == 0 and self.total_cache_access > 0:
            print(f"缓存统计: 访问次数={self.total_cache_access}, 期货缓存={len(self.futures_data_cache)}, 期权缓存={len(self.options_data_cache)}")
    
    def _update_cache_access(self, cache_key):
        """更新缓存访问计数"""
        self.total_cache_access += 1
        self.cache_access_count[cache_key] = self.total_cache_access

    def _parse_date_safe(self, date_str):
        """
        安全解析日期字符串，支持多种常见格式
        增强版本：支持更多日期格式和更好的错误处理
        """
        if pd.isna(date_str) or date_str is None:
            return pd.NaT
            
        date_str = str(date_str).strip()
        
        # 如果是空字符串
        if not date_str:
            return pd.NaT
            
        # 先处理特殊的数字格式日期（如"20240825.0"）
        if date_str.replace(".", "", 1).isdigit() and len(date_str) >= 8:
            try:
                # 去掉可能存在的小数点和小数部分
                clean_date = date_str.split('.')[0]
                if len(clean_date) >= 8:
                    year = int(clean_date[:4])
                    month = int(clean_date[4:6])
                    day = int(clean_date[6:8])
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        return datetime(year, month, day)
            except (ValueError, IndexError):
                pass
        
        # 多种日期格式尝试解析
        date_formats = [
            "%Y/%m/%d",     # 2024/08/25
            "%Y-%m-%d",     # 2024-08-25
            "%Y年%m月%d日",  # 2024年08月25日
            "%m/%d/%Y",     # 08/25/2024
            "%d/%m/%Y",     # 25/08/2024
            "%Y%m%d",       # 20240825
            "%Y.%m.%d",     # 2024.08.25
            "%d-%m-%Y",     # 25-08-2024
            "%d.%m.%Y",     # 25.08.2024
            "%Y/%m/%d %H:%M:%S",  # 带时间
            "%Y-%m-%d %H:%M:%S",  # 带时间
        ]
        
        # 取日期部分，忽略可能的时间
        date_part = date_str.split(' ')[0]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_part, fmt)
                # 验证日期合理性
                if 1900 <= parsed_date.year <= 2100:
                    return parsed_date
            except ValueError:
                continue
        
        # 如果所有格式都失败，记录警告但不抛出异常
        print(f"警告: 无法解析日期字符串 '{date_str}'，将返回NaT")
        return pd.NaT

    def parse_date(self, date_str):
        """
        公共接口：解析日期字符串，支持多种常见格式
        这是对_parse_date_safe方法的公共包装
        """
        return self._parse_date_safe(date_str)

    def load_futures_data(self, futures_code: str) -> pd.DataFrame:
        """
        加载期货数据，带缓存和错误处理优化
        """
        if futures_code in self.futures_data_cache:
            self._update_cache_access(futures_code)
            return self.futures_data_cache[futures_code]

        file_path = os.path.join(self.futures_dir, f"{futures_code}.csv")
        
        try:
            if not os.path.exists(file_path):
                raise DataLoadError(f"期货数据文件不存在: {file_path}")
                
            df = pd.read_csv(file_path, encoding='utf-8-sig') # utf-8-sig 处理 BOM
            df.rename(columns={'代码': 'symbol', '日期': 'date', '开盘': 'open',
                               '最高': 'high', '最低': 'low', '收盘': 'close',
                               '成交量': 'volume', '持仓量': 'open_interest',
                               '结算价': 'settle'}, inplace=True)
            df['date'] = df['date'].apply(self._parse_date_safe)
            df.dropna(subset=['date'], inplace=True)
            
            if df.empty:
                raise DataLoadError(f"期货数据文件为空或无有效日期: {file_path}")
                
            df = df.sort_values(by='date').reset_index(drop=True)
            for col in ['open', 'high', 'low', 'close', 'settle']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 缓存管理
            self._manage_cache_size(self.futures_data_cache, futures_code)
            self.futures_data_cache[futures_code] = df
            self._update_cache_access(futures_code)
            
            return df
            
        except DataLoadError:
            raise  # 重新抛出自定义异常
        except Exception as e:
            raise DataLoadError(f"加载期货数据失败 {file_path}: {str(e)}")
            
    def clear_cache(self):
        """清空所有缓存，释放内存"""
        self.futures_data_cache.clear()
        self.options_data_cache.clear()
        self.cache_access_count.clear()
        self.total_cache_access = 0
        print("数据缓存已清空")
    
    def load_option_data(self, option_code: str) -> pd.DataFrame:
        """
        加载期权数据，带缓存和错误处理优化
        """
        if option_code in self.options_data_cache:
            self._update_cache_access(option_code)
            return self.options_data_cache[option_code]

        # 期权文件名可能与期权代码完全一致，也可能需要映射
        # 尝试几种可能的文件命名方式
        file_paths_to_try = [
            os.path.join(self.options_dir, f"{option_code}.csv"),  # 原始代码
            os.path.join(self.options_dir, f"{option_code.upper()}.csv"),  # 大写
            os.path.join(self.options_dir, f"{option_code.lower()}.csv"),  # 小写
        ]
        
        # 特殊处理TA期权
        if "TA" in option_code or "ta" in option_code:
            print(f"尝试加载TA期权数据: {option_code}")
            
        last_error = None
        # 尝试所有可能的文件路径
        for file_path in file_paths_to_try:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                    df.rename(columns={'代码': 'option_code', '期货': 'underlying_futures',
                                       '日期': 'date', '开盘': 'open', '最高': 'high',
                                       '最低': 'low', '收盘': 'close', '成交量': 'volume',
                                       '持仓量': 'open_interest', '结算价': 'settle'},
                              inplace=True)
                    df['date'] = df['date'].apply(self._parse_date_safe)
                    df.dropna(subset=['date'], inplace=True)
                    
                    if df.empty:
                        raise DataLoadError(f"期权数据文件为空或无有效日期: {file_path}")
                        
                    df = df.sort_values(by='date').reset_index(drop=True)
                    for col in ['open', 'high', 'low', 'close', 'settle']:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # 缓存管理
                    self._manage_cache_size(self.options_data_cache, option_code)
                    self.options_data_cache[option_code] = df
                    self._update_cache_access(option_code)
                    
                    return df
                except Exception as e:
                    last_error = e
                    print(f"警告: 尝试加载期权数据 {file_path} 失败: {e}")
                    continue
        
        # 如果所有尝试都失败了，提供更详细的信息
        if "TA" in option_code or "ta" in option_code:
            print(f"无法找到TA期权数据文件: {option_code}")
            # 列出目录中包含TA的文件
            try:
                matching_files = [f for f in os.listdir(self.options_dir) 
                                if "TA" in f.upper() and f.endswith('.csv')]
                if matching_files:
                    print(f"目录中可能相关的TA文件: {matching_files[:10]}")
            except Exception as e:
                print(f"无法列出目录文件: {e}")
            
        error_msg = f"期权数据文件未找到: {file_paths_to_try}"
        if last_error:
            error_msg += f"，最后一个错误: {last_error}"
        raise DataLoadError(error_msg)

    def batch_load_options(self, option_codes_list, max_batch_size=50):
        """
        批量加载期权数据，提高性能
        """
        results = {}
        batch_count = 0
        total_batches = (len(option_codes_list) + max_batch_size - 1) // max_batch_size
        
        for i in range(0, len(option_codes_list), max_batch_size):
            batch = option_codes_list[i:i + max_batch_size]
            batch_count += 1
            
            print(f"批处理 {batch_count}/{total_batches}: 加载 {len(batch)} 个期权合约")
            
            batch_results = {}
            for option_code in batch:
                try:
                    df = self.load_option_data(option_code)
                    if not df.empty:
                        batch_results[option_code] = df
                except Exception as e:
                    print(f"批处理中加载期权 {option_code} 失败: {e}")
            
            results.update(batch_results)
            
            # 批处理间的短暂停顿，避免过度占用系统资源
            if batch_count < total_batches:
                import time
                time.sleep(0.1)
        
        print(f"批处理完成: 成功加载 {len(results)}/{len(option_codes_list)} 个期权合约")
        return results
    
    def get_cache_status(self):
        """获取缓存状态统计"""
        return {
            'futures_cache_size': len(self.futures_data_cache),
            'options_cache_size': len(self.options_data_cache),
            'total_access_count': self.total_cache_access,
            'cache_limit': self.cache_limit
        }

class OptionContractInfo:
    def __init__(self, config=None):
        # 简化的期权到期日规则：通常是标的期货交割月份前一个月的某个交易日
        # 这部分需要根据具体交易所的规则精确化！
        # 例如：大商所期权到期日为标的期货合约月份前一个月的第5个交易日
        # 这里用一个非常简化的例子，实际中您需要一个精确的日历和规则。
        self.MONTH_CODES = "FGHJKMNQUVXZ" # 用于期货月份代码，但您的数据中是数字月份
        # 缓存期权到期日，避免重复计算和日志输出
        self.expiry_date_cache = {}
        # 读取期权剩余天数数据文件
        try:
            # 获取配置中的期权数据目录
            if config:
                options_data_dir = config.get('data_paths.options_expiry_data_dir', 'd:/期权数据')
            else:
                options_data_dir = 'd:/期权数据'
            if not os.path.exists(options_data_dir):
                print(f"警告: 期权数据目录不存在: {options_data_dir}")
                self.expiry_data = None
                return
            
            # 获取所有xls文件
            xls_files = glob.glob(os.path.join(options_data_dir, "*.xls"))
            if not xls_files:
                print(f"警告: 在目录 {options_data_dir} 中未找到xls文件")
                self.expiry_data = None
                return
            
            # 按文件修改时间排序，取最新的
            latest_file = max(xls_files, key=os.path.getmtime)
            print(f"找到最新的期权数据文件: {os.path.basename(latest_file)}")
            
            # 读取最新的xls文件
            import pandas as pd
            # 首先检查文件实际内容
            with open(latest_file, 'rb') as f:
                header_bytes = f.read(20)
                print(f"文件头字节: {header_bytes}")
            
            # 尝试多种方式读取
            try:
                # 尝试1: 以CSV方式读取，指定编码为中文常见编码
                for encoding in ['gbk', 'gb2312', 'utf-8-sig', 'utf-8']:
                    try:
                        print(f"尝试以{encoding}编码读取为CSV")
                        self.expiry_data = pd.read_csv(latest_file, encoding=encoding, sep=None, engine='python')
                        print(f"成功以CSV方式读取，编码为{encoding}，列名为: {list(self.expiry_data.columns)}")
                        break
                    except Exception as e:
                        print(f"以{encoding}编码读取CSV失败: {e}")
                        continue
                
                # 如果CSV读取失败，尝试Excel
                if self.expiry_data is None:
                    for engine in ['openpyxl', 'xlrd']:
                        try:
                            print(f"尝试用{engine}引擎读取Excel")
                            self.expiry_data = pd.read_excel(latest_file, engine=engine)
                            print(f"成功用{engine}引擎读取Excel，列名为: {list(self.expiry_data.columns)}")
                            break
                        except Exception as e:
                            print(f"用{engine}引擎读取Excel失败: {e}")
                            continue
            except Exception as e:
                print(f"所有读取方法均失败: {e}")
                self.expiry_data = None
                return
                
            if self.expiry_data is None:
                print("无法使用任何方法读取文件，请检查文件格式")
                return
            
            # 清理代码列，去除="CODE"和引号
            if self.expiry_data is not None:
                # 查找代码列
                code_column = None
                for col in self.expiry_data.columns:
                    if '代码' in str(col):
                        code_column = col
                        print(f"找到代码列: {code_column}")
                        break
                
                if code_column:
                    # 处理代码格式，去掉="和"
                    self.expiry_data[code_column] = self.expiry_data[code_column].astype(str)
                    self.expiry_data[code_column] = self.expiry_data[code_column].str.replace('="', '')
                    self.expiry_data[code_column] = self.expiry_data[code_column].str.replace('"', '')
                    self.expiry_data[code_column] = self.expiry_data[code_column].str.replace('=', '')
                    print(f"代码格式处理后示例: {self.expiry_data[code_column].iloc[0] if not self.expiry_data.empty else 'N/A'}")
                    print(f"成功加载期权剩余天数数据，共{len(self.expiry_data)}条记录")
                else:
                    print(f"警告: 期权剩余天数数据文件中未找到代码列，尝试可用的列名: {list(self.expiry_data.columns)}")
                    self.expiry_data = None
        except Exception as e:
            print(f"加载期权剩余天数数据失败: {e}")
            self.expiry_data = None

    def parse_option_code(self, option_code: str):
        parts = option_code.split('-')
        if len(parts) != 3:
            print(f"警告: 期权代码格式无法解析: {option_code}")
            return None
        
        underlying_futures = parts[0]
        option_type = parts[1].upper() # 'C' or 'P'
        try:
            strike_price = float(parts[2])
        except ValueError:
            print(f"警告: 无法从期权代码解析行权价: {option_code}")
            return None

        # 尝试从标的期货代码解析年份和月份
        # 例如 A2507 -> underlying_base='A', year_short=25, month=07
        #      AG2507 -> underlying_base='AG', year_short=25, month=07
        underlying_base = ""
        year_short_str = ""
        month_str = ""

        for i, char in enumerate(underlying_futures):
            if char.isdigit():
                year_short_str = underlying_futures[i:i+2]
                month_str = underlying_futures[i+2:]
                break
            underlying_base += char
        
        if not (year_short_str and month_str and year_short_str.isdigit() and month_str.isdigit()):
            print(f"警告: 无法从标的期货代码 {underlying_futures} 解析年份和月份。")
            return None
            
        current_century = datetime.now().year // 100
        full_year = current_century * 100 + int(year_short_str)
        month = int(month_str)

        return {
            'option_code': option_code,
            'underlying_base': underlying_base, # 'A', 'AG' etc.
            'underlying_futures_code': underlying_futures, # 'A2507', 'AG2507'
            'futures_expiry_year': full_year,
            'futures_expiry_month': month,
            'option_type': option_type,
            'strike_price': strike_price
        }

    def get_option_expiry_date(self, parsed_code_info: dict, trade_date: datetime) -> datetime:
        """
        获取期权合约的实际到期日。
        【修改V1.5.1】: 增强到期日获取方法，优先从xls文件中直接提取到期日期字段
        - 首先尝试查找到期日期字段(如"到期日期"、"到期日"等列名)
        - 支持多种日期格式的解析
        - 如果未找到直接的到期日字段，回退到原有的剩余天数计算方法
        - 如果所有方法都失败，则终止程序
        【优化V1.8.3】: 增加缓存机制，避免重复计算和日志输出

        参数:
            parsed_code_info: 解析后的期权代码信息字典
            trade_date: 交易日期，用于计算剩余天数(如需)
        
        返回:
            datetime: 期权的到期日期
        """
        if not parsed_code_info:
            return pd.NaT

        option_code = parsed_code_info['option_code']
        
        # 检查缓存
        if option_code in self.expiry_date_cache:
            return self.expiry_date_cache[option_code]
        
        # 从xls数据文件中获取到期日信息
        if not hasattr(self, 'expiry_data') or self.expiry_data is None:
            error_msg = "未能加载期权剩余天数数据文件。请确保文件存在且格式正确。"
            print(f"错误: {error_msg}")
            raise ConfigurationError(error_msg)
        
        try:
            # 尝试查找匹配的期权代码 
            code_column = None
            for col in self.expiry_data.columns:
                if '代码' in str(col):
                    code_column = col
                    break
            
            if not code_column:
                error_msg = f"在数据文件中未找到代码列。可用的列: {list(self.expiry_data.columns)}"
                print(f"错误: {error_msg}")
                raise ConfigurationError(error_msg)
                
            # 在剩余天数数据中查找匹配的期权代码
            matching_rows = self.expiry_data[self.expiry_data[code_column] == option_code]
            
            # 如果没有精确匹配，尝试模糊匹配
            if matching_rows.empty:
                parts = option_code.split('-')
                if len(parts) >= 3:
                    base_code = parts[0]  # 如"AG2507"
                    matching_rows = self.expiry_data[self.expiry_data[code_column].str.contains(base_code, na=False)]
            
            if matching_rows.empty:
                error_msg = f"未找到与'{option_code}'匹配的记录"
                print(f"错误: {error_msg}")
                raise DataLoadError(error_msg)
            
            # 【修改V1.5.1】: 首先查找并尝试直接使用到期日期列
            expiry_date_columns = []
            for col in matching_rows.columns:
                col_str = str(col).lower()
                if ('到期' in col_str and '日期' in col_str) or ('到期日' in col_str) or ('expiry' in col_str) or ('expiration' in col_str):
                    expiry_date_columns.append(col)
            
            if expiry_date_columns:
                # 使用找到的到期日期列
                expiry_col = expiry_date_columns[0]
                expiry_date_str = matching_rows[expiry_col].iloc[0]
                
                # 先处理特殊的数字格式日期（如"20250825.0"）
                expiry_date_str = str(expiry_date_str).strip()
                try:
                    # 数字格式日期处理（如"20250825.0"或"20250825"）
                    if expiry_date_str.replace(".", "", 1).isdigit() and len(expiry_date_str) >= 8:
                        # 去掉可能存在的小数点和小数部分
                        date_str = expiry_date_str.split('.')[0]
                        # 确保至少有8位数字
                        if len(date_str) >= 8:
                            year = int(date_str[:4])
                            month = int(date_str[4:6])
                            day = int(date_str[6:8])
                            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                                expiry_date = datetime(year, month, day)
                                print(f"期权 {option_code} 的到期日为 {expiry_date.strftime('%Y-%m-%d')} (数字格式解析)")
                                # 缓存结果
                                self.expiry_date_cache[option_code] = expiry_date
                                return expiry_date
                except Exception as e:
                    print(f"尝试数字格式解析日期 '{expiry_date_str}' 失败: {e}")
                
                # 尝试多种日期格式解析到期日字符串
                for date_format in ('%Y/%m/%d', '%Y-%m-%d', '%Y年%m月%d日', '%m/%d/%Y'):
                    try:
                        expiry_date = datetime.strptime(str(expiry_date_str).split(' ')[0], date_format)
                        print(f"期权 {option_code} 的到期日为 {expiry_date.strftime('%Y-%m-%d')}")
                        # 缓存结果
                        self.expiry_date_cache[option_code] = expiry_date
                        return expiry_date
                    except ValueError:
                        continue
                
                print(f"警告: 无法解析到期日 '{expiry_date_str}'，尝试使用剩余天数计算")
            
            # 【备选方法】: 如果没有找到到期日列或解析失败，尝试使用剩余天数计算
            days_columns = []
            for col in matching_rows.columns:
                col_str = str(col).lower()
                if ('剩余' in col_str and '天' in col_str) or ('dte' in col_str) or ('day' in col_str) or ('天数' in col_str):
                    days_columns.append(col)
            
            if days_columns:
                days_col = days_columns[0]
                if not pd.isna(matching_rows[days_col].iloc[0]):
                    try:
                        remaining_days = float(matching_rows[days_col].iloc[0])
                        # 根据当前日期和剩余天数计算到期日
                        expiry_date = trade_date + pd.Timedelta(days=remaining_days)
                        print(f"期权 {option_code} 的剩余天数为 {remaining_days}，计算得到的到期日为 {expiry_date.strftime('%Y-%m-%d')}")
                        # 缓存结果
                        self.expiry_date_cache[option_code] = expiry_date
                        return expiry_date
                    except ValueError:
                        error_msg = f"无法将'{matching_rows[days_col].iloc[0]}'转换为数字"
                        print(f"错误: {error_msg}")
                        raise DateParseError(error_msg)
            
            # 如果两种方式都失败了
            error_msg = f"未能获取期权 {option_code} 的到期日信息"
            print(f"错误: {error_msg}")
            raise DataLoadError(error_msg)
                
        except Exception as e:
            error_msg = f"获取到期日失败: {e}"
            print(f"错误: {error_msg}")
            print(traceback.format_exc())
            raise OptionAnalysisError(error_msg)
        
        # 如果以上所有尝试都失败了（理论上不会执行到这里）
        error_msg = f"未能为期权 {option_code} 找到到期日信息"
        print(f"错误: {error_msg}")
        raise OptionAnalysisError(error_msg)


# --- 2. 核心计算函数 ---
class OptionCalculations:
    def __init__(self, risk_free_rate=0.02, config=None):
        self.risk_free_rate = risk_free_rate
        self.contract_info_parser = OptionContractInfo(config)


    def calculate_tte(self, trade_date: datetime, option_expiry_date: datetime) -> float:
        """计算剩余到期时间 (年化)"""
        if pd.isna(trade_date) or pd.isna(option_expiry_date):
            return 0.0
        if trade_date >= option_expiry_date:
            return 0.0  # 已到期或到期日当天
        # 国内交易日通常按240-252天算，这里简化用365.25
        # TTE通常计算到期权停止交易的时刻，为简化，这里计算到期日开始
        # 更精确的TTE会考虑交易时间，例如从当前时间到到期日下午3点
        time_delta_days = (option_expiry_date - trade_date).total_seconds() / (24 * 3600)
        return max(0.0, time_delta_days / 365.25)


    def black_scholes_merton(self, S, K, T, r, sigma, option_type='C'):
        """计算Black-Scholes-Merton期权价格"""
        if sigma <= 0 or T <= 0: # 波动率或时间为0（或负），期权只有内在价值
            if option_type == 'C':
                return max(0, S - K)
            else: # 'P'
                return max(0, K - S)

        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == 'C':
            price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
        elif option_type == 'P':
            price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
        else:
            raise ValueError("期权类型必须是 'C' 或 'P'")
        return price

    def calculate_implied_volatility(self, option_price_market, S, K, T, r, option_type='C', max_iter=100, tol=1e-5):
        """使用Newton-Raphson方法计算隐含波动率"""
        if option_price_market <= 0 or T <= 0:
             # 如果期权市价小于等于其内在价值（考虑非常小的时间价值），IV可能无解或接近0
            intrinsic_value = 0
            if option_type == 'C':
                intrinsic_value = max(0, S - K * math.exp(-r * T)) # 贴现后的行权价
            else:
                intrinsic_value = max(0, K * math.exp(-r * T) - S)
            
            if option_price_market <= intrinsic_value + tol : # 加上一点容忍度
                 return 0.0001 # 返回一个非常小的值表示接近0


        sigma = 0.5  # 初始猜测值
        for _ in range(max_iter):
            if sigma <= 0: sigma = tol # 防止sigma为0或负
            
            price_model = self.black_scholes_merton(S, K, T, r, sigma, option_type)
            
            # Vega 计算 (dPrice/dSigma)
            # d1, d2 需要重新计算或从bsm中获取
            if sigma * math.sqrt(T) == 0: # 避免除零
                vega = 0
            else:
                d1_calc = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
                vega = S * norm_pdf(d1_calc) * math.sqrt(T)

            diff = price_model - option_price_market

            if abs(diff) < tol:
                return sigma
            if vega < tol : # Vega过小，可能IV无解或迭代不稳定
                # 可以尝试二分法或返回一个标记（如np.nan）
                # print(f"警告: Vega过小({vega:.2e}) S={S},K={K},T={T},r={r},mkt_P={option_price_market},sigma={sigma},model_P={price_model}")
                # 尝试调整sigma，例如如果模型价比市场价高，降低sigma
                if price_model > option_price_market:
                    sigma = sigma * 0.9
                else:
                    sigma = sigma * 1.1
                if sigma > 2.0 : return 2.0 # IV上限
                if sigma < 0.0001 : return 0.0001 # IV下限
                continue # 继续尝试

            sigma = sigma - diff / vega
            
            # 限制IV在合理范围，如0.1%到200%
            sigma = max(0.0001, min(sigma, 2.0))


        # print(f"警告: IV计算未能在{max_iter}次迭代内收敛. S={S},K={K},T={T},mkt_P={option_price_market}, 最后 sigma={sigma:.4f}, diff={diff:.4f}")
        return sigma # 或返回 np.nan 表示未收敛

    def calculate_historical_volatility(self, close_prices: pd.Series, window: int = 20) -> float:
        if len(close_prices) < window + 1:
            return np.nan
        log_returns = np.log(close_prices / close_prices.shift(1))
        rolling_std = log_returns.rolling(window=window, min_periods=window).std() # 确保窗口期数据足够
        if rolling_std.empty or pd.isna(rolling_std.iloc[-1]):
            return np.nan
        annualized_hv = rolling_std.iloc[-1] * np.sqrt(252) # 假设一年252个交易日
        return annualized_hv

    def delta(self, S, K, T, r, sigma, option_type='C'):
        """计算期权的Delta值"""
        if T <= 0 or sigma <= 0: # 已到期或无波动
            if option_type == 'C':
                return 1.0 if S > K else (0.5 if S == K else 0.0)
            else: # 'P'
                return -1.0 if S < K else (-0.5 if S == K else 0.0)

        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        if option_type == 'C':
            return norm_cdf(d1)
        elif option_type == 'P':
            return norm_cdf(d1) - 1
        else:
            raise ValueError("期权类型必须是 'C' 或 'P'")

    def gamma(self, S, K, T, r, sigma):
        """计算期权的Gamma值"""
        if T <= 0 or sigma <= 0:
            return 0.0
            
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        return norm_pdf(d1) / (S * sigma * math.sqrt(T))

    def vega(self, S, K, T, r, sigma):
        """计算期权的Vega值 (波动率每变化1%，期权价格的变化)"""
        if T <= 0 or sigma <= 0:
            return 0.0
            
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        # Vega 通常表示波动率变化1%（即0.01）时期权价格的变化量，所以结果要除以100
        return S * norm_pdf(d1) * math.sqrt(T) / 100

    def theta(self, S, K, T, r, sigma, option_type='C'):
        """计算期权的Theta值 (年化)"""
        if T <= 0 or sigma <= 0:
            return 0.0

        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == 'C':
            theta_val = - (S * norm_pdf(d1) * sigma) / (2 * math.sqrt(T)) \
                        - r * K * math.exp(-r * T) * norm_cdf(d2)
        elif option_type == 'P':
            theta_val = - (S * norm_pdf(d1) * sigma) / (2 * math.sqrt(T)) \
                        + r * K * math.exp(-r * T) * norm_cdf(-d2)
        else:
            raise ValueError("期权类型必须是 'C' 或 'P'")
        return theta_val

# --- 3. 波动率指标计算 ---
class VolatilityAnalyzer:
    def __init__(self, data_loader: DataLoader, option_calc: OptionCalculations):
        self.loader = data_loader
        self.calculator = option_calc
        self.contract_info_parser = OptionContractInfo()

    def get_historical_iv_series(self, option_code: str, current_trade_date: datetime):
        parsed_info = self.contract_info_parser.parse_option_code(option_code)
        if not parsed_info:
            return pd.Series(dtype=float)

        option_df = self.loader.load_option_data(option_code)
        if option_df.empty:
            return pd.Series(dtype=float)
        
        # 确保只处理到 current_trade_date 的数据
        option_df_historical = option_df[option_df['date'] <= current_trade_date].copy()
        if option_df_historical.empty:
            return pd.Series(dtype=float)

        underlying_futures_code = parsed_info['underlying_futures_code']
        futures_df = self.loader.load_futures_data(underlying_futures_code)
        if futures_df.empty:
            return pd.Series(dtype=float)

        # 合并期权和期货数据，基于日期
        merged_df = pd.merge(option_df_historical, futures_df[['date', 'close']],
                             on='date', suffixes=('_option', '_futures'))
        if merged_df.empty:
            return pd.Series(dtype=float)

        iv_series = []
        dates_series = []

        # 获取一次期权到期日
        # 注意：真实的期权到期日对一个特定合约是固定的，不应依赖trade_date去动态获取。
        # 这里用current_trade_date仅是为了调用 get_option_expiry_date 的占位符逻辑。
        # 理想情况下，这个 option_expiry_date 应该是合约的固有属性。
        # 我们假设，对于历史IV计算，我们使用该合约的真实固定到期日。
        # 这里我将尝试在第一次调用时获取，并复用。
        # 【【【重要！！】】】您必须在这里提供一种可靠的方式来获取 option_code 的真实固定到期日！


        for _, row in merged_df.iterrows():
            trade_date = row['date']
            option_price = row['close_option'] # 或 'settle_option'
            underlying_price = row['close_futures'] # 或 'settle_futures'
            
            # 【【【关键：期权到期日获取】】】
            # 您需要实现一个可靠的 get_option_expiry_date 方法，
            # 它能够根据 option_code (parsed_info) 返回该期权的固定到期日。
            # 不应每次都用 trade_date 去"猜"。
            # 为了演示，我继续用之前的简化版，但它不适合生产。
            option_expiry_date = self.contract_info_parser.get_option_expiry_date(parsed_info, trade_date)
            if pd.isna(option_expiry_date):
                # print(f"日期 {trade_date}: 无法获取 {option_code} 的到期日，跳过IV计算。")
                iv_series.append(np.nan)
                dates_series.append(trade_date)
                continue

            tte = self.calculator.calculate_tte(trade_date, option_expiry_date)
            
            if tte <= 0 or pd.isna(option_price) or pd.isna(underlying_price) or underlying_price <=0 or option_price <=0:
                iv_series.append(np.nan)
                dates_series.append(trade_date)
                continue

            iv = self.calculator.calculate_implied_volatility(
                option_price_market=option_price,
                S=underlying_price,
                K=parsed_info['strike_price'],
                T=tte,
                r=self.calculator.risk_free_rate,
                option_type=parsed_info['option_type']
            )
            iv_series.append(iv)
            dates_series.append(trade_date)
        
        if not dates_series:
            return pd.Series(dtype=float)
            
        result_series = pd.Series(data=iv_series, index=pd.DatetimeIndex(dates_series)).dropna()
        return result_series


    def get_iv_rank_percentile(self, historical_iv_series: pd.Series, current_iv: float, window_days=252):
        if historical_iv_series.empty or pd.isna(current_iv) or len(historical_iv_series) < 2:
            return np.nan, np.nan # IV Rank, IV Percentile

        # 只取最近window_days的IV数据进行排名
        # 计算截止日期（当前日期减去window_days天）
        if isinstance(historical_iv_series.index, pd.DatetimeIndex):
            cutoff_date = historical_iv_series.index[-1] - pd.Timedelta(days=window_days)
            series_for_rank = historical_iv_series[historical_iv_series.index >= cutoff_date]
        else:
            # 如果不是日期索引，直接取最后window_days个数据点
            series_for_rank = historical_iv_series.iloc[-min(window_days, len(historical_iv_series)):]
            
        if len(series_for_rank) < 2 : # 数据太少无法排名
             return np.nan, np.nan

        min_iv = series_for_rank.min()
        max_iv = series_for_rank.max()

        if max_iv == min_iv: # 避免除零
            iv_rank = 50.0 if current_iv == min_iv else (100.0 if current_iv > min_iv else 0.0)
        else:
            iv_rank = (current_iv - min_iv) / (max_iv - min_iv) * 100
            iv_rank = max(0, min(100, iv_rank)) # 确保在0-100之间

        # IV Percentile: 当前IV在历史序列中的百分位
        # (比多少历史IV值高)
        iv_percentile = np.sum(current_iv >= series_for_rank) / len(series_for_rank) * 100
        
        return iv_rank, iv_percentile

    def get_iv_trend(self, historical_iv_series: pd.Series, short_window=5, long_window=10):
        if historical_iv_series.empty or len(historical_iv_series) < long_window:
            return {"main_trend": "未知", "above_short_ma": False}

        current_iv = historical_iv_series.iloc[-1]
        # 增加更短时间窗口的均线(3天)
        sma_ultra_short = historical_iv_series.rolling(window=3, min_periods=1).mean().iloc[-1]
        sma_short = historical_iv_series.rolling(window=short_window, min_periods=1).mean().iloc[-1]
        sma_long = historical_iv_series.rolling(window=long_window, min_periods=1).mean().iloc[-1]

        # 获取前一天的IV，用于判断短期动量
        yesterday_iv = historical_iv_series.iloc[-2] if len(historical_iv_series) > 1 else current_iv

        # 无论主趋势如何，额外判断IV是否高于短期均线
        is_above_short_ma = current_iv > sma_ultra_short or current_iv > sma_short
        
        # 主趋势判断
        main_trend = "盘整"  # 默认状态
        
        # 根据IV与不同均线的关系，细分趋势强度
        if current_iv > yesterday_iv and current_iv > sma_ultra_short and sma_ultra_short > sma_short and sma_short > sma_long:
            main_trend = "强势上升趋势"  # 最强上升：IV上涨且高于所有均线，且均线依次递增
        elif current_iv > sma_ultra_short and sma_ultra_short > sma_short and sma_short > sma_long:
            main_trend = "上升趋势"      # 标准上升：IV高于所有均线，且均线依次递增
        elif current_iv > sma_short and sma_short > sma_long:
            main_trend = "温和上升趋势"  # 温和上升：IV高于5天和10天均线，5天高于10天
        elif current_iv > sma_ultra_short and sma_ultra_short > sma_short:
            main_trend = "短期上升趋势"  # 短期上升：IV高于3天和5天均线，3天高于5天
        elif current_iv < yesterday_iv and current_iv < sma_ultra_short and sma_ultra_short < sma_short and sma_short < sma_long:
            main_trend = "强势下降趋势"  # 最强下降：IV下跌且低于所有均线，且均线依次递减
        elif current_iv < sma_ultra_short and sma_ultra_short < sma_short and sma_short < sma_long:
            main_trend = "下降趋势"      # 标准下降：IV低于所有均线，且均线依次递减
        elif current_iv < sma_short and sma_short < sma_long:
            main_trend = "温和下降趋势"  # 温和下降：IV低于5天和10天均线，5天低于10天
        elif current_iv < sma_ultra_short and sma_ultra_short < sma_short:
            main_trend = "短期下降趋势"  # 短期下降：IV低于3天和5天均线，3天低于5天
        elif current_iv > sma_long and current_iv > sma_short:
            main_trend = "高于均线上升中" # IV高于5天和10天均线，但不满足上升趋势条件
        elif current_iv > sma_long:
            main_trend = "高于长期均线"   # IV仅高于10天均线
        elif current_iv > sma_ultra_short or current_iv > sma_short:
            main_trend = "高于短期均线"    # IV大于3日均线或5日均线中的任意一个
        elif current_iv < sma_long and current_iv < sma_short:
            main_trend = "低于均线下降中" # IV低于5天和10天均线，但不满足下降趋势条件
        elif current_iv < sma_long:
            main_trend = "低于长期均线"   # IV仅低于10天均线
            
        # 返回字典，包含主趋势和是否高于短期均线的信息
        return {
            "main_trend": main_trend,
            "above_short_ma": is_above_short_ma
        }

    def calculate_volatility_skew_kurtosis(self, underlying_futures_code, option_date, window_days=30):
        """计算波动率微笑、偏度和峰度
        
        Args:
            underlying_futures_code: 标的期货代码
            option_date: 分析日期
            window_days: 分析窗口天数
            
        Returns:
            dict: 包含波动率微笑特征的字典
        """
        # 找到所有与该期货相关的活跃期权
        option_files = glob.glob(os.path.join(self.loader.options_dir, "*.csv"))
        related_options = []
        
        for file_path in option_files:
            option_code = os.path.basename(file_path).replace(".csv", "")
            parsed_info = self.contract_info_parser.parse_option_code(option_code)
            
            if not parsed_info:
                continue
                
            # 检查是否为同一标的期货
            if parsed_info['underlying_futures_code'] == underlying_futures_code:
                # 检查是否在有效期内
                option_df = self.loader.load_option_data(option_code)
                if not option_df.empty and option_date in option_df['date'].values:
                    # 计算到期日并确认期权未到期
                    option_expiry = self.contract_info_parser.get_option_expiry_date(parsed_info, option_date)
                    if option_expiry > option_date:
                        # 只选择近月期权(DTE < 90)进行分析
                        tte = self.calculator.calculate_tte(option_date, option_expiry)
                        if tte * 365.25 < 90:
                            related_options.append((option_code, parsed_info))
        
        if not related_options:
            print(f"无法找到与{underlying_futures_code}相关的有效期权")
            return None
        
        # 获取标的期货当日价格
        futures_df = self.loader.load_futures_data(underlying_futures_code)
        if futures_df.empty:
            return None
        
        futures_price = futures_df[futures_df['date'] == option_date]['close'].iloc[0]
        
        # 收集不同行权价的IV数据
        call_iv_data = []  # [(行权价/标的价格比率, IV)]
        put_iv_data = []   # [(行权价/标的价格比率, IV)]
        
        for option_code, parsed_info in related_options:
            strike_ratio = parsed_info['strike_price'] / futures_price
            
            # 计算IV
            option_df = self.loader.load_option_data(option_code)
            option_data = option_df[option_df['date'] == option_date]
            if option_data.empty:
                continue
                
            option_price = option_data['close'].iloc[0]
            option_expiry = self.contract_info_parser.get_option_expiry_date(parsed_info, option_date)
            tte = self.calculator.calculate_tte(option_date, option_expiry)
            
            try:
                iv = self.calculator.calculate_implied_volatility(
                    option_price_market=option_price,
                    S=futures_price,
                    K=parsed_info['strike_price'],
                    T=tte,
                    r=self.calculator.risk_free_rate,
                    option_type=parsed_info['option_type']
                )
                
                if not pd.isna(iv):
                    if parsed_info['option_type'] == 'C':
                        call_iv_data.append((strike_ratio, iv))
                    else:
                        put_iv_data.append((strike_ratio, iv))
            except:
                continue
        
        # 确保有足够数据进行分析
        if len(call_iv_data) < 3 and len(put_iv_data) < 3:
            return None
        
        # 计算波动率偏度和峰度
        skew_metrics = {}
        
        # 1. 简单的25Delta-ATM-25Delta波动率差异(常用偏度指标)
        if len(call_iv_data) >= 3 and len(put_iv_data) >= 3:
            # 按行权价比率排序
            call_iv_data.sort(key=lambda x: x[0])
            put_iv_data.sort(key=lambda x: x[0])
            
            # 找到最接近ATM的期权
            atm_index_call = min(range(len(call_iv_data)), key=lambda i: abs(call_iv_data[i][0] - 1.0))
            atm_index_put = min(range(len(put_iv_data)), key=lambda i: abs(put_iv_data[i][0] - 1.0))
            
            atm_iv = (call_iv_data[atm_index_call][1] + put_iv_data[atm_index_put][1]) / 2
            
            # 尝试找到25Delta期权(约为80%和120%行权价)的IV
            otm_call_index = min(range(len(call_iv_data)), key=lambda i: abs(call_iv_data[i][0] - 1.2))
            otm_put_index = min(range(len(put_iv_data)), key=lambda i: abs(put_iv_data[i][0] - 0.8))
            
            otm_call_iv = call_iv_data[otm_call_index][1]
            otm_put_iv = put_iv_data[otm_put_index][1]
            
            # 计算偏度(正值表示下行风险溢价更高，负值表示上行风险溢价更高)
            skew = otm_put_iv - otm_call_iv
            skew_metrics['ivSkew'] = skew
            skew_metrics['ivSkewNormalized'] = skew / atm_iv  # 归一化偏度
            
            # 计算微笑陡峭度(峰度指标)
            butterfly = (otm_call_iv + otm_put_iv) / 2 - atm_iv
            skew_metrics['ivButterfly'] = butterfly
            skew_metrics['ivButterflyNormalized'] = butterfly / atm_iv  # 归一化峰度
        
        # 2. 拟合二阶曲线计算整体偏度和峰度
        all_iv_data = call_iv_data + put_iv_data
        if len(all_iv_data) >= 5:  # 确保有足够多的点进行拟合
            strike_ratios = [x[0] for x in all_iv_data]
            ivs = [x[1] for x in all_iv_data]
            
            try:
                import numpy as np
                from scipy import stats
                
                # 二阶多项式拟合(a*x^2 + b*x + c)
                poly_coefs = np.polyfit(strike_ratios, ivs, 2)
                
                # a系数代表曲率(峰度)，b系数代表斜率(偏度)
                skew_metrics['polySkew'] = poly_coefs[1]  # 一阶系数表示偏度
                skew_metrics['polyKurtosis'] = poly_coefs[0]  # 二阶系数表示峰度
                
                # 统计学偏度和峰度
                skew_metrics['statSkew'] = stats.skew(ivs)
                skew_metrics['statKurtosis'] = stats.kurtosis(ivs)
            except:
                pass
        
        return skew_metrics


# --- 4. 决策逻辑 ---
class OptionBuyingAdvisor:
    def __init__(self, vol_analyzer: VolatilityAnalyzer):
        self.analyzer = vol_analyzer

    def evaluate_buy_opportunity(self, option_code: str, current_trade_date: datetime,
                                 hv_window: int = 10, iv_rank_window_days: int = 365,
                                 assumed_trend: str = "看多" # '看多' -> 买Call, '看空' -> 买Put
                                ):
        print(f"\n=== 开始评估期权 {option_code} 在日期 {current_trade_date.strftime('%Y-%m-%d')} 的买入机会 (假设趋势: {assumed_trend}) ===")

        parsed_info = self.analyzer.contract_info_parser.parse_option_code(option_code)
        if not parsed_info:
            return {"decision": "不交易", "reason": f"无法解析期权代码 {option_code}", "details": {}}

        # 1. 计算标的期货的历史波动率 (HV)
        underlying_futures_code = parsed_info['underlying_futures_code']
        futures_df = self.analyzer.loader.load_futures_data(underlying_futures_code)
        if futures_df.empty or len(futures_df[futures_df['date'] <= current_trade_date]) < hv_window +1:
            return {"decision": "不交易", "reason": f"期货 {underlying_futures_code} 数据不足以计算HV (需要{hv_window+1}条数据点，实际只有{len(futures_df[futures_df['date'] <= current_trade_date])}条)", "details": {}}
        
        futures_df_for_hv = futures_df[futures_df['date'] <= current_trade_date]
        current_hv = self.analyzer.calculator.calculate_historical_volatility(futures_df_for_hv['close'], window=hv_window)
        if pd.isna(current_hv):
            return {"decision": "不交易", "reason": f"无法计算期货 {underlying_futures_code} 的HV", "details": {}}

        # 2. 计算期权的历史IV序列, 当前IV, IV Rank/Percentile, IV Trend
        historical_iv_series = self.analyzer.get_historical_iv_series(option_code, current_trade_date)
        if historical_iv_series.empty or len(historical_iv_series) < 2: # 需要至少2个点来判断趋势和rank
            return {"decision": "不交易", "reason": f"期权 {option_code} 的历史IV数据不足", "details": {"hv": current_hv}}

        current_iv = historical_iv_series.iloc[-1]
        if pd.isna(current_iv):
             return {"decision": "不交易", "reason": f"无法获取期权 {option_code} 的当前IV", "details": {"hv": current_hv}}
             
        # 获取昨日IV，用于比较
        yesterday_iv = None
        if len(historical_iv_series) >= 2:
            yesterday_iv = historical_iv_series.iloc[-2]
            
        # IV与昨日IV比较
        iv_higher_than_yesterday = False
        if yesterday_iv is not None and not pd.isna(yesterday_iv):
            iv_higher_than_yesterday = current_iv > yesterday_iv

        # 获取当日成交量和持仓量
        current_volume = np.nan
        current_open_interest = np.nan
        option_data_for_liquidity = self.analyzer.loader.load_option_data(option_code)
        
        # 添加获取期权价格趋势信息
        current_close = np.nan
        yesterday_close = np.nan
        ma3_close = np.nan
        price_uptrend = False
        
        if not option_data_for_liquidity.empty:
            option_data_sorted = option_data_for_liquidity.sort_values(by='date')
            latest_data = option_data_sorted[option_data_sorted['date'] <= current_trade_date]
            
            if not latest_data.empty:
                current_volume = latest_data['volume'].iloc[-1]
                current_open_interest = latest_data['open_interest'].iloc[-1]
                
                # 计算价格趋势指标
                if len(latest_data) >= 2:  # 至少需要两天数据计算涨跌
                    current_close = latest_data['close'].iloc[-1]
                    yesterday_close = latest_data['close'].iloc[-2]
                    
                    # 计算3日和5日均线(如果有足够数据)
                    ma3_close = np.nan
                    ma5_close = np.nan
                    
                    if len(latest_data) >= 3:
                        ma3_close = latest_data['close'].tail(3).mean()
                    
                    if len(latest_data) >= 5:
                        ma5_close = latest_data['close'].tail(5).mean()
                        
                    # 恢复严格的价格趋势判断逻辑：当前收盘价 > 昨收 且 大于3日均线
                    if len(latest_data) >= 5:
                        # 使用放宽版逻辑：高于昨收且(高于3日均线或高于5日均线)
                        ma5_close = latest_data['close'].tail(5).mean()
                        price_uptrend = (current_close > yesterday_close) and ((current_close > ma3_close) or (current_close > ma5_close))
                    elif len(latest_data) >= 3:
                        # 数据不足5天但有3天，使用严格逻辑
                        price_uptrend = (current_close > yesterday_close) and (current_close > ma3_close)
                    else:
                        # 数据不足3天，仅判断是否高于昨收
                        price_uptrend = (current_close > yesterday_close)

        iv_rank, iv_percentile = self.analyzer.get_iv_rank_percentile(historical_iv_series, current_iv, window_days=iv_rank_window_days)
        iv_trend_info = self.analyzer.get_iv_trend(historical_iv_series)  # 现在返回的是字典

        # 添加波动率偏度/峰度分析
        vol_skew_metrics = self.analyzer.calculate_volatility_skew_kurtosis(
            underlying_futures_code=underlying_futures_code,
            option_date=current_trade_date
        )

        # 获取期权合约的参数用于计算希腊字母
        S_underlying = None
        # 从 merged_df 或 futures_df 获取最新的标的收盘价
        # 确保 futures_df 已经根据 current_trade_date 筛选或直接使用最新的
        futures_data_for_greeks = self.analyzer.loader.load_futures_data(parsed_info['underlying_futures_code'])
        if not futures_data_for_greeks.empty:
            latest_futures_data = futures_data_for_greeks[futures_data_for_greeks['date'] <= current_trade_date]
            if not latest_futures_data.empty:
                S_underlying = latest_futures_data['close'].iloc[-1]
        
        K_strike = parsed_info['strike_price']
        # T_expiry_actual需要从get_option_expiry_date获取
        option_expiry_date = self.analyzer.contract_info_parser.get_option_expiry_date(parsed_info, current_trade_date)
        T_expiry = self.analyzer.calculator.calculate_tte(current_trade_date, option_expiry_date)
        
        # 计算希腊字母
        delta_val = gamma_val = theta_daily_val = vega_val = theta_vega_ratio = None
        if S_underlying is not None and not pd.isna(current_iv) and T_expiry > 0:
            try:
                risk_free_rate = 0.02  # 默认无风险利率
                delta_val = self.analyzer.calculator.delta(S_underlying, K_strike, T_expiry, risk_free_rate, current_iv, parsed_info['option_type'])
                gamma_val = self.analyzer.calculator.gamma(S_underlying, K_strike, T_expiry, risk_free_rate, current_iv)
                theta_val = self.analyzer.calculator.theta(S_underlying, K_strike, T_expiry, risk_free_rate, current_iv, parsed_info['option_type'])
                theta_daily_val = theta_val / 365.25  # 转换为每日时间衰减
                vega_val = self.analyzer.calculator.vega(S_underlying, K_strike, T_expiry, risk_free_rate, current_iv)
                
                # 计算Theta/Vega比率
                if vega_val and vega_val != 0:
                    theta_vega_ratio = abs(theta_daily_val) / vega_val
                else:
                    theta_vega_ratio = None
            except Exception as e:
                print(f"计算希腊字母时出错: {e}")
        
        # 计算DTE
        dte_val = T_expiry * 365.25 if T_expiry else None
        
        # 获取期权类型用于希腊字母分析
        option_type_greek = parsed_info['option_type']
        
        # 构建详细信息字典
        details = {
            "underlying_futures": underlying_futures_code,
            "option_type": parsed_info['option_type'],
            "strike": K_strike,
            "current_hv_underlying": f"{current_hv:.2%}" if not pd.isna(current_hv) else "N/A",
            "current_iv_option": f"{current_iv:.2%}" if not pd.isna(current_iv) else "N/A",
            "yesterday_iv_option": f"{yesterday_iv:.2%}" if not pd.isna(yesterday_iv) else "N/A",
            "iv_higher_than_yesterday": iv_higher_than_yesterday,
            "iv_rank": f"{iv_rank:.0f}%" if not pd.isna(iv_rank) else "N/A",
            "iv_percentile": f"{iv_percentile:.0f}%" if not pd.isna(iv_percentile) else "N/A",
            "iv_trend": iv_trend_info["main_trend"],
            "iv_above_short_ma": iv_trend_info["above_short_ma"],
            "delta": f"{delta_val:.4f}" if pd.notna(delta_val) else "N/A",
            "gamma": f"{gamma_val:.4f}" if pd.notna(gamma_val) else "N/A",
            "theta_daily": f"{theta_daily_val:.4f}" if pd.notna(theta_daily_val) else "N/A",
            "vega": f"{vega_val:.4f}" if pd.notna(vega_val) else "N/A",
            "dte": f"{dte_val:.0f}" if pd.notna(dte_val) else "N/A",
            "theta_vega_ratio": f"{theta_vega_ratio:.2f}" if pd.notna(theta_vega_ratio) else "N/A",
            "current_volume": f"{current_volume:.0f}" if pd.notna(current_volume) else "N/A",
            "current_open_interest": f"{current_open_interest:.0f}" if pd.notna(current_open_interest) else "N/A",
            # 添加价格趋势相关信息
            "price_uptrend": price_uptrend,
            "current_close": f"{current_close:.2f}" if pd.notna(current_close) else "N/A",
            "yesterday_close": f"{yesterday_close:.2f}" if pd.notna(yesterday_close) else "N/A",
            "ma3_close": f"{ma3_close:.2f}" if pd.notna(ma3_close) else "N/A",
            "ma5_close": f"{ma5_close:.2f}" if pd.notna(ma5_close) else "N/A"
        }

        # 将波动率结构特征添加到details
        if vol_skew_metrics:
            details.update({
                "iv_skew": f"{vol_skew_metrics.get('ivSkew', 'N/A'):.2%}" if 'ivSkew' in vol_skew_metrics else "N/A",
                "iv_butterfly": f"{vol_skew_metrics.get('ivButterfly', 'N/A'):.2%}" if 'ivButterfly' in vol_skew_metrics else "N/A",
                "iv_skew_normalized": f"{vol_skew_metrics.get('ivSkewNormalized', 'N/A'):.2f}" if 'ivSkewNormalized' in vol_skew_metrics else "N/A",
                "iv_butterfly_normalized": f"{vol_skew_metrics.get('ivButterflyNormalized', 'N/A'):.2f}" if 'ivButterflyNormalized' in vol_skew_metrics else "N/A"
            })

        # 决策逻辑 (基于评分或规则)
        score = 0
        reasons = []

        # --- 原有评分逻辑开始 ---
        # IV vs HV (核心比较)
        if not pd.isna(current_iv) and not pd.isna(current_hv):
            if current_iv < current_hv * 0.85:
                score += 2
                reasons.append("IV显著低于HV (便宜)")
            elif current_iv < current_hv:
                score += 1
                reasons.append("IV低于HV (较便宜)")
            elif current_iv > current_hv * 1.2: # IV 略高于HV太多则谨慎
                score -= 1.0
                reasons.append("IV略高于HV")
            if current_iv > current_hv * 1.5: # IV 远高于HV则非常不利
                score -=1.5
                reasons.append("IV远高于HV (昂贵)")
                
        # IV今日与昨日比较（非常重要的买方信号）
        if iv_higher_than_yesterday:
            score += 2.0  # 今日IV上涨，对买方非常有利
            reasons.append("IV高于昨日")
        else:
            score -= 1.5  # 今日IV下跌，对买方不利
            reasons.append("IV低于昨日")

        # 对于买方策略来说，我们希望有IV趋势上升、IV低于HV的组合，这是最有利的情况
        # 如果IV显著低于HV，但IV趋势下降，可能是个陷阱，调整评分
        if "IV显著低于HV (便宜)" in reasons and (iv_trend_info["main_trend"] == "下降趋势" or iv_trend_info["main_trend"] == "低于长期均线下降中"):
            score -= 1.0  # 减少这种组合的评分
            reasons.append("警告:IV便宜但趋势下降")

        # IV Rank
        if not pd.isna(iv_rank):
            if iv_rank < 25:
                score += 2
                reasons.append(f"IV Rank低 ({iv_rank:.0f}%)")
            elif iv_rank < 50:
                score += 1
                reasons.append(f"IV Rank中等偏低 ({iv_rank:.0f}%)")
            elif iv_rank > 75:
                score -=1.5
                reasons.append(f"IV Rank高 ({iv_rank:.0f}%)")

        # IV Trend - 使用main_trend
        iv_main_trend = iv_trend_info["main_trend"]
        if iv_main_trend == "强势上升趋势":
            score += 3.0  # 最强上升趋势给予更高权重
            reasons.append("IV强势上升")
        elif iv_main_trend == "上升趋势":
            score += 2.5  
            reasons.append("IV趋势上升")
        elif iv_main_trend == "温和上升趋势" or iv_main_trend == "短期上升趋势":
            score += 2.0
            reasons.append(f"IV{iv_main_trend}")
        elif iv_main_trend == "高于均线上升中":
            score += 1.5  
            reasons.append("IV高于均线上升")
        elif iv_main_trend == "强势下降趋势":
            score -= 3.0  # 最强下降趋势更大幅减分
            reasons.append("IV强势下降")
        elif iv_main_trend == "下降趋势":
            score -= 2.5  
            reasons.append("IV趋势下降")
        elif iv_main_trend == "温和下降趋势" or iv_main_trend == "短期下降趋势":
            score -= 2.0
            reasons.append(f"IV{iv_main_trend}")
        elif iv_main_trend == "低于均线下降中":
            score -= 1.5
            reasons.append("IV低于均线下降")
        
        # 额外检查IV是否高于短期均线
        if iv_trend_info["above_short_ma"]:
            score += 1.0  # 额外加分
            reasons.append("IV高于短期均线")
        else:
            score -= 0.5  # 轻微减分
            reasons.append("IV低于短期均线")
        # --- 原有评分逻辑结束 ---

        # --- 新增基于希腊字母的评分调整 ---
        # 1. Delta 策略适应性 (可根据用户偏好选择 profile: "neutral_vol" 或 "directional_vol")
        # 这里我们简单示例，假设以 "neutral_vol" 为主，鼓励接近平值的期权
        if pd.notna(delta_val):
            if option_type_greek == 'C':
                if 0.4 <= delta_val <= 0.6:
                    score += 0.5
                    reasons.append(f"Delta ({delta_val:.2f}) 适中，利于波动率策略")
                elif delta_val < 0.25 or delta_val > 0.75: # 深度虚值或实值，对纯波动率策略可能不是最优
                    score -= 0.25
                    reasons.append(f"Delta ({delta_val:.2f}) 偏离中性")
            elif option_type_greek == 'P':
                if -0.6 <= delta_val <= -0.4:
                    score += 0.5
                    reasons.append(f"Delta ({delta_val:.2f}) 适中，利于波动率策略")
                elif delta_val > -0.25 or delta_val < -0.75:
                    score -= 0.25
                    reasons.append(f"Delta ({delta_val:.2f}) 偏离中性")
        
        # 2. Theta 风险管理
        # 对到期时间为0或N/A的期权直接返回不买入决策，不进行后续评分
        if not pd.notna(dte_val) or str(details.get('dte', 'N/A')) == 'N/A' or dte_val <= 0:
            return {"decision": "不买入", "reason": f"DTE无效或为0天，期权已到期或即将到期，风险极高", "details": details}
        
        # 处理有效的DTE值
        if pd.notna(dte_val):
            # 对到期时间极短的期权直接返回不买入决策，不进行后续评分
            if dte_val <= 2:  # 剩余2天或更少，风险极高，直接不推荐
                return {"decision": "不买入", "reason": f"DTE过短 ({dte_val:.0f}天)，即将到期，风险极高", "details": details}
            elif dte_val < 10: # 剩余10天以内，风险较高
                score -= 1.0
                reasons.append(f"警告: DTE极短 ({dte_val:.0f}天)，Theta衰减快，风险极高")
            elif dte_val < 20: # 10 <= dte_val < 20
                score -= 0.5
                reasons.append(f"注意: DTE较短 ({dte_val:.0f}天)，Theta影响增加，时间价值损耗快")
            # 20 <= dte_val <= 60 天是较优范围 (0分)
            elif dte_val > 120: # DTE过长，超过120天
                score -= 2.0 # 显著减分，以实现不推荐买入的效果
                reasons.append(f"警告: DTE过长 ({dte_val:.0f}天)，弹性差/权利金过高，一般不推荐买入")
            elif dte_val > 60: # 60 < dte_val <= 120 天
                score -= 0.5 # 从-0.25调整为-0.5
                reasons.append(f"注意: DTE偏长 ({dte_val:.0f}天)，弹性下降/权利金较高，风险增加")
            # else: 20 <= dte_val <= 60 天，作为理想区间，不加分也不扣分

        if pd.notna(theta_vega_ratio):
            # Theta通常为负，Vega为正。theta_vega_ratio 一般为负。
            # 绝对值越大，代表每日Theta损失相对于Vega收益越大，对买方不利。
            if abs(theta_vega_ratio) > 0.3: # 比如，每日Theta是Vega的30%以上
                score -= 0.5
                reasons.append(f"Theta/Vega比率 ({theta_vega_ratio:.2f})较高，时间价值损耗相对Vega较大")
            elif abs(theta_vega_ratio) < 0.1: # Theta影响相对较小
                score += 0.25
                reasons.append(f"Theta/Vega比率 ({theta_vega_ratio:.2f})较低，时间价值损耗相对Vega较小")
        # --- 希腊字母评分调整结束 ---

        # --- 新增流动性评分 --- (V1.5.9)
        # 最低成交量阈值
        MIN_VOLUME_THRESHOLD = 100
        # 最低持仓量阈值
        MIN_OPEN_INTEREST_THRESHOLD = 200
        # 良好流动性成交量阈值
        GOOD_VOLUME_THRESHOLD = 500
        # 良好流动性持仓量阈值
        GOOD_OPEN_INTEREST_THRESHOLD = 1000
        # 极低流动性阈值（新增）
        EXTREMELY_LOW_VOLUME_THRESHOLD = 20
        # 极低持仓量阈值（新增）
        EXTREMELY_LOW_OPEN_INTEREST_THRESHOLD = 50

        if pd.notna(current_volume) and pd.notna(current_open_interest):
            liquidity_issues = []
            volume_insufficient = current_volume < MIN_VOLUME_THRESHOLD
            open_interest_insufficient = current_open_interest < MIN_OPEN_INTEREST_THRESHOLD
            
            # 检查极低流动性条件（新增）
            extremely_low_liquidity = current_volume < EXTREMELY_LOW_VOLUME_THRESHOLD or current_open_interest < EXTREMELY_LOW_OPEN_INTEREST_THRESHOLD
            
            if extremely_low_liquidity:
                # 极低流动性直接返回不交易决策
                return {"decision": "不买入", "reason": f"流动性极低 (成交量:{current_volume:.0f} < {EXTREMELY_LOW_VOLUME_THRESHOLD} 或 持仓量:{current_open_interest:.0f} < {EXTREMELY_LOW_OPEN_INTEREST_THRESHOLD})", "details": details}
            
            if volume_insufficient:
                liquidity_issues.append(f"成交量:{current_volume:.0f} < {MIN_VOLUME_THRESHOLD}")
            if open_interest_insufficient:
                liquidity_issues.append(f"持仓量:{current_open_interest:.0f} < {MIN_OPEN_INTEREST_THRESHOLD}")

            if liquidity_issues:
                score -= 1.5 # 流动性不足，显著减分
                reasons.append(f"流动性不足 ({' 且 '.join(liquidity_issues)})")
            elif current_volume > GOOD_VOLUME_THRESHOLD and current_open_interest > GOOD_OPEN_INTEREST_THRESHOLD:
                score += 0.5 # 流动性良好，适度加分
                reasons.append(f"流动性良好 (成交量:{current_volume:.0f}, 持仓量:{current_open_interest:.0f})")
            # 其他中间情况不加不减分
        else:
            # 如果无法获取成交量或持仓量数据，也视为流动性风险，给予减分
            score -= 0.5 
            reasons.append("警告: 未能获取当日成交量或持仓量数据")

        # --- 新增波动率结构评分 --- (V1.7.0)
        if vol_skew_metrics and not pd.isna(current_iv):
            # 1. 利用偏度进行策略优化
            if 'ivSkew' in vol_skew_metrics and pd.notna(vol_skew_metrics['ivSkew']):
                skew = vol_skew_metrics['ivSkew']
                
                # 策略调整：看多时，负偏度(上行风险溢价高)更有利
                if assumed_trend == "看多" and skew < -0.05:
                    score += 0.75
                    reasons.append("波动率负偏度显著，市场高估上行风险，利于看多")
                # 看空时，正偏度(下行风险溢价高)更有利
                elif assumed_trend == "看空" and skew > 0.05:
                    score += 0.75
                    reasons.append("波动率正偏度显著，市场高估下行风险，利于看空")
                # 极端偏度可能意味着市场预期明显，可能不利于逆势
                elif (assumed_trend == "看多" and skew > 0.1) or (assumed_trend == "看空" and skew < -0.1):
                    score -= 1.0
                    reasons.append("波动率偏度与交易方向相反，市场预期与您相反")
                    
            # 2. 利用峰度(butterfly)进行策略优化
            if 'ivButterfly' in vol_skew_metrics and pd.notna(vol_skew_metrics['ivButterfly']):
                butterfly = vol_skew_metrics['ivButterfly']
                
                # 高峰度表明市场预期大幅波动，买方可能有优势
                if butterfly > 0.03:  # 显著高峰度
                    score += 0.5
                    reasons.append("波动率高峰度，市场预期大幅波动，有利于期权买方")
                # 低峰度表明市场预期平稳，买方不利
                elif butterfly < -0.01:  # 平坦或反向峰度
                    score -= 0.75
                    reasons.append("波动率低峰度，市场预期低波动，不利于期权买方")

        # 最终决策 (阈值可调)
        decision_str = "不买入"
        
        # 添加价格趋势判断逻辑 - 当价格下跌时限制推荐级别
        price_downtrend = False
        if pd.notna(current_close) and pd.notna(yesterday_close):
            price_downtrend = current_close < yesterday_close
            
        # 检查新的价格趋势条件是否满足
        price_trend_satisfied = price_uptrend  # 使用已计算的price_uptrend变量
        
        # 根据评分和价格趋势确定决策级别
        if price_downtrend:
            # 当价格下跌时，无论评分多高，最多只能"谨慎买入"
            if score >= 0.5:
                decision_str = "谨慎买入(风险较高)"
                reasons.append("期权价格低于昨日收盘价，降低推荐级别")
        elif not price_trend_satisfied:
            # 当价格趋势条件不满足时（价格未大于昨收且大于3日或5日均线），限制推荐级别
            if score >= 0.5:
                decision_str = "谨慎买入(风险较高)"
                reasons.append("价格趋势条件不满足(需大于昨收且大于3日或5日均线)，降低推荐级别")
        else:
            # 价格趋势满足条件时，正常评分逻辑
            if score >= 4.0:
                decision_str = "强烈建议买入"
            elif score >= 2.0:  # 提高门槛，从1.5提高到2.0
                decision_str = "可以考虑买入"
            # 添加一个新的警告级别
            elif score >= 0.5:
                decision_str = "谨慎买入(风险较高)"
        
        final_reason = f"综合评分: {score:.1f}. " + ", ".join(reasons) if reasons else "无明显买入信号"
        if not reasons and score < 2.0 : final_reason = f"综合评分: {score:.1f}. 波动率条件不佳。"


        return {"decision": decision_str, "reason": final_reason, "details": details}


# --- 5. 主程序流程 ---
if __name__ == '__main__':
    # 加载配置
    config = Config()
    
    # 从配置文件获取路径和参数
    FUTURES_DATA_DIR = config.get('data_paths.futures_data_dir')
    OPTIONS_DATA_DIR = config.get('data_paths.options_data_dir')
    TREND_FILE = config.get('data_paths.trend_file')
    OUTPUT_DIR = config.get('data_paths.output_dir')
    RISK_FREE_RATE = config.get('parameters.risk_free_rate', 0.02)
    
    # 确保输出目录存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"创建输出目录: {OUTPUT_DIR}")
        
    # 定义输出Excel文件路径
    OUTPUT_EXCEL_FILE = os.path.join(OUTPUT_DIR, "期权交易建议与说明.xlsx")

    # 初始化服务
    data_loader = DataLoader(futures_dir=FUTURES_DATA_DIR, options_dir=OPTIONS_DATA_DIR)
    option_calculator = OptionCalculations(risk_free_rate=RISK_FREE_RATE, config=config)
    vol_analyzer = VolatilityAnalyzer(data_loader=data_loader, option_calc=option_calculator)
    advisor = OptionBuyingAdvisor(vol_analyzer=vol_analyzer)

    print("*"*10 + " 期权买方交易系统 - 波动率分析 " + "*"*10)
    print("重要提示: 本系统假设趋势已由您判断。决策主要基于波动率条件。")
    print("重要提示: 期权到期日的准确获取对IV计算至关重要，请确保OptionContractInfo类中get_option_expiry_date方法准确实现！")
    print(f"分析结果将保存到: {OUTPUT_EXCEL_FILE}")
    
    # 加载趋势文件
    trend_df = None
    try:
        trend_df = pd.read_csv(TREND_FILE, encoding='utf-8-sig')
        print(f"成功加载趋势文件，共 {len(trend_df)} 条记录")
        # 显示趋势文件样例
        print("趋势文件样例:")
        print(trend_df.head(3))
    except Exception as e:
        error_msg = f"加载趋势文件失败: {e}"
        print(error_msg)
        raise DataLoadError(error_msg)
        
    # 创建品种趋势映射字典
    symbol_trend_map = {}

    # 不再使用全局最新日期，而是为每个品种取其最新记录
    print("\n使用每个品种各自最新日期的趋势数据，直接从symbol和operation列映射")

    # 按symbol分组，然后取每组的最新记录
    latest_trends = trend_df.sort_values('datetime').groupby('symbol').last().reset_index()

    for _, row in latest_trends.iterrows():
        # 从symbol列获取品种代码，大写处理保证匹配一致性
        symbol = str(row['symbol']).strip().upper() # 直接使用 'symbol'
        # 直接从operation列获取趋势值
        operation = str(row['operation']).strip() # 直接使用 'operation'
        
        # 标准化处理操作值
        if operation == '多多多' or operation == '多头':
            operation = "多多多"
        elif operation == '多':
            operation = "多" 
        elif operation == '空' or operation == '空头':
            operation = "空"
        elif operation == '0' or operation == '' or operation == '震荡':
            operation = "0"
            
        # 添加基本映射，每个品种就是它自己
        symbol_trend_map[symbol] = operation
        print(f"添加趋势映射: {symbol} -> {operation}")
        
        # 特殊监控PS和SI
        if symbol == "PS":
            print(f"★★★ 发现PS品种，趋势为{operation}，已添加到映射字典")
        elif symbol == "SI":
            print(f"★★★ 发现SI品种，趋势为{operation}，已添加到映射字典")

    print(f"\n已加载各品种各自的最新趋势数据，共 {len(symbol_trend_map)} 个品种")
    print("部分趋势数据:")
    items_to_show = list(symbol_trend_map.items())[:10]  # 显示前10个
    for symbol, trend in items_to_show:
        print(f"{symbol}: {trend}")

    # 打印详细的趋势映射信息进行调试
    print("\n所有品种趋势信息:")
    for symbol, trend in symbol_trend_map.items():
        print(f"{symbol}: {trend}")
        
    # PS/SI的特殊检查逻辑已于2024-07-30移除，以简化处理流程并依赖趋势文件中的直接大写匹配。

    # 不需要检查特殊品种

    # 获取所有期权csv文件
    option_files = glob.glob(os.path.join(OPTIONS_DATA_DIR, "*.csv"))
    if not option_files:
        error_msg = f"未在 {OPTIONS_DATA_DIR} 目录下找到任何期权CSV文件"
        print(f"错误: {error_msg}")
        raise DataLoadError(error_msg)

    # 提取期权合约代码和月份信息
    option_codes = []
    skipped_files = []
    all_options = []
    
    for file_path in option_files:
        option_code = os.path.basename(file_path).replace(".csv", "")
        all_options.append(option_code)
        # 尝试解析代码以获取到期月份和品种
        try:
            # 期权代码格式通常为 XXX2506-C-YYYY
            parts = option_code.split('-')
            if len(parts) >= 1:
                base_code = parts[0]  # 例如 "AG2506" 或 "TA2507"
                
                # 提取品种代码，通常是字母部分
                symbol = ''.join([c for c in base_code if not c.isdigit()]).upper()  # 如"AG"或"TA"，转为大写
                
                month_part = base_code[-4:]  # 例如 "2506"
                if month_part.isdigit():
                    year = 2000 + int(month_part[:2])  # 2500 -> 2025年
                    month = int(month_part[2:])  # 06 -> 6月
                    # 仅保留近四个月的期权
                    current_date = datetime.now()
                    target_date = datetime(year, month, 15)  # 假设每月15日为参考日
                    months_diff = (year - current_date.year) * 12 + (month - current_date.month)
                    
                    # 调试输出
                    if months_diff >= 0 and months_diff <= 4:
                        print(f"期权合约: {option_code}, 品种: {symbol}, 趋势匹配: {symbol in symbol_trend_map}")

                            
                    if 0 <= months_diff <= 4:  # 最近4个月内的期权
                        # 检查该品种是否在趋势文件中
                        if symbol in symbol_trend_map:
                            trend_operation = symbol_trend_map[symbol]
                            # 特殊监控PS和SI期权的映射情况
                            if symbol == "PS" or symbol == "SI":
                                print(f"★★★ 期权{option_code}匹配到品种{symbol}，趋势值为{trend_operation}")
                            
                            # 确定应该分析的期权类型
                            if trend_operation == "多多多" and "-C-" in option_code:  # 多头趋势，只分析认购期权
                                option_codes.append((option_code, file_path, symbol, "看多"))
                                print(f"添加多头期权: {option_code} ({symbol})")
                            elif trend_operation == "空" and "-P-" in option_code:  # 空头趋势，只分析认沽期权
                                option_codes.append((option_code, file_path, symbol, "看空"))
                                print(f"添加空头期权: {option_code} ({symbol})")
                            elif trend_operation == "0":  # 0表示观望
                                # 也分析这些合约，但标记为观望状态
                                if "-C-" in option_code:
                                    option_codes.append((option_code, file_path, symbol, "观望"))
                                    print(f"添加观望期权(多): {option_code} ({symbol})")
                                elif "-P-" in option_code:
                                    option_codes.append((option_code, file_path, symbol, "观望"))
                                    print(f"添加观望期权(空): {option_code} ({symbol})")
                        else:
                            # 特殊监控PS和SI的情况
                            if symbol == "PS" or symbol == "SI":
                                print(f"警告: ★★★ 特殊品种 {symbol} 不在趋势文件中，symbol_trend_map中的键: {list(symbol_trend_map.keys())}")
                            print(f"警告: 品种 {symbol} 不在趋势文件中，跳过: {option_code}")
                else:
                    skipped_files.append(f"{option_code} (无效月份)")
            else:
                skipped_files.append(f"{option_code} (格式错误)")
        except Exception as e:
            print(f"无法解析期权代码 {option_code}: {e}")
            skipped_files.append(f"{option_code} (解析错误)")
            continue

    if not option_codes:
        error_msg = "未找到适合分析的期权合约"
        print(error_msg)
        print("跳过的文件:", skipped_files[:20])  # 显示部分跳过的文件
        raise DataLoadError(error_msg)

    print(f"\n共找到 {len(option_codes)} 个符合条件的期权合约进行分析")
    print(f"跳过了 {len(skipped_files)} 个合约")
    
    # 打印前10个将要分析的合约
    print("\n将要分析的合约示例:")
    for i, (code, _, symbol, trend) in enumerate(option_codes[:10]):
        print(f"{i+1}. {symbol} {code} (趋势: {trend})")



    # 用于收集所有结果和推荐合约
    all_results_list = []
    recommendations = []
    
    fieldnames = ['期权代码', '品种', '趋势', '分析日期', '评分', '决策', '理由', 
                 '标的期货', '期权类型', '行权价', 
                 '标的历史波动率', '当前隐含波动率', '昨日隐含波动率', 'IV高于昨日', 
                 'IV Rank', 'IV Percentile', 'IV趋势', 'IV高于短期均线',
                 'Delta', 'Gamma', 'Theta (每日)', 'Vega', 'DTE (天)', 'Theta/Vega比率',
                 '成交量', '持仓量',
                 '波动率偏度', '波动率峰度', '归一化偏度', '归一化峰度',
                 '价格趋势向上', '当前收盘价', '昨日收盘价', '3日均线价格', '5日均线价格']
    
    # 分批处理期权，每批最多500个，避免内存占用过高
    batch_size = config.get('parameters.batch_size', 500)
    total_options = len(option_codes)
    
    # 创建品种-最高分合约字典，用于确保每个品种至少有一个推荐
    symbol_best_options = {}
    # 创建品种与其全部合约的映射字典，用于每个品种强制推荐一个合约
    symbol_all_options = {}
    # 记录已经被推荐的品种
    recommended_symbols = set()
    
    for batch_idx in range(0, total_options, batch_size):
        batch_end = min(batch_idx + batch_size, total_options)
        print(f"\n处理第 {batch_idx+1} 到 {batch_end} 个期权合约 (共{total_options}个)")
        
        for idx, (option_code, file_path, symbol, assumed_trend) in enumerate(option_codes[batch_idx:batch_end]):
            try:
                if idx % 50 == 0:
                    print(f"已完成: {batch_idx + idx}/{total_options}")
                
                # 获取品种趋势
                trend_operation = symbol_trend_map.get(symbol, "未知")
                
                # 加载该期权数据
                option_df = data_loader.load_option_data(option_code)
                if option_df.empty:
                    print(f"跳过期权 {option_code}: 数据为空")
                    continue
                
                # 获取最新日期
                latest_date = option_df['date'].max()
                
                # 评估期权买入机会
                result = advisor.evaluate_buy_opportunity(
                    option_code=option_code,
                    current_trade_date=latest_date,
                    assumed_trend=assumed_trend
                )
                
                # 提取评分，决策通常格式为"强烈建议买入"或"可以考虑买入"或"不买入"
                score = 0
                if "综合评分:" in result['reason']:
                    try:
                        # 修改提取逻辑，获取完整的浮点数评分而非仅整数部分
                        score_str = result['reason'].split("综合评分:")[1].split(".")[0].strip()
                        score_dec = result['reason'].split("综合评分:")[1].split(".")[1].split()[0] if len(result['reason'].split("综合评分:")[1].split(".")) > 1 else "0"
                        score = float(f"{score_str}.{score_dec}")
                    except Exception as e:
                        print(f"提取评分时出错: {e}")
                        pass
                
                # 准备写入的数据行
                row_data = {
                    '期权代码': option_code,
                    '品种': symbol,
                    '趋势': trend_operation,
                    '分析日期': latest_date.strftime('%Y-%m-%d'),
                    '评分': f"{score:.1f}",
                    '决策': result['decision'],
                    '理由': result['reason'],
                    '标的期货': result['details'].get('underlying_futures', 'N/A'),
                    '期权类型': result['details'].get('option_type', 'N/A'),
                    '行权价': result['details'].get('strike', 'N/A'),
                    '标的历史波动率': result['details'].get('current_hv_underlying', 'N/A'),
                    '当前隐含波动率': result['details'].get('current_iv_option', 'N/A'),
                    '昨日隐含波动率': result['details'].get('yesterday_iv_option', 'N/A'),
                    'IV高于昨日': result['details'].get('iv_higher_than_yesterday', 'N/A'),
                    'IV Rank': result['details'].get('iv_rank', 'N/A'),
                    'IV Percentile': result['details'].get('iv_percentile', 'N/A'),
                    'IV趋势': result['details'].get('iv_trend', 'N/A'),
                    'IV高于短期均线': result['details'].get('iv_above_short_ma', 'N/A'),
                    'Delta': result['details'].get('delta', 'N/A'),
                    'Gamma': result['details'].get('gamma', 'N/A'),
                    'Theta (每日)': result['details'].get('theta_daily', 'N/A'),
                    'Vega': result['details'].get('vega', 'N/A'),
                    'DTE (天)': result['details'].get('dte', 'N/A'),
                    'Theta/Vega比率': result['details'].get('theta_vega_ratio', 'N/A'),
                    '成交量': result['details'].get('current_volume', 'N/A'),
                    '持仓量': result['details'].get('current_open_interest', 'N/A'),
                    '波动率偏度': result['details'].get('iv_skew', 'N/A'),
                    '波动率峰度': result['details'].get('iv_butterfly', 'N/A'),
                    '归一化偏度': result['details'].get('iv_skew_normalized', 'N/A'),
                    '归一化峰度': result['details'].get('iv_butterfly_normalized', 'N/A'),
                    '价格趋势向上': result['details'].get('price_uptrend', 'N/A'),
                    '当前收盘价': result['details'].get('current_close', 'N/A'),
                    '昨日收盘价': result['details'].get('yesterday_close', 'N/A'),
                    '3日均线价格': result['details'].get('ma3_close', 'N/A'),
                    '5日均线价格': result['details'].get('ma5_close', 'N/A')
                }
                all_results_list.append(row_data)
                
                # 收集有交易信号的推荐合约
                should_recommend = False
                
                # 检查当前期权类型与趋势是否匹配
                option_type_match = False
                # 获取期权类型
                option_type = result['details'].get('option_type', '')
                # 获取品种趋势
                symbol_from_option = result['details'].get('underlying_futures', '')
                symbol = ''.join([c for c in symbol_from_option if not c.isdigit()]).upper()
                trend_operation = symbol_trend_map.get(symbol, "未知")
                
                print(f"检查期权 {option_code} 的匹配: 品种={symbol}, 趋势={trend_operation}, 类型={option_type}")
                
                if (trend_operation == "多多多" and option_type == 'C'):
                    # 多头趋势，使用认购期权
                    option_type_match = True
                    print(f"多头趋势匹配认购期权: {option_code}")
                elif (trend_operation == "空" and option_type == 'P'):
                    # 空头趋势，使用认沽期权
                    option_type_match = True
                    print(f"空头趋势匹配认沽期权: {option_code}")
                elif trend_operation == "0":
                    # 震荡趋势可以接受两种期权
                    iv_higher = result['details'].get('iv_higher_than_yesterday', False)
                    if (option_type == 'C' and iv_higher):
                        option_type_match = True
                        print(f"震荡趋势+IV上涨匹配认购期权: {option_code}")
                    elif (option_type == 'P' and iv_higher):
                        option_type_match = True
                        print(f"震荡趋势+IV上涨匹配认沽期权: {option_code}")
                
                # 修改后的条件：决策结果 + 期权类型与趋势匹配 + IV高于昨日 + 最低评分要求 + 价格趋势向上
                if ("强烈建议买入" in result['decision'] or "可以考虑买入" in result['decision']):
                    # 获取IV高于昨日的信息
                    iv_higher = result['details'].get('iv_higher_than_yesterday', False)
                    
                    # 获取价格趋势信息
                    price_uptrend = result['details'].get('price_uptrend', False)
                    
                    # 检查DTE条件
                    dte_str = result['details'].get('dte', 'N/A')
                    is_valid_dte = False
                    
                    # 检查DTE是否为数值且大于2天
                    if dte_str != 'N/A' and str(dte_str).replace('.', '', 1).isdigit():
                        dte_val = float(dte_str)
                        is_valid_dte = dte_val > 2
                    
                    # 同时满足五个条件：决策为买入 + 期权类型与趋势匹配 + IV高于昨日 + 评分至少3分 + 价格趋势向上 + DTE > 2天
                    if option_type_match and iv_higher and float(score) >= 3.0 and price_uptrend and is_valid_dte:
                        should_recommend = True
                        print(f"推荐期权 {option_code}: 匹配趋势={trend_operation}, 评分={score}, IV上涨={iv_higher}, 价格上涨={price_uptrend}")
                    
                    # 记录未推荐的原因
                    elif option_type_match and iv_higher and float(score) >= 3.0:
                        if not price_uptrend:
                            print(f"注意: {option_code} 评分{score}未被推荐, 原因: 价格趋势不向上")
                        elif not is_valid_dte:
                            print(f"注意: {option_code} 评分{score}未被推荐, 原因: DTE过短或无效 (DTE={dte_str})")
                    # 其他未推荐的原因也记录下来
                    elif not option_type_match and float(score) >= 3.0:
                        print(f"注意: {option_code} 评分{score}未被推荐, 原因: 期权类型不匹配趋势 (趋势={trend_operation}, 类型={option_type})")
                    elif not iv_higher and float(score) >= 3.0:
                        print(f"注意: {option_code} 评分{score}未被推荐, 原因: IV未高于昨日")

                if should_recommend:
                    recommendations.append(row_data) # 使用相同的row_data结构
                    recommended_symbols.add(symbol)  # 记录已推荐的品种
                    print(f"已添加推荐期权: {option_code}")
                
                # 收集每个品种的最高分合约(包括所有期权，不论是否符合严格推荐条件)
                if option_type_match and "流动性极低" not in result['reason']:
                    # 只考虑与趋势匹配的期权类型，且排除流动性极低的合约
                    
                    # 1. 更新当前品种的所有有效期权列表
                    if symbol not in symbol_all_options:
                        symbol_all_options[symbol] = []
                    
                    # 添加当前合约到该品种的列表
                    symbol_all_options[symbol].append(row_data)
                    
                    # 2. 更新当前品种的最高分合约
                    # 只有在DTE > 2天且为数值的情况下才会考虑为最佳合约
                dte_str = row_data.get('DTE (天)', 'N/A')
                is_valid_dte = dte_str != 'N/A' and float(dte_str) > 2 if dte_str.replace('.', '', 1).isdigit() else False
                
                if is_valid_dte and (symbol not in symbol_best_options or float(score) > float(symbol_best_options[symbol]['评分'])):
                    symbol_best_options[symbol] = row_data
                    print(f"更新品种 {symbol} 的最高分合约: {option_code}, 评分={score}, DTE={dte_str}")
            
            except Exception as e:
                print(f"处理期权 {option_code} 时发生错误: {e}")
                print(traceback.format_exc())

    print(f"\n所有期权分析完成。")

    # 确保每个品种至少有一个推荐合约(但只限于DTE>2天的合约，且该品种尚未有推荐)
    for symbol, best_option in symbol_best_options.items():
        if symbol not in recommended_symbols:
            # 初始化变量确保在所有路径中都被定义
            temp_results = []
            valid_options = []
            
            # 该品种尚未有推荐合约,检查该合约的DTE是否合适
            dte_str = best_option.get('DTE (天)', 'N/A')
            is_valid_dte = False
            if dte_str != 'N/A' and str(dte_str).replace('.', '', 1).isdigit():
                dte_val = float(dte_str)
                is_valid_dte = dte_val > 2
            
            # 只添加DTE>2天的合约
            if is_valid_dte:
                print(f"为品种 {symbol} 添加最高分合约: {best_option['期权代码']}, 评分={best_option['评分']}, DTE={dte_str}")
                recommendations.append(best_option)
                recommended_symbols.add(symbol)
            else:
                print(f"品种 {symbol} 的最高分合约DTE不足，尝试查找其他合适的合约")
                
                # 初始化变量确保在所有路径中都被定义
                temp_results = []
                valid_options = []
                
                # 查找该品种所有合约，找出所有DTE>2天的合约
                all_symbol_options = []
                # 优化：使用已缓存的数据避免重复加载
                symbol_options_found = 0
                for file_path in option_files:
                    option_code = os.path.basename(file_path).replace(".csv", "")
                    try:
                        parts = option_code.split('-')
                        if len(parts) >= 1:
                            # 提取品种代码
                            option_symbol = ''.join([c for c in parts[0] if not c.isdigit()]).upper()
                            
                            # 检查是否为当前品种
                            if option_symbol == symbol:
                                symbol_options_found += 1
                                # 检查是否已在缓存中
                                try:
                                    option_df = data_loader.load_option_data(option_code)
                                    if not option_df.empty:
                                        # 获取最新日期
                                        parsed_info = advisor.analyzer.contract_info_parser.parse_option_code(option_code)
                                        if parsed_info:
                                            option_expiry_date = advisor.analyzer.contract_info_parser.get_option_expiry_date(parsed_info, latest_date)
                                            tte = advisor.analyzer.calculator.calculate_tte(latest_date, option_expiry_date)
                                            dte_days = tte * 365.25
                                            
                                            # 找到DTE>2天的合约
                                            if dte_days > 2:
                                                all_symbol_options.append((option_code, file_path, option_symbol, "备选"))
                                except Exception as e:
                                    print(f"加载期权 {option_code} 失败: {e}")
                    except Exception:
                        continue
                
                # 如果找到了合适的合约，评估并添加最高分的一个
                if all_symbol_options:
                    print(f"为品种 {symbol} 找到 {len(all_symbol_options)} 个DTE>2天的备选合约")
                    # 临时评估这些合约，选择评分最高的
                    temp_results = []
                    for opt_code, _, _, _ in all_symbol_options:
                        opt_df = data_loader.load_option_data(opt_code)
                        if not opt_df.empty:
                            opt_latest_date = opt_df['date'].max()
                            # 评估该合约
                            result = advisor.evaluate_buy_opportunity(
                                option_code=opt_code,
                                current_trade_date=opt_latest_date,
                                assumed_trend="看多" if "-C-" in opt_code else "看空"
                            )
                            # 提取评分 - 使用统一的评分提取函数
                            score = extract_score_from_reason(result['reason'])
                            
                            # 构建完整的行数据
                            row_data = {
                                '期权代码': opt_code,
                                '品种': symbol,
                                '趋势': symbol_trend_map.get(symbol, "未知"),
                                '分析日期': opt_latest_date.strftime('%Y-%m-%d'),
                                '评分': f"{score:.1f}",
                                '决策': result['decision'],
                                '理由': result['reason'],
                                '标的期货': result['details'].get('underlying_futures', 'N/A'),
                                '期权类型': result['details'].get('option_type', 'N/A'),
                                '行权价': result['details'].get('strike', 'N/A'),
                                '标的历史波动率': result['details'].get('current_hv_underlying', 'N/A'),
                                '当前隐含波动率': result['details'].get('current_iv_option', 'N/A'),
                                '昨日隐含波动率': result['details'].get('yesterday_iv_option', 'N/A'),
                                'IV高于昨日': result['details'].get('iv_higher_than_yesterday', 'N/A'),
                                'IV Rank': result['details'].get('iv_rank', 'N/A'),
                                'IV Percentile': result['details'].get('iv_percentile', 'N/A'),
                                'IV趋势': result['details'].get('iv_trend', 'N/A'),
                                'IV高于短期均线': result['details'].get('iv_above_short_ma', 'N/A'),
                                'Delta': result['details'].get('delta', 'N/A'),
                                'Gamma': result['details'].get('gamma', 'N/A'),
                                'Theta (每日)': result['details'].get('theta_daily', 'N/A'),
                                'Vega': result['details'].get('vega', 'N/A'),
                                'DTE (天)': result['details'].get('dte', 'N/A'),
                                'Theta/Vega比率': result['details'].get('theta_vega_ratio', 'N/A'),
                                '成交量': result['details'].get('current_volume', 'N/A'),
                                '持仓量': result['details'].get('current_open_interest', 'N/A'),
                                '波动率偏度': result['details'].get('iv_skew', 'N/A'),
                                '波动率峰度': result['details'].get('iv_butterfly', 'N/A'),
                                '归一化偏度': result['details'].get('iv_skew_normalized', 'N/A'),
                                '归一化峰度': result['details'].get('iv_butterfly_normalized', 'N/A'),
                                '价格趋势向上': result['details'].get('price_uptrend', 'N/A'),
                                '当前收盘价': result['details'].get('current_close', 'N/A'),
                                '昨日收盘价': result['details'].get('yesterday_close', 'N/A'),
                                '3日均线价格': result['details'].get('ma3_close', 'N/A'),
                                '5日均线价格': result['details'].get('ma5_close', 'N/A')
                            }
                            temp_results.append((row_data, score))
                    
                    # 按评分排序选择最佳合约
                    if temp_results:
                        temp_results.sort(key=lambda x: x[1], reverse=True)
                        best_option = temp_results[0][0]
                        valid_options = [best_option]  # 使用 valid_options 而不是 sorted_options
                        print(f"为品种 {symbol} 选择评分最高的备选合约: {best_option['期权代码']}, 评分={best_option['评分']}")
                    else:
                        # 如果评估失败，默认返回空
                        valid_options = []
                else:
                    print(f"品种 {symbol} 找不到任何DTE>2天的合适合约，跳过推荐")
                    valid_options = []  # 确保变量被定义
                
            # 按评分排序(如果 valid_options 不是从 temp_results 来的)
            if valid_options and not temp_results:
                valid_options = sorted(valid_options, key=lambda x: float(x['评分']), reverse=True)
            
            if valid_options:  # 确保列表非空
                best_option = valid_options[0]
                dte_str = best_option.get('DTE (天)', 'N/A')
                recommendations.append(best_option)
                recommended_symbols.add(symbol)
                print(f"[勉强推荐] 为品种 {symbol} 添加最高分合约: {best_option['期权代码']}, 评分={best_option['评分']}, DTE={dte_str}")
                
                # 如果评分太低，标记为"勉强推荐"以示区别
                if float(best_option['评分']) < 3.0:
                    best_option['理由'] = f"[勉强推荐] {best_option['理由']}"
                    if "不买入" in best_option['决策']:
                        best_option['决策'] = "谨慎买入(评分低)"

    # 将结果写入Excel文件
    all_results_df = pd.DataFrame(all_results_list)
    recommendations_df = pd.DataFrame(recommendations)

    # 对推荐合约进行排序
    if not recommendations_df.empty:
        recommendations_df['评分'] = recommendations_df['评分'].astype(float)
        recommendations_df = recommendations_df.sort_values(by=['品种', '评分'], ascending=[True, False])

    # 定义使用说明文本
    usage_instructions_text = """【系统概览与数据要求】
- 本系统基于波动率分析，为期权买方提供决策支持。
- 系统假设您已对标的期货的未来趋势有预判（如看多、看空、盘整）。
- 确保以下数据文件路径正确且数据为最新：
  * 期货历史数据：d:/bb/期货历史/csv格式/
  * 期权历史数据：d:/bb/期权历史/csv格式/
  * 趋势判断数据：d:/bb/xia/模拟商品期权操作.csv (用于指定各品种的预设趋势)
  * 期权数据XLS文件：d:/期权数据/*.xls (用于获取期权合约详细信息，如到期日、剩余天数等)

【核心输出指标解读】
- 期权代码: 交易软件中唯一的期权合约标识。
- 品种: 期权对应的期货品种，如AG (白银)。
- 趋势: 根据您的趋势判断文件或系统默认设置的当前品种趋势（看多/空/观望）。
- 分析日期: 本次分析所基于的最新数据日期。
- 评分: 系统根据各项波动率指标和希腊字母综合计算得出的买入倾向评分，越高越倾向于买入。
- 决策: 基于评分给出的交易建议，如"强烈建议买入"、"可以考虑买入"等。
- 理由: 形成当前评分和决策的主要原因和指标表现。
- 标的期货: 该期权合约对应的具体期货合约代码，如ag2412。
- 期权类型: C (Call) 代表认购期权，P (Put) 代表认沽期权。
- 行权价: 期权合约规定的买方有权买入或卖出标的资产的价格。
- 标的历史波动率 (HV): 标的期货在过去一段时间（默认10日）的实际价格波动程度，反映历史波动水平。
- 当前隐含波动率 (IV): 当前期权市场价格反推出的对未来波动率的预期。IV是期权定价的核心，也是本策略重点分析对象。
- 昨日隐含波动率: 前一个交易日的隐含波动率，用于观察IV的短期变化。
- IV高于昨日: 若为True，表示当日IV较昨日上涨，通常对波动率买方有利。
- IV Rank: 当前IV在过去一年（或指定周期，默认365个日历日）IV区间中的相对位置（0%-100%）。例如，IV Rank 20%表示当前IV处于过去一年从最低到最高范围的20%分位点。低Rank通常被视为IV相对便宜。
    - 注意: 如果一个期权合约的实际交易历史不足设定的365天回顾期，IV Rank将基于其自上市以来的全部可用IV数据进行计算。解读时请注意，此Rank反映的是该合约在其自身较短历史中的相对位置。
- IV Percentile: 当前IV高于过去一年（或指定周期）中百分之多少的IV值。例如，IV Percentile 30%表示当前IV高于历史上30%的IV读数。高Percentile表示当前IV处于历史较高水平。
    - 注意: 与IV Rank类似，若合约历史数据不足365天，IV Percentile也将基于其全部可用历史数据计算。在比较不同合约的IV Rank/Percentile时，应考虑到它们各自历史数据长度的差异。
- IV趋势: 基于短期、中期均线判断的IV近期走势方向和强度（如强势上升、温和下降、盘整等）。
- IV高于短期均线: 若为True，表示当前IV突破了短期均线（如3日或5日），可能预示IV短期走强。
- 成交量 (当日): 该期权合约在分析当日的总成交手数。反映了合约的即时市场参与度。
- 持仓量 (当日): 该期权合约在分析当日结束后的未平仓合约数量。反映了市场对该合约的累积兴趣和资金容量。
    - 流动性评估: 系统会综合当日成交量和持仓量来评估合约的流动性。
        - 流动性极低 (成交量 < 20手 或 持仓量 < 50手): 将直接标记为"不买入"并且不会出现在推荐列表中，因为这类合约交易难度极高，滑点过大。
        - 流动性不足 (成交量 < 100手 或 持仓量 < 200手): 将显著减分 (-1.5)，因为难以成交或交易成本高。
        - 流动性良好 (成交量 > 500手 且 持仓量 > 1000手): 将适度加分 (+0.5)。
        - 未能获取数据: 也会视为流动性风险并减分 (-0.5)。
- 价格趋势向上: 表示期权价格是否处于上涨趋势，是合约推荐的关键条件之一。满足两个条件则为True：1) 当前收盘价高于昨日收盘价；2) 当前收盘价高于3日均线。这有助于避免在价格下跌时买入，即使其他指标良好。当价格趋势条件不满足时，推荐级别会降为"谨慎买入(风险较高)"。
- 当前收盘价: 期权合约最近交易日的收盘价格。
- 昨日收盘价: 前一交易日的期权合约收盘价。
- 3日均线价格: 期权合约最近3个交易日收盘价的平均值，用于判断短期趋势。
- 5日均线价格: 期权合约最近5个交易日收盘价的平均值，与3日均线共同用于判断短期趋势。
- 波动率偏度: 表示期权波动率曲线的不对称性，通常为实值PUT和虚值CALL的IV差异。正偏度表示下行保护溢价更高，负偏度表示上行风险溢价更高。
- 波动率峰度: 表示期权波动率曲线的陡峭程度，通常通过平值期权和等权虚值期权的IV差异计算。高峰度表示市场预期大幅波动，低峰度表示预期平稳。
- 归一化偏度与峰度: 将偏度和峰度除以平值IV，使不同时间和品种的数据更具可比性。

【期权希腊字母详解】
- Delta (Δ): 衡量标的资产价格变动1单位时期权价格的预期变动量。对于认购期权，Delta介于0到1之间；对于认沽期权，介于-1到0之间。
    - 应用: Delta帮助您判断期权的方向性风险敞口。如果您是纯粹做多波动率（即认为无论方向，波动会加大），可能会倾向于 Delta 绝对值接近 0.5 的期权（接近平值）。如果您有明确的方向性判断（如强烈看多某品种），可以选择相应 Delta 的期权（如看多时选Delta较高的认购期权）。
- Gamma (Γ): 衡量标的资产价格变动1单位时期权Delta值的预期变动量。Gamma反映了Delta随标的价格变动的速度。
    - 应用: 高Gamma意味着Delta变化快，期权价格对标的价格变化的敏感度变化也快。这对于期望市场价格发生大幅波动的情况可能有利，但也意味着风险敞口变化更快，需要更密切的监控。平值期权的Gamma通常最大。
- Theta (Θ) (每日): 衡量时间流逝一天，期权价格因时间价值衰减而预期损失的金额（假设其他因素不变）。Theta对期权买方通常为负值。
    - 应用: Theta直接告诉您作为期权买方每天需要付出的"时间成本"或"持有成本"。随着到期日的临近，Theta的绝对值通常会加速增大，尤其对于平值和虚值期权。
- Vega (ν): 衡量标的资产隐含波动率变动1个百分点时期权价格的预期变动量。
    - 应用: Vega是衡量期权对波动率变化的敏感度。如果您预期市场波动率将上升（无论方向），做多Vega（即买入期权）是核心策略。Vega越高的期权，从波动率上升中获益或因波动率下降而亏损的幅度也越大。
- DTE (天) (Days To Expiry): 期权合约剩余的到期天数。
    - 应用: DTE对期权买方的策略至关重要。本系统根据您的实盘经验，对DTE的评分逻辑如下：
        - DTE < 10天: 因Theta衰减风险极高，显著减分 (-1.0)。
        - 10 <= DTE < 20天: Theta风险仍较高，时间价值损耗快，适度减分 (-0.5)。
        - 20 <= DTE <= 60天: 此区间被视为较优选择，在时间价值、权利金成本和期权弹性之间取得较好平衡，不加分也不减分 (0)。
        - 60 < DTE <= 120天: 弹性开始下降，权利金相对偏高，风险增加，减分调整为 (-0.5)。
        - DTE > 120天: 因弹性差、权利金过高、资金效率不佳等因素，显著减分 (-2.0)，一般不推荐买入此类远期期权。
- Theta/Vega 比率: 每日时间价值损耗（Theta）与波动率敏感度（Vega）的比率。通常计算为 (每日Theta / Vega值)。
    - 应用: 这是一个关键的风险收益衡量指标，尤其对于波动率交易者。这个比率的绝对值越小，说明期权因时间流逝的价值损耗相对于其从波动率上升中获益的潜力来说越小，对波动率买方越有利。例如，如果比率为-0.1，意味着每天损失的Theta是Vega的10%。选择绝对值较小的比率，可以在等待波动率上升时，承受相对较小的时间衰减成本。

【波动率结构分析与策略优化】
- 波动率偏度(Skew)解读:
    - 正偏度(PUT偏贵): 表示市场更担忧下行风险，愿意为保护支付更高溢价。通常对看空策略有利。
    - 负偏度(CALL偏贵): 表示市场预期上行风险较大，愿意为上涨机会支付更高溢价。通常对看多策略有利。
    - 策略应用: 本系统在看多时偏好负偏度环境(+0.75分)，看空时偏好正偏度环境(+0.75分)。如果偏度与交易方向显著相反，则会减分(-1.0分)。

- 波动率峰度(Butterfly)解读:
    - 高峰度: 表示市场预期标的将发生大幅波动，但方向不确定。有利于波动率买方策略(+0.5分)。
    - 低峰度: 表示市场预期标的将平稳运行，波动较小。不利于波动率买方策略(-0.75分)。
    - 策略应用: 作为期权买方，高峰度环境更利于获利，特别是使用跨式(Straddle)或宽跨(Strangle)等非方向性策略。

【推荐条件说明】
系统在推荐期权合约时，会同时考虑以下五个核心条件：
1. 决策为"强烈建议买入"或"可以考虑买入"（即综合评分足够高）
2. 期权类型与趋势匹配（看多用认购期权C，看空用认沽期权P）
3. IV高于昨日（隐含波动率有上涨趋势）
4. 综合评分至少达到3.0分（所有波动率指标的综合评估结果）
5. 价格趋势向上（当前收盘价>昨收价 且 >3日均线）

此外，系统包含重要安全机制：
- 无论评分多高，当期权收盘价低于昨日收盘价时，最多只能给出"谨慎买入"的建议，不会给出"强烈建议买入"或"可以考虑买入"的推荐
- 强制排除了流动性极低（成交量<20手或持仓量<50手）的合约，这些合约无论评分多高都不会被推荐，因为其交易风险过高

只有同时满足以上条件的合约才会出现在推荐列表中。如果某个合约评分高但因价格趋势不向上而未被推荐，系统会在日志中记录提示信息。

【注意事项】
- 本系统提供的所有信息和建议仅供参考，不构成任何投资要约或实际操作指引。
- 期权交易具有较高风险，投资者应在充分了解相关风险的基础上谨慎决策。
- 市场状况瞬息万变，历史数据和模型分析不能完全预测未来。
- 价格趋势筛选可能会过滤掉部分评分高但价格下跌的合约，这是一个风险控制措施。"""
    
    # 将使用说明文本按行分割，创建DataFrame
    instructions_lines = usage_instructions_text.split('\\n')
    instructions_df = pd.DataFrame(instructions_lines, columns=['说明内容'])

    try:
        with pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine='openpyxl') as writer:
            all_results_df.to_excel(writer, sheet_name='交易建议结果', index=False)
            if not recommendations_df.empty:
                recommendations_df.to_excel(writer, sheet_name='推荐合约列表', index=False)
            else:
                # 如果没有推荐，也创建一个空的sheet或者提示信息
                pd.DataFrame([{'信息': '本次分析未产生推荐合约'}]).to_excel(writer, sheet_name='推荐合约列表', index=False)
            instructions_df.to_excel(writer, sheet_name='使用说明', index=False)
        print(f"结果已保存到Excel文件: {OUTPUT_EXCEL_FILE}")
    except Exception as e:
        print(f"保存到Excel文件失败: {e}")
        print("尝试将主要结果保存为CSV...")
        try:
            fallback_csv_path = os.path.join(OUTPUT_DIR, "期权交易建议结果_fallback.csv")
            all_results_df.to_csv(fallback_csv_path, index=False, encoding='utf-8-sig')
            print(f"主要结果已作为CSV保存到: {fallback_csv_path}")
            if not recommendations_df.empty:
                fallback_rec_csv_path = os.path.join(OUTPUT_DIR, "期权推荐合约_fallback.csv")
                recommendations_df.to_csv(fallback_rec_csv_path, index=False, encoding='utf-8-sig')
                print(f"推荐合约已作为CSV保存到: {fallback_rec_csv_path}")
        except Exception as csv_e:
            print(f"保存为CSV也失败: {csv_e}")


    # 打印推荐合约总结 (如果存在)
    if not recommendations_df.empty:
        print(f"\n=== 推荐期权合约 (共{len(recommendations_df)}个) ===")
        # 按趋势分组显示
        trend_groups = {
            "多多多": [],
            "空": []
        }
        
        # recommendations_df已经是DataFrame，可以直接迭代其行
        for _, rec_row in recommendations_df.iterrows():
            trend = rec_row['趋势']
            if trend in trend_groups:
                trend_groups[trend].append(rec_row.to_dict()) # 转回字典方便处理
        
        # 按固定顺序显示不同趋势的推荐
        trend_order = ["多多多", "空"]
            
        for trend_val in trend_order: # 修改变量名以避免与外部trend变量冲突
            if trend_val not in trend_groups or not trend_groups[trend_val]:
                continue
                
            recs_list = trend_groups[trend_val]
            # 评分已经是float类型，直接用lambda排序
            sorted_recs = sorted(recs_list, key=lambda x: x['评分'], reverse=True)
            top_n = min(5, len(sorted_recs))
            
            print(f"\n--- 趋势 '{trend_val}' 的前{top_n}个推荐期权合约 ---")
            for i, rec_dict in enumerate(sorted_recs[:top_n]):
                print(f"{i+1}. {rec_dict['品种']} {rec_dict['期权代码']} ({rec_dict['期权类型']}) - 行权价:{rec_dict['行权价']} - 评分:{rec_dict['评分']:.1f} - {rec_dict['决策']} - IV:{rec_dict['当前隐含波动率']}")
    else:
        print("\n未发现有交易信号的期权合约")

    print("\n分析完成！")

    # 在所有计算和文件输出后，打印关键的调试信息
    print("\n" + "="*30)
    print("脚本执行完毕后的关键调试信息")
    print("="*30)

    print("\n1. 最终的品种趋势映射 (symbol_trend_map):")
    if symbol_trend_map:
        for symbol_key, trend_value in symbol_trend_map.items():
            print(f"  {symbol_key}: {trend_value}")
    else:
        print("  symbol_trend_map 为空。")

    print("\n2. 进入详细评估的期权合约列表 (option_codes):")
    if option_codes:
        print(f"  共 {len(option_codes)} 个合约进入评估。")
        # 为了避免打印过多内容，可以只打印前N个或与PS/SI相关的
        ps_si_option_codes_to_print = []
        other_option_codes_to_print = []
        for opt_code_info in option_codes:
            # opt_code_info is a tuple: (option_code, file_path, symbol, assumed_trend)
            if "PS" in opt_code_info[0].upper() or "SI" in opt_code_info[0].upper():
                ps_si_option_codes_to_print.append(opt_code_info)
            else:
                if len(other_option_codes_to_print) < 20: # 最多打印20个其他合约作为示例
                    other_option_codes_to_print.append(opt_code_info)
        
        if ps_si_option_codes_to_print:
            print("\n  涉及PS或SI的期权合约:")
            for i, (code, _, sym, trend) in enumerate(ps_si_option_codes_to_print):
                print(f"    {i+1}. 代码: {code}, 品种: {sym}, 预设趋势: {trend}")
        else:
            print("\n  未找到涉及PS或SI的期权合约进入评估。")

        if other_option_codes_to_print:
            print("\n  其他部分期权合约 (最多20条):")
            for i, (code, _, sym, trend) in enumerate(other_option_codes_to_print):
                print(f"    {i+1}. 代码: {code}, 品种: {sym}, 预设趋势: {trend}")
        
        if not ps_si_option_codes_to_print and not other_option_codes_to_print and option_codes:
             # 如果option_codes不为空，但上面两个列表都为空（不太可能发生除非option_codes少于20且不含PS/SI）
             print("\n  option_codes 中包含的合约 (前20条):")
             for i, (code, _, sym, trend) in enumerate(option_codes[:20]):
                print(f"    {i+1}. 代码: {code}, 品种: {sym}, 预设趋势: {trend}")

    else:
        print("  option_codes 列表为空，没有合约进入详细评估。")
    
    # 性能总结报告
    cache_status = data_loader.get_cache_status()
    total_options_processed = len(option_codes) if option_codes else 0
    recommendations_count = len(recommendations) if recommendations else 0
    
    print("\n" + "="*50)
    print("性能总结报告")
    print("="*50)
    print(f"处理的期权合约总数: {total_options_processed}")
    print(f"推荐的期权合约数量: {recommendations_count}")
    print(f"缓存访问总次数: {cache_status['total_access_count']}")
    print(f"期货数据缓存大小: {cache_status['futures_cache_size']}")
    print(f"期权数据缓存大小: {cache_status['options_cache_size']}")
    print(f"缓存限制: {cache_status['cache_limit']}")
    
    # 性能建议
    print("\n性能优化建议:")
    if cache_status['total_access_count'] > 1000:
        print("- 系统进行了大量数据访问，考虑增加缓存大小或使用更快的存储设备")
    
    if total_options_processed > 500:
        print("- 处理了大量期权合约，建议考虑并行处理或分批处理")
    
    cache_utilization = (cache_status['futures_cache_size'] + cache_status['options_cache_size']) / cache_status['cache_limit']
    if cache_utilization < 0.5:
        print("- 缓存使用率较低，可以考虑减少缓存大小以节省内存")
    elif cache_utilization > 0.9:
        print("- 缓存使用率较高，建议增加缓存大小以减少重复加载")
    
    print("\n" + "="*30)

