# 安全政策

## 支持的版本

我们目前支持以下版本的安全更新：

| 版本 | 支持状态 |
| ------- | ------------------ |
| 1.8.x   | :white_check_mark: |
| < 1.8   | :x:                |

## 报告漏洞

如果您发现了安全漏洞，请**不要**在公开的Issue中报告。

请发送邮件至：956551063@qq.com

请在邮件中包含：
- 漏洞的详细描述
- 重现步骤
- 可能的影响范围
- 建议的修复方案（如果有）

我们将在收到报告后48小时内回复，并在确认漏洞后尽快发布修复版本。

## 安全最佳实践

### 配置安全
- 不要在代码中硬编码敏感信息
- 使用 `config.json` 存储配置，不要提交到版本控制
- 定期更新依赖包

### 数据安全
- 本地存储的交易数据请妥善保管
- 不要在公共网络传输敏感交易信息
- 建议使用加密存储重要数据

### 网络安全
- 使用 HTTPS 连接交易所API
- 验证 SSL 证书
- 实施适当的访问控制

## 依赖安全

定期运行以下命令检查依赖包安全：

```bash
pip audit
```

如发现安全漏洞，请及时更新相关包版本。

感谢您帮助保持项目安全！
