# Contributing to Tider

欢迎通过 GitHub Issues 提交问题或功能建议，通过 Pull Request 提交改进。

## 开发环境

需要 Python 3.11 或更新版本。

```console
git clone https://github.com/ZLotusRain/tider.git
cd tider
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

Windows 可使用 `.venv\Scripts\activate` 激活环境。需要可选下载器或 SOCKS 支持时，按 README 安装对应 extra。

## 报告问题

请附上 Tider 和 Python 版本、操作系统、安装方式、预期与实际行为，以及可以独立运行的最小复现。
可以通过 `tider --version` 查看框架版本。日志和示例中请移除密码、令牌、Cookie 等私密信息。
安全漏洞请按 [安全反馈说明](SECURITY.md) 私下报告。

## 提交修改

- 每个 Pull Request 尽量解决一个明确问题，并说明修改原因、影响范围和验证方法。
- 使用英文 Conventional Commits 提交说明，例如 `fix(crawler): close late responses during shutdown`。
- 保持现有直接、明确的代码风格；涉及请求、响应、代理和并发资源时，说明所有权及清理路径。
- 用户可见的变更请更新 README 或 CHANGELOG；不要将账号配置、运行日志、实验数据和本地环境文件一起提交。

## 打包检查

```console
python -m pip install --upgrade build twine
python -m build
python -m twine check --strict dist/*
```

每次检查前确认产物目录中没有混入旧版本文件。安装验证应在新的虚拟环境中进行，避免依赖开发环境中额外安装的包。
仓库提供可手动触发的 **Packaging check** 工作流，用于构建、包元数据检查和安装后的 CLI 检查，不运行回归测试，也不上传 PyPI。
