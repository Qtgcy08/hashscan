# 核鉴HashScan - 文件哈希计算工具

![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)
![License](https://img.shields.io/badge/License-LGPLv3-green.svg)

> **核验如鉴，明察秋毫**  
> **核鉴**一名，典出《宋史·职官志》："核验文牍，鉴察秋毫"。
一个简单实用的文件哈希计算工具，支持多线程处理和多种输出格式。

## 功能特点

- **多算法支持**：支持全系列 Python hashlib 哈希算法，支持国密SM3
- **快速扫描**：多线程处理，自动优化队列大小
- **实用功能**：查找重复文件、支持递归扫描
- **多种输出**：文本、JSON、Excel等格式

## 快速开始

### 安装依赖
```bash
pip install -r requirements.txt
```

### 基本使用
扫描当前目录：
```bash
python hashscan.py
```

递归扫描指定目录：
```bash
python hashscan.py -f /path/to/folder -r
```

## 使用示例

1. **计算文件哈希值**：
   ```bash
   python hashscan.py -f ~/Documents -a sha256
   ```

2. **查找重复文件**：
   ```bash
   python hashscan.py -f /downloads -s
   ```

3. **生成Excel报告**：
   ```bash
   python hashscan.py -f /photos --format xlsx -o report.xlsx
   ```

## 交互控制

扫描过程中可以输入命令：
- `p` 暂停扫描
- `c` 继续扫描
- `q` 退出程序

## 参数说明

```
基本参数：
  -f, --folder      扫描目录(默认当前目录)
  -a, --algorithms  哈希算法(默认md5)
  -r, --recursive   递归扫描子目录

性能控制：
  -t, --threads     线程数(默认CPU核心数)

输出选项：
  -o, --output      输出文件
  --format          输出格式(text/json/yaml/markdown/xlsx)
  -s, --same        仅显示重复文件
```

## 技术亮点

1. **动态队列调整**：根据系统负载自动调整队列大小
2. **实时进度显示**：显示CPU/内存使用率和处理进度
3. **线程状态监控**：实时查看各线程处理状态
4. **错误处理机制**：记录并显示处理失败的文件

## 常见问题

**Q**: 如何处理大文件？
**A**: 工具采用分块读取方式，内存占用低，适合处理大文件。

**Q**: 为什么扫描速度慢？
**A**: 可以尝试增加线程数(-t参数)或减少同时使用的哈希算法数量。

**Q**: 如何验证文件完整性？
**A**: 生成哈希报告后，可以定期重新扫描并与原始报告对比。