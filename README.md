# HashScan - 智能文件哈希扫描工具

![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)
![License](https://img.shields.io/badge/License-LGPLv3-green.svg)

HashScan 是一个功能强大的文件哈希扫描工具，支持多种哈希算法、多线程处理和动态资源管理，能够高效地扫描文件系统并计算文件哈希值。

## 功能特点

- **多算法支持**：支持所有 Python hashlib 提供的哈希算法 (MD5, SHA1, SHA256 等)
- **智能队列管理**：动态调整队列大小，根据系统负载优化性能
- **多线程处理**：充分利用多核CPU，提高扫描速度
- **多种输出格式**：支持文本、JSON、YAML、Markdown 和 Excel 格式
- **重复文件检测**：可识别并报告具有相同哈希值的文件
- **实时监控**：显示扫描进度和线程状态
- **交互式控制**：支持暂停、继续和终止扫描操作

## 安装使用

### 环境要求

- Python 3.7 或更高版本
- 使用 Rich 库以获得更好的终端显示效果

### 安装依赖

```bash
pip install -r requirements.txt
```

### 基本使用

```bash
python hash_scanner.py -f /path/to/folder
```

### 命令行选项

```
usage: hash_scanner.py [-h] [-f FOLDER] [-a ALGO [ALGO ...]] [-r] [-t THREADS]
                       [-s] [-o OUTPUT] [--format {text,json,yaml,markdown,xlsx}]

智能文件哈希扫描工具

可选参数:
  -h, --help            显示帮助信息并退出
  -f FOLDER, --folder FOLDER
                        要扫描的目标文件夹路径 (默认: 当前目录)
  -a ALGO [ALGO ...], --algorithms ALGO [ALGO ...]
                        要使用的哈希算法 (默认: md5)
  -r, --recursive       递归扫描子目录
  -t THREADS, --threads THREADS
                        使用的线程数 (默认: CPU核心数)
  -s, --same           仅显示重复文件
  -o OUTPUT, --output OUTPUT
                        将结果输出到指定文件
  --format {text,json,yaml,markdown,xlsx}
                        输出格式 (默认: text)

支持的哈希算法: blake2b, blake2s, md5, sha1, sha224, sha256, sha384, sha3_224, sha3_256, sha3_384, sha3_512, sha512, shake_128, shake_256
```

### 使用示例

1. **基本扫描** (使用MD5算法扫描当前目录):
   ```bash
   python hash_scanner.py
   ```

2. **递归扫描** (使用多种算法扫描目录及其子目录):
   ```bash
   python hash_scanner.py -f /path/to/folder -a md5 sha1 sha256 -r
   ```

3. **查找重复文件**:
   ```bash
   python hash_scanner.py -f /path/to/folder -s
   ```

4. **生成Excel报告**:
   ```bash
   python hash_scanner.py -f /path/to/folder --format xlsx -o report.xlsx
   ```

5. **拖放文件夹扫描**:
   直接将文件夹拖放到脚本文件上运行

## 交互命令

在扫描过程中，可以输入以下命令控制程序:

- `p` - 暂停扫描
- `c` - 继续扫描
- `q` - 终止扫描

## 输出格式

HashScan 支持多种输出格式:

1. **文本格式** (默认): 适合直接在终端查看
2. **JSON格式**: 适合程序处理
3. **YAML格式**: 适合配置文件
4. **Markdown格式**: 适合文档记录
5. **Excel格式**: 适合数据分析 (需要指定输出文件)

## 性能优化

- **动态队列调整**: 根据系统负载自动调整队列大小
- **内存视图优化**: 使用内存视图提高哈希计算效率
- **批量处理**: 分块读取文件减少IO操作

## 错误处理

程序会记录并显示扫描过程中的错误，包括:

- 无权限访问的文件
- 不存在的文件
- 其他IO错误

## 许可证

本项目使用 LGPLv3 许可证 - 详情请参阅 LICENSE 文件

## 贡献

欢迎提交问题和拉取请求改进本项目