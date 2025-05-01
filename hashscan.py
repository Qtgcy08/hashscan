import os
import argparse
import hashlib
import sys
import threading
from queue import Queue, Full, Empty
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
import signal
import multiprocessing
import time

MAX_QUEUE_SIZE = 1000
TERMINATE_SIGNAL = "TERMINATE"
stop_event = threading.Event()

def signal_handler(sig, frame):
    print("\n操作被用户中断")
    stop_event.set()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

class HashScanner:
    def __init__(self, folder, algorithms, recursive, threads):
        self.folder = Path(folder).resolve()
        self.algorithms = [a.lower() for a in algorithms]
        self.recursive = recursive
        self.threads = threads
        self.file_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.hash_results = defaultdict(dict)
        
        # 线程安全计数器
        self.total_files = 0
        self.processed_files = 0
        self.total_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        
        # 线程状态跟踪
        self.thread_status = {}
        self.status_lock = threading.Lock()

    def _validate_folder(self):
        if not self.folder.exists():
            raise FileNotFoundError(f"目标文件夹不存在: {self.folder}")
        if not self.folder.is_dir():
            raise NotADirectoryError(f"路径不是目录: {self.folder}")

    def _file_producer(self):
        try:
            file_iter = self.folder.rglob('*') if self.recursive else self.folder.iterdir()
            for item in file_iter:
                if stop_event.is_set():
                    break
                try:
                    if item.is_file():
                        with self.total_lock:
                            self.total_files += 1
                        while not stop_event.is_set():
                            try:
                                self.file_queue.put(str(item), timeout=0.1)
                                break
                            except Full:
                                continue
                except PermissionError:
                    print(f"\n无法添加文件（权限不足）: {item}")
                except Exception as e:
                    print(f"\n处理文件时出错: {item} - {str(e)}")
        except Exception as e:
            print(f"\n生产文件路径时发生致命错误: {str(e)}")
            stop_event.set()
        finally:
            for _ in range(self.threads):
                self.file_queue.put(TERMINATE_SIGNAL)

    def _hash_consumer(self, thread_id):
        try:
            while not stop_event.is_set():
                try:
                    file_path = self.file_queue.get(timeout=0.5)
                    if file_path == TERMINATE_SIGNAL:
                        self.file_queue.put(TERMINATE_SIGNAL)
                        break

                    # 更新线程状态
                    with self.status_lock:
                        self.thread_status[thread_id] = file_path[-40:]

                    try:
                        with open(file_path, 'rb') as f:
                            hash_objs = {algo: getattr(hashlib, algo)() for algo in self.algorithms}
                            while chunk := f.read(4096):
                                for h in hash_objs.values():
                                    h.update(chunk)
                            hashes = {algo: h.hexdigest() for algo, h in hash_objs.items()}
                    except Exception as e:
                        hashes = f"Error: {str(e)}"
                    
                    # 更新处理进度
                    with self.progress_lock:
                        self.processed_files += 1
                    
                    # 清除线程状态
                    with self.status_lock:
                        self.thread_status[thread_id] = None
                except Empty:
                    continue
        except Exception as e:
            print(f"\n工作线程异常: {str(e)}")

def _print_status(self, pbar):
    """在进度条上方显示线程状态"""
    with self.status_lock:
        status_lines = []
        for tid in sorted(self.thread_status.keys()):
            file = self.thread_status[tid]
            status = f"线程 {tid:>2}: {file if file else '空闲':<40}"
            status_lines.append(status)

    # 保存原始光标位置
    sys.stdout.write("\033[s")
    
    # 移动到进度条上方
    sys.stdout.write(f"\033[{len(status_lines)+1}A")
    
    # 清除原有状态区域
    for _ in range(len(status_lines)):
        sys.stdout.write("\033[K\n")
    sys.stdout.write("\033[K")
    
    # 回退到状态区域起始位置
    sys.stdout.write(f"\033[{len(status_lines)+1}F")
    
    # 打印新状态
    sys.stdout.write("\n\033[K".join(status_lines))
    
    # 恢复原始光标位置（进度条位置）
    sys.stdout.write("\033[u")
    sys.stdout.flush()

def run(self):
    try:
        print("[阶段1/2] 扫描并处理文件...")
        
        # 启动生产者线程
        producer = threading.Thread(target=self._file_producer, daemon=True)
        producer.start()

        # 启动消费者线程
        workers = []
        for i in range(self.threads):
            t = threading.Thread(target=self._hash_consumer, args=(i+1,))
            t.start()
            workers.append(t)
            self.thread_status[i+1] = None

        # 主线程控制显示
        with tqdm(total=0, desc="处理进度", unit="file", 
                dynamic_ncols=True, position=0) as pbar:  # 新增position参数
            last_total = 0
            last_processed = 0
            while True:
                # 获取最新状态
                with self.total_lock:
                    current_total = self.total_files
                with self.progress_lock:
                    current_processed = self.processed_files

                # 更新进度条
                pbar.total = current_total
                pbar.update(current_processed - last_processed)
                
                # 显示线程状态（在进度条上方）
                self._print_status(pbar)
                
                last_processed = current_processed
                last_total = current_total

                # 终止条件检查
                if not producer.is_alive() and all(not t.is_alive() for t in workers):
                    break
                
                time.sleep(0.2)

        # 清除状态显示
        sys.stdout.write("\033[J")  # 清除从光标到屏幕末尾
        return self.hash_results
    except Exception as e:
        stop_event.set()
        raise e

def main():
    parser = argparse.ArgumentParser(description="文件哈希值计算查重工具", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--folder', default='.', help="目标文件夹 (默认: 当前目录)")
    parser.add_argument('-a', '--algorithms', nargs='+', default=['md5'],
                        choices=[hashlib.algorithms_available], 
                        help="选择哈希算法 (可多选，默认: md5)")
    parser.add_argument('-r', '--recursive', action='store_true', 
                        help="递归扫描子目录")
    parser.add_argument('-o', '--output', help="结果输出文件 (默认: 打印到终端)")
    parser.add_argument('-t', '--thread', type=int, 
                        help=f"工作线程数 (默认: CPU逻辑核心数 {multiprocessing.cpu_count()})")
    parser.add_argument('-s', '--same', action='store_true',
                        help="仅打印重复文件")
    
    args = parser.parse_args()
    worker_threads = args.thread or multiprocessing.cpu_count()
    
    try:
        scanner = HashScanner(args.folder, args.algorithms, args.recursive, worker_threads)
        print(f"使用工作线程数: {worker_threads}")
        hash_results = scanner.run()
        
        # 生成报告内容
        if args.same:
            duplicates = scanner._analyze_duplicates()
            report = []
            if duplicates:
                report.append("重复文件分析结果：")
                for algo, groups in duplicates.items():
                    report.append(f"\n[{algo.upper()}]")
                    for hash_val, files in groups.items():
                        report.append(f"  {hash_val}:")
                        report.extend([f"    → {f}" for f in files])
            else:
                report.append("未发现重复文件")
        else:
            report = []
            for file_path, hashes in hash_results.items():
                report.append(f"文件: {file_path}")
                if isinstance(hashes, dict):
                    for algo in sorted(hashes.keys()):
                        report.append(f"  {algo.upper()}: {hashes[algo]}")
                else:
                    report.append(f"  错误: {hashes}")
                report.append("")  # 空行分隔

        # 输出结果
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write("\n".join(report))
            print(f"\n结果已保存至 {args.output}")
        else:
            print("\n" + "\n".join(report))
            
    except KeyboardInterrupt:
        print("\n操作已中断")
    except Exception as e:
        print(f"\n运行出错: {str(e)}")

if __name__ == '__main__':
    main()  # 修复点：确保这里没有未闭合的try块