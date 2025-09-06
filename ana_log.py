import csv
import os

LOG_FILE = 'classification_log.csv'

def analyze_mp4_percentage_in_porn():
    """
    统计 classification_log.csv 文件中,
    被标记为色情(label=1)的文件里, .mp4格式的占比。
    """
    if not os.path.exists(LOG_FILE):
        print(f"错误: 日志文件 '{LOG_FILE}' 不存在。")
        print("请先运行 example.py 来生成日志文件。")
        return

    total_porn_files = 0
    mp4_porn_files = 0

    try:
        with open(LOG_FILE, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader) # 读取并跳过表头

            # 确保CSV格式正确 (filepath, label)
            if header != ['filepath', 'label']:
                print(f"错误: CSV文件 '{LOG_FILE}' 的表头格式不正确。应为 ['filepath', 'label']。")
                return

            for row in reader:
                if len(row) != 2:
                    continue # 跳过格式不正确的行

                filepath, label = row
                
                # 检查标签是否为 '1' (色情)
                if label == '1':
                    total_porn_files += 1
                    # 检查文件名是否以 .mp4 结尾 (不区分大小写)
                    if filepath.lower().endswith('.mp4'):
                        mp4_porn_files += 1
    
    except Exception as e:
        print(f"读取或处理文件时发生错误: {e}")
        return

    # 计算并打印结果
    if total_porn_files == 0:
        print("日志文件中没有找到被标记为'色情'的记录。")
    else:
        percentage = (mp4_porn_files / total_porn_files) * 100
        print("=" * 40)
        print("色情文件格式占比分析结果:")
        print("-" * 40)
        print(f"总共找到 {total_porn_files} 个被标记为 '色情' 的文件。")
        print(f"其中, .mp4 格式的文件有 {mp4_porn_files} 个。")
        print(f".mp4 格式的占比为: {percentage:.2f}%")
        print("=" * 40)

if __name__ == '__main__':
    analyze_mp4_percentage_in_porn()
