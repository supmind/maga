import csv
import os
import shutil

# --- 配置 ---
LOG_FILE = 'classification_log.csv'
TEMP_FILE = 'classification_log.tmp'

# 定义需要修正为 '1' 的关键词列表 (不区分大小写)
CORRECTION_KEYWORDS = [
    'madoubt.com',
    '10musume',
    '1pondo',
    'heyzo',
    'ssis',  # 使用小写以便进行不区分大小写的比较
    'fc2ppv' # fc2-ppv 在预处理后可能会变成 fc2ppv
]
# --- 脚本主逻辑 ---

def correct_labels_by_keyword():
    """
    根据关键词列表，修正 classification_log.csv 文件中的标签。
    """
    if not os.path.exists(LOG_FILE):
        print(f"错误: 日志文件 '{LOG_FILE}' 不存在。")
        return

    rows_corrected = 0
    try:
        with open(LOG_FILE, 'r', newline='', encoding='utf-8') as infile,              open(TEMP_FILE, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # 写入表头
            header = next(reader)
            writer.writerow(header)

            # 逐行处理
            for row in reader:
                if len(row) != 2:
                    writer.writerow(row)
                    continue

                filepath, label = row
                original_label = label
                
                # 检查filepath是否包含任一修正关键词
                filepath_lower = filepath.lower()
                for keyword in CORRECTION_KEYWORDS:
                    if keyword in filepath_lower:
                        label = '1' # 发现关键词，修正label
                        break # 找到一个就行，跳出内层循环
                
                if original_label != label:
                    rows_corrected += 1
                
                writer.writerow([filepath, label])

        # 用修正后的文件安全地替换原文件
        shutil.move(TEMP_FILE, LOG_FILE)
        
        print("=" * 40)
        print("关键词标签修正完成！")
        print(f"总共修正了 {rows_corrected} 行的标签。")
        print(f"文件 '{LOG_FILE}' 已被更新。")
        print("=" * 40)

    except Exception as e:
        print(f"处理文件时发生错误: {e}")
        # 如果出错，删除可能已创建的临时文件
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)

if __name__ == '__main__':
    correct_labels_by_keyword()
