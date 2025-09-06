import csv
import os
import shutil

LOG_FILE = 'classification_log.csv'
TEMP_FILE = 'classification_log.tmp'

def correct_xxx_labels():
    """
    读取 a classification_log.csv 文件,
    将所有filepath中包含 'xxx' 的行的label修正为 1。
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
                    writer.writerow(row) # 保持格式不正确的行
                    continue

                filepath, label = row
                
                # 检查filepath是否包含 'xxx' (不区分大小写)
                if 'xxx' in filepath.lower():
                    if label != '1':
                        label = '1' # 修正label
                        rows_corrected += 1
                
                writer.writerow([filepath, label])

        # 用修正后的文件安全地替换原文件
        shutil.move(TEMP_FILE, LOG_FILE)
        
        print("=" * 40)
        print("标签修正完成！")
        print(f"总共修正了 {rows_corrected} 行的标签。")
        print(f"文件 '{LOG_FILE}' 已被更新。")
        print("=" * 40)

    except Exception as e:
        print(f"处理文件时发生错误: {e}")
        # 如果出错，删除可能已创建的临时文件
        if os.path.exists(TEMP_FILE):
            os.remove(TEMP_FILE)

if __name__ == '__main__':
    correct_xxx_labels()
