import csv
import os
import re
from collections import Counter
import jieba

# --- 配置 ---
LOG_FILE = 'classification_log.csv'
# 从我们的工具文件中导入现有的关键词列表，以避免重复推荐
try:
    from porn_classifier_utils import PORN_KEYWORDS, NORMAL_KEYWORDS
except ImportError:
    print("警告: 无法导入 porn_classifier_utils.py。将使用内置的空关键词列表。")
    PORN_KEYWORDS, NORMAL_KEYWORDS = [], []

# 定义一个简单的停用词列表 (可以根据需要扩充)
STOP_WORDS = {
    'the', 'a', 'an', 'in', 'on', 'at', 'of', 'for', 'to', 'and', 'or', 'is', 'are', 'was', 'were',
    'com', 'www', 'xyz', 'net', 'org', 'rar', 'zip', 'mp4', 'mkv', 'avi', 'wmv',
    '的', '了', '在', '是', '我', '你', '他', '她', '它', '和', '与', '或', '一个', '一些',
    '系列', '高清', '字幕', '中文', '合集'
}
# --- 脚本主逻辑 ---

def clean_and_tokenize(text):
    """清洗并分词文本"""
    # 移除URL和文件扩展名等常见噪声
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'\.(mp4|mkv|avi|wmv|zip|rar)$', '', text, flags=re.IGNORECASE)
    
    # 使用jieba分词
    tokens = jieba.lcut(text.lower())
    
    # 清理和过滤
    cleaned_tokens = []
    for token in tokens:
        # 移除非中英文字符
        token = re.sub(r'[^a-zA-Z\u4e00-\u9fff]', '', token)
        # 过滤掉停用词、短词和数字
        if token and token not in STOP_WORDS and not token.isdigit() and len(token) > 1:
            cleaned_tokens.append(token)
            
    return cleaned_tokens

def discover_new_keywords():
    """
    从 classification_log.csv 中发现新的关键词。
    """
    if not os.path.exists(LOG_FILE):
        print(f"错误: 日志文件 '{LOG_FILE}' 不存在。")
        return

    porn_texts = []
    normal_texts = []

    print("1. 正在读取和分组日志数据...")
    with open(LOG_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # 跳过表头
        for row in reader:
            if len(row) == 2:
                filepath, label = row
                if label == '0':
                    porn_texts.append(filepath)
                else:
                    normal_texts.append(filepath)

    if not porn_texts:
        print("错误: 日志中没有找到标记为 '1' (色情) 的数据，无法进行分析。")
        return
        
    print("2. 正在对文本进行分词和清洗...")
    porn_word_counts = Counter(token for text in porn_texts for token in clean_and_tokenize(text))
    normal_word_counts = Counter(token for text in normal_texts for token in clean_and_tokenize(text))

    print("3. 正在计算关键词分数并寻找候选词...")
    candidates = {}
    for word, count in porn_word_counts.items():
        # 忽略已经存在的关键词和出现次数太少的词
        if word in PORN_KEYWORDS or word in NORMAL_KEYWORDS or count < 3:
            continue
            
        # 计算分数：在色情文件中出现的次数 / (在正常文件中出现的次数 + 1)
        # 加1是为了防止除以零
        score = count / (normal_word_counts.get(word, 0) + 1)
        
        # 分数越高，说明这个词越有可能是色情关键词
        if score > 2.0: # 仅考虑分数大于2的词 (即在色情文档中出现频率至少是正常文档2倍以上)
            candidates[word] = score

    # 按分数从高到低排序
    sorted_candidates = sorted(candidates.items(), key=lambda item: item[1], reverse=True)

    print("\n" + "=" * 50)
    print("               新的色情关键词推荐 (Top 20)")
    print("=" * 50)
    if not sorted_candidates:
        print("未发现足够强大的新关键词。")
    else:
        print(f"{'关键词':<15} | {'分数 (越高越好)':<20}")
        print("-" * 50)
        for word, score in sorted_candidates[:20]:
            print(f"{word:<15} | {score:<20.2f}")
    print("=" * 50)
    print("\n提示: 请人工审核以上推荐词，然后将合适的词手动添加到 `porn_classifier_utils.py` 的 `PORN_KEYWORDS` 列表中。")


if __name__ == '__main__':
    discover_new_keywords()
