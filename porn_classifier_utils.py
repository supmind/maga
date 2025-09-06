import re
import jieba
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin

# 1. 定义关键词库和模式
PORN_KEYWORDS = [
    '国产', '偷拍', '自拍', '流出', '无码', '中文', '字幕', '成人', '巨乳', '少妇',
    '萝莉', '人妻', '熟女', '乱伦', '破解', 'VIP', 'amateur', 'homemade', 'teen',
    'milf', 'porn', 'xxx', 'hardcore', 'creampie', 'gangbang', 'blowjob', 'anal',
    'BDSM', 'fetish', 'uncensored', 'jav', 'asian', 'ebony', 'fc2', 'tokyo-hot',
    '1pondo', 'carribeancom', 'sod', 'prestige', 'moodyz', 's1', 'hmpa', '無修正'
]

NORMAL_KEYWORDS = [
    # General
    '电影', '电视剧', '教程', '课程', '讲座', '风景', '旅游', 'vlog', '发布会',
    'tutorial', 'course', 'lecture', 'travel', 'movie', 'show', 'trailer', 'official',
    'install', 'repack', 'cracked', 'software', 'game',
    # TV Shows
    'S01E01', 'S10E12', 'S03E05', 'HDTV', 'WEB-DL', 'WEBRip', 'BluRay', 'x264', 'x265', 'AAC',
    # Release Groups / Scene Tags
    'YIFY', 'RARBG', 'SPARKS', 'GECKOS', 'IMMERSE', 'GalaxyRG', 'TGx', 'EZTV',
    # Resolutions
    '720p', '1080p', '2160p', '4K',
    # Common English Words
    'The', 'Of', 'And', 'A', 'With', 'Season', 'Episode'
]

RELEASE_CODE_PATTERN = re.compile(r'\b[a-z]{2,5}-\d{3,5}\b', re.IGNORECASE)

# 2. 自定义特征提取器
class CustomFeatureTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        features = []
        for text in X:
            text_lower = text.lower()
            # 特征1: 是否包含番号
            has_release_code = 1 if RELEASE_CODE_PATTERN.search(text_lower) else 0
            # 特征2: 色情关键词计数
            porn_keyword_count = sum(1 for keyword in PORN_KEYWORDS if keyword.lower() in text_lower)
            # 特征3: 正常关键词计数
            normal_keyword_count = sum(1 for keyword in NORMAL_KEYWORDS if keyword.lower() in text_lower)

            features.append([
                has_release_code,
                porn_keyword_count,
                normal_keyword_count
            ])
        return np.array(features)

# 文本预处理函数
def preprocess_text(text):
    return RELEASE_CODE_PATTERN.sub(r' \g<0> ', text)

# 中文分词器
def chinese_tokenizer(text):
    """
    使用jieba进行分词
    """
    return jieba.lcut(text)

def is_target_language(text: str) -> bool:
    """
    检查文本是否主要由中文、日文或英文字符组成。
    如果检测到大量其他语言（如西里尔文、韩文）的字符，则返回False。
    """
    if not text or text.isspace():
        return True # 空或空白字符串不属于任何特定语言，默认通过

    # 定义各种语言字符的正则表达式
    # CJK Unified Ideographs: 中日韩统一表意文字
    # Hiragana, Katakana: 日文平假名、片假名
    # Hangul: 韩文
    # Cyrillic: 俄文等西里尔字母
    cjk_chars = re.findall(r'[\u4e00-\u9fff\u3040-\u30ff]', text)
    english_chars = re.findall(r'[a-zA-Z]', text)
    cyrillic_chars = re.findall(r'[\u0400-\u04ff]', text)
    hangul_chars = re.findall(r'[\uac00-\ud7a3]', text)

    # 计算目标语言和非目标语言的字符数
    target_lang_char_count = len(cjk_chars) + len(english_chars)
    other_lang_char_count = len(cyrillic_chars) + len(hangul_chars)

    total_chars = target_lang_char_count + other_lang_char_count

    # 如果完全没有识别出任何目标或非目标语言的字符，则默认通过
    if total_chars == 0:
        return True

    # 如果非目标语言的字符数量超过了目标语言字符，则认为是“其他语言”
    # 或者如果非目标语言字符占比超过30%，也认为是“其他语言”
    if other_lang_char_count > target_lang_char_count:
        return False
    if other_lang_char_count / total_chars > 0.3:
        return False

    return True
