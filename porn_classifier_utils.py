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
    '电影', '电视剧', '教程', '课程', '讲座', '风景', '旅游', 'vlog', '发布会',
    'tutorial', 'course', 'lecture', 'travel', 'movie', 'show', 'S01E01', 'S10E12',
    'install', 'repack', 'The', 'Of', 'And', 'Trailer', 'Official'
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
