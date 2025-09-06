import random
import os
import csv
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfVectorizer

# 从工具模块中导入共享的组件
from porn_classifier_utils import (
    preprocess_text,
    chinese_tokenizer,
    CustomFeatureTransformer,
    PORN_KEYWORDS,
    NORMAL_KEYWORDS
)

# 数据集生成 (保持在此文件中，因为它只用于训练)
def generate_filename(is_porn):
    if is_porn:
        # 对于色情样本，将中文关键词无缝拼接，模拟真实文件名
        keywords = random.sample(PORN_KEYWORDS, random.randint(1, 3))
        # 检查关键词是否主要为中文，如果是，则无缝拼接
        is_mostly_chinese = any('\u4e00' <= char <= '\u9fff' for char in keywords[0])
        if is_mostly_chinese:
            keyword_part = "".join(keywords)
        else:
            keyword_part = " ".join(keywords)

        code_part = f"{random.choice(['ABC', 'DEF', 'GHI'])}-{random.randint(100, 999)}"
        return f"[{keyword_part}] {code_part} some other text.mp4"
    else:
        # 正常样本保持空格分隔
        keyword_part = ' '.join(random.sample(NORMAL_KEYWORDS, random.randint(1, 3)))
        return f"{keyword_part} (2023) Official Trailer.mkv"

def generate_dataset(num_samples=2000):
    filenames = [generate_filename(True) for _ in range(num_samples // 2)]
    filenames.extend([generate_filename(False) for _ in range(num_samples // 2)])
    labels = [1] * (num_samples // 2) + [0] * (num_samples // 2)
    return filenames, labels

def load_data_from_csv(filepath):
    """从CSV文件加载数据"""
    filenames, labels = [], []
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # 跳过表头
        for row in reader:
            if len(row) == 2:
                filenames.append(row[0])
                labels.append(int(row[1]))
    return filenames, labels

def train_and_save_model(output_path='porn_classification_pipeline.joblib'):
    """
    构建、训练并保存整个分类流水线
    """
    log_file = 'classification_log.csv'
    # 检查是否存在真实的日志数据，如果存在则使用它，否则使用模拟数据
    if os.path.exists(log_file) and os.path.getsize(log_file) > 50: # 50 bytes as a threshold for non-empty
        print("1. Found existing log file. Training model from `classification_log.csv`...")
        filenames, labels = load_data_from_csv(log_file)
        if not filenames:
             print("   Log file is empty. Falling back to simulated data.")
             print("1. Generating simulated dataset...")
             filenames, labels = generate_dataset()
    else:
        print("1. No existing log file found. Training model from simulated data...")
        filenames, labels = generate_dataset()


    # 构建特征处理流水线
    feature_union = FeatureUnion([
        ('tfidf', TfidfVectorizer(
            tokenizer=chinese_tokenizer,
            preprocessor=preprocess_text,
            ngram_range=(1, 2),
            max_df=0.8,
            min_df=3
        )),
        ('custom', CustomFeatureTransformer())
    ])

    # 构建完整的分类流水线
    pipeline = Pipeline([
        ('features', feature_union),
        ('classifier', LogisticRegression(random_state=42, solver='liblinear'))
    ])

    # 训练流水线
    print("2. Training the full pipeline...")
    # 使用分层抽样 (stratify=labels) 确保即使在小数据集上，测试集也能保持原始的类别比例
    X_train, X_test, y_train, y_test = train_test_split(
        filenames, labels, test_size=0.2, random_state=42, stratify=labels
    )
    pipeline.fit(X_train, y_train)

    # 评估流水线
    print("3. Evaluating pipeline performance...")
    y_pred = pipeline.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['Normal', 'Porn']))

    # 保存整个流水线
    joblib.dump(pipeline, output_path)
    print(f"Complete pipeline saved to {output_path}")

if __name__ == "__main__":
    train_and_save_model()
