import joblib
import os

# 从工具模块中导入共享的组件
# 这是必需的，以便joblib/pickle可以找到这些自定义类的定义
from porn_classifier_utils import preprocess_text, CustomFeatureTransformer

# 流水线文件路径
PIPELINE_PATH = 'porn_classification_pipeline.joblib'

# 检查文件是否存在
if not os.path.exists(PIPELINE_PATH):
    raise FileNotFoundError(
        "Classification pipeline not found. "
        "Please run 'porn_classifier.py' first to train and save the pipeline."
    )

# 加载整个流水线
PIPELINE = joblib.load(PIPELINE_PATH)

def is_porn_video(filename: str) -> bool:
    """
    使用加载的分类流水线判断文件名是否可能指向一个色情视频。

    :param filename: str, a video filename.
    :return: bool, True if it's likely a porn video, False otherwise.
    """
    if not isinstance(filename, str) or not filename.strip():
        return False

    # 流水线会处理所有的预处理和特征提取
    # 我们只需要传递原始的文件名（在一个列表中）
    prediction = PIPELINE.predict([filename])

    # 返回结果
    return bool(prediction[0])

if __name__ == '__main__':
    # 示例用法
    test_filenames = [
        # 正面样本 (应该返回 True)
        "[国产偷拍] 女友闺蜜来家里做客 [1080p].mp4",
        "FC2-PPV-1234567 無碼破解版.avi",
        "Tokyo-Hot-n1234 [uncensored] JAV classic.mkv",
        "ABP-123 My Wife's Affair.mp4",

        # 负面样本 (应该返回 False)
        "The.Matrix.1999.BluRay.1080p.mkv",
        "My Travel Vlog - Episode 03.mp4",
        "Python Tutorial for Beginners (2023).mp4",
        "Family Guy S20E10.avi",
        "Lecture 5 - Advanced Algorithms.mp4",
        "photo_of_my_dog.jpg",
        ""
    ]

    for name in test_filenames:
        result = is_porn_video(name)
        print(f"Filename: '{name}' -> Likely Porn: {result}")
