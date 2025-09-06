import pytest
from main import is_porn_video

# 使用 parametrize 装饰器来定义一组应该返回True的测试用例
@pytest.mark.parametrize("filename", [
    "[国产偷拍]女友闺蜜来家里做客[1080p].mp4", # Removed space
    "FC2-PPV-1234567 無碼破解版.avi",
    "Tokyo-Hot-n1234 [uncensored] JAV classic.mkv",
    "ABP-123 My Wife's Affair.mp4",
    "some-random-text-SOD-456.mkv",
    "【中文字幕】巨乳人妻的誘惑.mp4",
    "[国产自拍]无码流出高清.mp4" # New realistic test case
])
def test_positive_cases(filename):
    """
    测试应该被分类为色情内容的样本 (True)
    """
    assert is_porn_video(filename) is True

# 定义一组应该返回False的测试用例
@pytest.mark.parametrize("filename", [
    "The.Matrix.1999.BluRay.1080p.mkv",
    "Python Tutorial for Beginners (2023).mp4",
    "Lecture 5 - Advanced Algorithms.mp4",
    "photo_of_my_dog.jpg",
    "Install_guide_v2.zip",
    "TVShow.S01E05.HDTV.x264-LOL.mp4"
])
def test_negative_cases(filename):
    """
    测试应该被分类为正常内容的样本 (False)
    """
    assert is_porn_video(filename) is False

# 定义一组边缘情况和已知失败用例的测试
@pytest.mark.parametrize("filename, expected", [
    ("", False),
    ("   ", False),
    (None, False),
    (12345, False),
    # 已知的失败用例 (False Positives)
    pytest.param("My Travel Vlog - Episode 03.mp4", False, marks=pytest.mark.xfail(reason="Known false positive")),
    pytest.param("Family Guy S20E10.avi", False, marks=pytest.mark.xfail(reason="Known false positive")),
    pytest.param("My Awesome Holiday Trip.mov", False, marks=pytest.mark.xfail(reason="Known false positive"))
])
def test_edge_and_known_failing_cases(filename, expected):
    """
    测试边缘情况和已知的模型弱点
    """
    assert is_porn_video(filename) is expected
