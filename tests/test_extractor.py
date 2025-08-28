# -*- coding: utf-8 -*-
import pytest
from pathlib import Path
from screenshot.extractor import H264KeyframeExtractor

# Get the directory of the current test file
TEST_DIR = Path(__file__).parent

@pytest.fixture(scope="module")
def moov_data():
    """Loads the moov.dat fixture."""
    moov_path = TEST_DIR / "fixtures" / "moov.dat"
    with open(moov_path, "rb") as f:
        return f.read()

class TestH264KeyframeExtractor:
    def test_initialization_and_parsing(self, moov_data):
        """
        Tests that the extractor initializes correctly and parses the moov box
        without errors, populating the keyframe and sample lists.
        """
        # WHEN the extractor is initialized with the moov data
        try:
            extractor = H264KeyframeExtractor(moov_data)
        except Exception as e:
            pytest.fail(f"H264KeyframeExtractor initialization failed with an exception: {e}")

        # THEN the parsing should be successful and populate the lists
        assert extractor is not None
        assert len(extractor.samples) > 0, "The samples list should not be empty."
        assert len(extractor.keyframes) > 0, "The keyframes list should not be empty."

        # AND the parsed attributes should be correct for a standard avc1 video
        assert extractor.mode == 'avc1', f"Expected mode 'avc1' but got '{extractor.mode}'"
        assert extractor.nal_length_size == 4, f"Expected NAL length size of 4 but got {extractor.nal_length_size}"
        assert extractor.timescale > 0, f"Timescale should be a positive integer but got {extractor.timescale}"
