import av
import io

def get_video_duration(video_data: bytes) -> float:
    """
    Parses the given video data (in-memory bytes) to get its duration.

    :param video_data: A byte string containing at least the header of the video file.
    :return: The duration of the video in seconds, or 0.0 if it cannot be determined.
    """
    try:
        with io.BytesIO(video_data) as file_like:
            with av.open(file_like) as container:
                if container.duration is None:
                    return 0.0
                # Duration is in AV_TIME_BASE units, convert to seconds
                return container.duration / av.time_base
    except av.AVError as e:
        print(f"PyAV error while getting duration: {e}")
        return 0.0
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return 0.0

def generate_screenshot_from_chunk(video_chunk: bytes):
    """
    Generates a screenshot from a chunk of video data.
    It decodes the first valid frame found in the chunk.

    :param video_chunk: A byte string containing a decodable part of a video stream.
    :return: A PIL Image object of the screenshot, or None.
    """
    try:
        with io.BytesIO(video_chunk) as file_like:
            with av.open(file_like) as container:
                # Decode the first frame we can get from this chunk
                for frame in container.decode(video=0):
                    return frame.to_image()
    except av.AVError as e:
        print(f"PyAV error while generating screenshot: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# The more complex logic of seeking to a timestamp will be in the orchestrator,
# as it needs to coordinate downloading the correct chunks first.

# Example usage (for testing purposes)
if __name__ == '__main__':
    print("video.py is meant to be used as a module.")
    # To test this, you would need a valid chunk of a video file.
    # e.g. a small .mp4 file itself could act as a chunk.
    # with open("sample_chunk.mp4", "rb") as f:
    #     chunk_data = f.read()
    #     img = generate_screenshot_from_chunk(chunk_data)
    #     if img:
    #         img.save("screenshot.jpg")
    #         print("Screenshot saved to screenshot.jpg")
