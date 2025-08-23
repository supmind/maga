import av
import os
import traceback

def log(infohash, message):
    """Helper for consistent logging."""
    print(f"[{infohash}][Orchestrator:{os.getpid()}] {message}")

def create_screenshots_from_stream(file_like_object, infohash: str, queue, num_screenshots: int = 20):
    container = None
    log(infohash, "Starting screenshot creation process.")
    try:
        log(infohash, "Attempting to open stream with PyAV...")
        container = av.open(file_like_object, "r")
        log(infohash, "Stream opened successfully.")

        if not container.streams.video:
            log(infohash, "ERROR - No video streams found in container.")
            return

        video_stream = container.streams.video[0]
        log(infohash, f"Found video stream. Codec: {video_stream.codec_context.name}, Rate: {video_stream.average_rate}")

        if container.duration is None:
            log(infohash, "ERROR - Could not determine container duration.")
            return

        duration_sec = container.duration / av.time_base
        log(infohash, f"Video duration is {duration_sec:.2f}s.")

        screenshots_taken = 0
        for i in range(num_screenshots):
            timestamp_sec = (duration_sec / (num_screenshots + 1)) * (i + 1)
            try:
                log(infohash, f"Processing screenshot #{i+1} at {timestamp_sec:.2f}s...")

                # Seek to the keyframe before the target timestamp
                seek_target = int(timestamp_sec * av.time_base)
                container.seek(seek_target, backward=True, any_frame=False, stream=video_stream)
                log(infohash, f"Seeked to {timestamp_sec:.2f}s.")

                # Decode frames and find the one closest to the target timestamp
                best_frame = None
                min_diff = float('inf')

                for frame in container.decode(video=0):
                    frame_timestamp_sec = frame.pts * frame.time_base
                    diff = abs(frame_timestamp_sec - timestamp_sec)

                    if diff < min_diff:
                        min_diff = diff
                        best_frame = frame

                    if frame_timestamp_sec > timestamp_sec + 1:
                        break

                if best_frame is None:
                    log(infohash, f"WARNING - Could not decode any frames near {timestamp_sec:.2f}s.")
                    continue

                frame = best_frame
                output_filename = f"screenshots/{infohash}_{int(frame.pts * frame.time_base)}.jpg"
                frame.to_image().save(output_filename)
                screenshots_taken += 1
                log(infohash, f"SUCCESS - Saved screenshot to {output_filename}")
                if queue:
                    queue.put(1)

            except StopIteration:
                log(infohash, f"WARNING - Could not decode frame at {timestamp_sec:.2f}s (end of stream?).")
                break
            except Exception as e:
                log(infohash, f"ERROR - Failed to generate screenshot at {timestamp_sec:.2f}s: {e}")

    except av.error.InvalidDataError as e:
        log(infohash, f"ERROR - Failed to open stream, file data may be corrupt or invalid: {e}")
    except Exception as e:
        log(infohash, f"FATAL - An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        if container:
            container.close()
        log(infohash, f"Finished screenshot creation process. Total screenshots taken: {screenshots_taken}")
