import streamlit as st
from moviepy import VideoFileClip

def convert_mp4_to_webp(mp4_temp_file_path, webp_temp_file_path):
    """Converts MP4 to WebP using moviepy."""
    try:
        video_clip = VideoFileClip(mp4_temp_file_path)
        video_clip.write_videofile(
            webp_temp_file_path,
            codec='libwebp',
            preset='default',
            ffmpeg_params=[
                '-vf', 'fps=25',
                '-lossless', '0',
                '-q:v', '80', 
                '-loop', '0'
            ],
            audio=False,
            logger=None
        )
        video_clip.close()
        return True
    except Exception as e:
        st.error(f"Failed to convert video: {e}")
        return False
    
