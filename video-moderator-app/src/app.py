import streamlit as st
from cached_resources import get_videos_to_review
from utils.storage_utilities import (approve_video, regenerate_video,
                                     remove_video, file_exists)
from utils.task_queue_tools import publish_task

# --- UI ---
st.set_page_config(layout="wide", page_title="Video Moderation Tool")
st.title("📹 Video Moderation Tool")

if 'moderator_id' not in st.session_state:
    st.sidebar.header("Login")
    
    moderator_email: str = st.sidebar.text_input(
        label="Please enter your email:", 
        key="moderator_email_input"
    )
    
    if st.sidebar.button("Login"):
        if moderator_email and '@' in moderator_email:
            st.session_state.moderator_id = moderator_email
            st.rerun()
        else:
            st.sidebar.error("Invalid email.")
    
    st.info("Please log in to start.")
    
else:
    moderator_id: str = st.session_state.moderator_id
    st.sidebar.success(f"Logged in as: **{moderator_id}**")
    
    with st.sidebar.expander("Moderator Actions"):
        refresh_button = st.button("🔄 Publish new assets for video generation", on_click=publish_task)

    if 'video_queue' not in st.session_state:
        st.session_state.video_queue = get_videos_to_review()

    if not st.session_state.video_queue:
        st.success("🎉 All videos have been reviewed!")
        
        if st.button("🔄 Check for New Videos"):
            st.cache_data.clear()
            st.session_state.video_queue = get_videos_to_review()
            st.rerun()

    else:
        current_video_data = st.session_state.video_queue[0]
        gtin: str = current_video_data["gtin"]
        reference_image_gcs_uris: list[str] = current_video_data["reference_image_gcs_uris"]
        video_path_gcs: str = current_video_data["video_gcs_uri"]
        initial_prompt: str = current_video_data["prompt"]
        initial_notes: str = current_video_data.get("notes", "")
        category: str = current_video_data["category"]
        
        try:
            video_file_url: str = f"https://storage.cloud.google.com/{video_path_gcs.replace('gs://', '')}"
            if not file_exists(video_path_gcs):
                st.session_state.video_queue.pop(0)
                st.rerun()
           
        except Exception as e:
            st.error(f"Error getting assets: {e}")
            st.stop()

        # UI LAYOUT: 50/50 Split
        col1, col2 = st.columns([0.5, 0.5])
        
        # --- LEFT COLUMN: INPUTS (Images + Prompt) ---
        with col1:
            st.markdown(f"**GTIN:** `{gtin}`. **Category:** `{category}`")
            
            number_of_columns: int = len(reference_image_gcs_uris)
            reference_image_columns: list = st.columns(number_of_columns)
            
            for index, reference in enumerate(reference_image_gcs_uris):
                image_url: str = f"https://storage.cloud.google.com/{reference.replace('gs://', '')}"
                column = reference_image_columns[index]
                
                with column:
                    st.image(image_url, caption=f"Reference Image {index + 1}", use_container_width=True)
            
            # Reduced height for prompt to save screen space (was 200)
            edited_prompt = st.text_area("Prompt:", value=initial_prompt, height=120)
            edited_notes = st.text_area("Notes:", value=initial_notes, height=80)

        # --- RIGHT COLUMN: OUTPUT (Video + Actions) ---
        with col2:

             with st.container(border=False, horizontal_alignment="center"):
                st.video(video_file_url, width="stretch", autoplay=True)

                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("✅ Approve", use_container_width=True):
                        approve_video(
                            gtin=gtin,
                            notes=edited_notes,
                            moderator=st.session_state.moderator_id,
                            video_gcs_uri=video_path_gcs,
                            category=category,
                            reference_image_gcs_uris=reference_image_gcs_uris,
                            prompt=edited_prompt
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()
                        
                with c2:                
                    if st.button("♻️ Regenerate", width="stretch"):
                        regenerate_video(
                            gtin=gtin,
                            category=category,
                            video_gcs_uri=video_path_gcs,
                            moderator=st.session_state.moderator_id,
                            notes=edited_notes,
                            reference_image_gcs_uris=reference_image_gcs_uris,
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()
                    
                with c3:
                    if st.button("🗑️ Remove", width="stretch"):
                        remove_video(
                            gtin=gtin,
                            moderator=st.session_state.moderator_id,
                            video_gcs_uri=video_path_gcs,
                            reference_image_gcs_uris=reference_image_gcs_uris,
                            category=category,
                            notes=edited_notes
                        )
                        st.session_state.video_queue.pop(0)
                        st.rerun()