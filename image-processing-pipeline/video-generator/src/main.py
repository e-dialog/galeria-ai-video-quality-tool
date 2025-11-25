"""
Video generation module using Google GenAI VEO model.
1. Generates a video from a single image using GenAI VEO model.
2. Uploads the generated video to a specified GCS bucket.
3. Logs the video generation event to BigQuery.
"""
from utils.genai_utils import generate_video
from utils.logging_utils import log_error
from utils.storage_utils import move_assets_to_processed

def main(request) -> tuple[str, int]:
    data: dict = request.get_json(silent=True)

    gtin: str | None = data.get('gtin')
    category: str | None = data.get('category')
    assets: list[str] | None = data.get('assets')

    assert gtin is not None, "gtin is required"
    assert category is not None, "category is required"
    assert assets is not None, "Assets are required"

    try:
        video_gcs_uri, used_prompt = generate_video(
            gtin,
            category,

            # Video generation can only take 3 subject references
            # https://ai.google.dev/gemini-api/docs/video?example=dialogue#reference-images
            assets[:3],
        )

    except Exception as exception:
        print(f"Error during video generation: {exception}")
        log_error(gtin, assets, str(exception))
        return str(exception), 500

    move_assets_to_processed(gtin, assets, video_gcs_uri, used_prompt)

    return "OK", 200


# For local testing purposes. Run `python main.py`
if __name__ == '__main__':
    gtin: str = "4017182010037"
    category: str = "female_clothes"
    assets: list[str] = [
        "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/4017182010037_Laudert_01.jpg",
        "gs://galeria-veo3-input-assets-galeria-retail-api-dev/female_clothes/4017182010037_Laudert_02.jpg"
    ]

    video_gcs_uri, prompt = generate_video(
        gtin=gtin,
        category=category,
        gcs_uris=assets
    )

    move_assets_to_processed(gtin, assets, video_gcs_uri, prompt)
