# Video Quality Tool
> This repository consists of 2 main components:
> - Image Processing Pipeline
> - Video Evaluation App

## Image Processing Pipeline
The Image Processing Pipeline is orchestrated using Google Cloud services. When a new image asset is uploaded to the designated "new-assets" Cloud Storage bucket, a Cloud Storage event listener triggers a Cloud Function or Cloud Run service acting as a "Task Generator." This generator is responsible for creating a new job: it logs the asset's details and a unique job ID into a BigQuery table for tracking, and then dispatches a Cloud Tasks job to another service responsible for video generation.

Upon successful video generation, both the original image and the newly created video are moved from the "new-assets" bucket to a "processed-assets" bucket. This ensures that only unprocessed images remain in the initial bucket.

To handle cases where a generated video is no longer needed, a separate Cloud Storage event listener monitors the "processed-assets" bucket for deletion events specific to video files. If a video is deleted, a corresponding Cloud Function identifies the associated original image (e.g., via metadata or naming conventions) and moves it back to the "new-assets" bucket, making it available for re-processing or new video generation.

## Video Evaluation App
The Video Evaluation App is a web application built using Streamlit that allows users to evaluate videos for quality. It provides a user-friendly interface for users to upload videos, view their content, and provide feedback on various quality metrics. The app also includes features for tracking user actions and logging activities on a BigQuery table.

The app is deployed on Google Cloud Run and is accessible via a URL. The app is also configured to run locally for development and testing purposes.
