# Use the official Streamlit image as a base
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt ./requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code and other files
# (A .dockerignore file is recommended to skip .git, etc.)
COPY . .

# Use the PORT environment variable provided by Cloud Run
ENV PORT 8080

# Expose the port
EXPOSE $PORT

# MODIFIED CMD:
# - Uses $PORT
# - Adds --server.address=0.0.0.0 (CRITICAL for Cloud Run)
# - Keeps --server.headless=true
CMD streamlit run app.py --server.port $PORT --server.address=0.0.0.0 --server.headless true