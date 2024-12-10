# Dockerfile
# Use the official Python image as base
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV APP_HOME=/app

# Create the working directory
WORKDIR $APP_HOME

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Download spaCy English model separately
RUN python -m spacy download en_core_web_sm

# Download NLTK data
RUN python -m nltk.downloader punkt

# Copy the rest of the application code
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Set the entry point to run the app with gunicorn for better performance
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app", "--workers", "3", "--threads", "2", "--timeout", "120"]
