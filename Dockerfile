# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for pdfminer and other tools
# This can vary depending on your dependencies.
RUN apt-get update && apt-get install -y \
    build-essential \
    poppler-utils \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Download spacy model
RUN python -m spacy download en_core_web_sm

# Copy the rest of the application code into the container
COPY . .

# Expose the port your Flask app runs on
EXPOSE 5000

# Set environment variables as needed (optional)
# ENV FLASK_ENV=production

# Command to run the Flask app
CMD ["python", "app.py"]
