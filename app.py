# app.py
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os
import zipfile
import tempfile
from resume_processor import (
    process_resumes, process_links,
    process_folder_resumes, process_file_resume
)
from pymongo import MongoClient

app = Flask(__name__)
CORS(app)

@app.route('/process_dataframe', methods=['POST'])
def process_dataframe():
    """
    Process DataFrame as before:
    Expects a JSON payload with resume data and returns updated DataFrame as JSON.
    """
    try:
        data = request.get_json()
        if data is None:
            return "No JSON data provided", 400
        df = pd.DataFrame(data)
        updated_df = process_resumes(df)
        return updated_df.to_json(orient='records'), 200
    except Exception as e:
        return f"Error processing DataFrame: {e}", 500

@app.route('/process_s3_links', methods=['POST'])
def process_s3_links_endpoint():
    """
    Now accepts a CSV file with a column 'links' containing resume URLs.
    Returns a CSV file with extracted data.
    """
    try:
        if 'file' not in request.files:
            return "No file part in the request", 400
        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400

        # Read the CSV file containing links
        df = pd.read_csv(file)
        if 'links' not in df.columns:
            return "CSV must contain a 'links' column", 400

        links = df['links'].dropna().tolist()
        results = process_links(links)

        # Convert results to DataFrame and save as CSV
        result_df = pd.DataFrame(results)
        output_file = 'final_output.csv'
        result_df.to_csv(output_file, index=False)

        return send_file(output_file, as_attachment=True, attachment_filename='final_output.csv'), 200
    except Exception as e:
        return f"Error processing S3 links: {e}", 500

@app.route('/process_mongo', methods=['POST'])
def process_mongo():
    """
    Expects JSON payload:
    {
      "connection_string": "mongodb+srv://user:pass@cluster.mongodb.net",
      "db_name": "myDB",
      "collection_name": "myCollection"
    }

    After processing, returns a CSV file.
    """
    try:
        data = request.get_json()
        if data is None or 'connection_string' not in data or 'db_name' not in data or 'collection_name' not in data:
            return "Connection string, database name, and collection name are required", 400
        connection_string = data['connection_string']
        db_name = data['db_name']
        collection_name = data['collection_name']

        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        documents = list(collection.find())
        df = pd.DataFrame(documents)

        updated_df = process_resumes(df)
        output_file = 'final_output.csv'
        updated_df.to_csv(output_file, index=False)

        return send_file(output_file, as_attachment=True, attachment_filename='final_output.csv'), 200
    except Exception as e:
        return f"Error processing MongoDB data: {e}", 500

@app.route('/process_folder', methods=['POST'])
def process_folder_endpoint():
    """
    Accepts a ZIP file uploaded by the user containing multiple resumes.
    Extracts them, processes all supported resumes, and returns a CSV file.
    """
    try:
        if 'file' not in request.files:
            return "No file part in the request", 400
        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400

        # Create a temporary directory to extract files
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, 'resumes.zip')
            file.save(zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Process resumes in the extracted folder
            results = process_folder_resumes(temp_dir)

        # Convert results to DataFrame and save as CSV
        result_df = pd.DataFrame(results)
        output_file = 'final_output.csv'
        result_df.to_csv(output_file, index=False)

        return send_file(output_file, as_attachment=True, attachment_filename='final_output.csv'), 200
    except Exception as e:
        return f"Error processing folder: {e}", 500

@app.route('/process_file', methods=['POST'])
def process_file_endpoint():
    """
    Accepts a single resume file upload (PDF or DOC/DOCX).
    Processes and returns a CSV file with extracted data.
    """
    try:
        if 'file' not in request.files:
            return "No file part in the request", 400
        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400

        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, file.filename)
            file.save(file_path)
            result = process_file_resume(file_path)
        
        # Convert result to DataFrame and save as CSV
        result_df = pd.DataFrame([result])
        output_file = 'final_output.csv'
        result_df.to_csv(output_file, index=False)

        return send_file(output_file, as_attachment=True, attachment_filename='final_output.csv'), 200
    except Exception as e:
        return f"Error processing file: {e}", 500

if __name__ == '__main__':
    # In production, do not use debug=True. Configure Gunicorn or another WSGI server.
    app.run(host='0.0.0.0', port=5000)
