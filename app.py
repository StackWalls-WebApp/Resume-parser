from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os
from pymongo import MongoClient
from resume_processor import (
    process_links, process_folder_resumes, process_file_resume, process_resumes
)
from werkzeug.utils import secure_filename
import json

app = Flask(__name__)
CORS(app)

# Ensure results directory exists
os.makedirs("results", exist_ok=True)

@app.route('/process_s3_links', methods=['POST'])
def process_s3_links():
    try:
        data = request.get_json()
        if data is None or 'links' not in data:
            return "No links provided", 400
        links = data['links']
        if not isinstance(links, list):
            return "Links should be a list", 400
        # Process the links
        results = process_links(links)
        # Save results locally
        result_path = os.path.join("results", "links_results.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        return jsonify(results), 200
    except Exception as e:
        return f"Error processing S3 links: {e}", 500


@app.route('/process_mongo', methods=['POST'])
def process_mongo():
    try:
        data = request.get_json()
        if data is None or 'connection_string' not in data or 'db_name' not in data or 'collection_name' not in data:
            return "Connection string, database name, and collection name are required", 400
        connection_string = data['connection_string']
        db_name = data['db_name']
        collection_name = data['collection_name']
        # Connect to MongoDB and fetch documents
        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]
        documents = list(collection.find())
        df = pd.DataFrame(documents)

        # Extract resumes from `resume.url` or `resume` and process them
        processed_results = []
        if 'resume' in df.columns or 'resume.url' in df.columns:
            from resume_processor import process_file_from_url
            for _, row in df.iterrows():
                resume_url = None
                if 'resume.url' in row and pd.notna(row['resume.url']):
                    resume_url = row['resume.url']
                elif 'resume' in row and pd.notna(row['resume']):
                    resume_val = row['resume']
                    if isinstance(resume_val, dict) and 'url' in resume_val:
                        resume_url = resume_val['url']
                    elif isinstance(resume_val, dict) and 'link' in resume_val:
                        resume_url = resume_val['link']
                    elif isinstance(resume_val, str):
                        resume_url = resume_val

                if resume_url:
                    try:
                        processed = process_file_from_url(resume_url)
                        processed['Resume URL'] = resume_url
                        processed_results.append(processed)
                    except:
                        pass

        # Save results locally
        result_path = os.path.join("results", "mongo_results.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(processed_results, f, ensure_ascii=False, indent=4)

        return jsonify(processed_results), 200
    except Exception as e:
        return f"Error processing MongoDB data: {e}", 500


@app.route('/process_folder', methods=['POST'])
def process_folder_endpoint():
    try:
        data = request.get_json()
        if data is None or 'folder_path' not in data:
            return "Folder path is required", 400
        folder_path = data['folder_path']
        if not os.path.isdir(folder_path):
            return "Invalid folder path", 400
        # Process resumes in the folder
        results = process_folder_resumes(folder_path)

        # Save results locally
        result_path = os.path.join("results", "folder_results.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        return jsonify(results), 200
    except Exception as e:
        return f"Error processing folder: {e}", 500


@app.route('/process_file', methods=['POST'])
def process_file_endpoint():
    try:
        data = request.get_json()
        if data is None or 'file_path' not in data:
            return "File path is required", 400
        file_path = data['file_path']
        if not os.path.isfile(file_path):
            return "Invalid file path", 400
        # Process the resume file
        result = process_file_resume(file_path)
        if result:
            # Save result locally
            filename = os.path.basename(file_path)
            result_path = os.path.join("results", f"{filename}_result.json")
            with open(result_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            return jsonify(result), 200
        else:
            return "Failed to process file", 500
    except Exception as e:
        return f"Error processing file: {e}", 500


@app.route('/upload_resume', methods=['POST'])
def upload_resume():
    """
    Accepts a file upload from Postman (form-data), processes the resume, and returns the result.
    Example: 
    Key: file, Type: file, Value: <select your resume file>
    """
    try:
        if 'file' not in request.files:
            return "No file part in request", 400
        
        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400
        
        filename = secure_filename(file.filename)
        upload_path = os.path.join("results", filename)
        file.save(upload_path)

        from resume_processor import process_resume

        # Process the uploaded file directly
        processed_data = process_resume(upload_path, upload_path)

        # Save the processed results
        result_path = os.path.join("results", f"{filename}_upload_result.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)

        return jsonify(processed_data), 200

    except Exception as e:
        return f"Error processing uploaded file: {e}", 500


@app.route('/process_dataframe', methods=['POST'])
def process_dataframe():
    """
    This endpoint takes a JSON body representing a DataFrame (list of dicts),
    processes the resumes, and returns the updated DataFrame as JSON.
    """
    try:
        data = request.get_json()
        if data is None:
            return "No JSON data provided", 400
        df = pd.DataFrame(data)
        updated_df = process_resumes(df)

        # Save results locally
        result_path = os.path.join("results", "dataframe_results.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(updated_df.to_dict(orient='records'), f, ensure_ascii=False, indent=4)

        return updated_df.to_json(orient='records'), 200
    except Exception as e:
        return f"Error processing DataFrame: {e}", 500


if __name__ == '__main__':
    # Run with python app.py for local development
    # In production, gunicorn will run it as per Dockerfile CMD
    app.run(host='0.0.0.0', port=5000, debug=True)
