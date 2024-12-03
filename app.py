# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from resume_processor import (
    process_resumes, process_links,
    process_folder_resumes, process_file_resume
)
from pymongo import MongoClient
import os

app = Flask(__name__)
CORS(app)

@app.route('/process_dataframe', methods=['POST'])
def process_dataframe():
    try:
        data = request.get_json()
        if data is None:
            return "No JSON data provided", 400
        df = pd.DataFrame(data)
        # Process the resumes
        updated_df = process_resumes(df)
        # Return the updated DataFrame as JSON
        return updated_df.to_json(orient='records'), 200
    except Exception as e:
        return f"Error processing DataFrame: {e}", 500
    
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
        # Return the results as JSON
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
        # Process the resumes
        updated_df = process_resumes(df)
        # Return the updated DataFrame as JSON
        return updated_df.to_json(orient='records'), 200
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
        # Return the results as JSON
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
            return jsonify(result), 200
        else:
            return "Failed to process file", 500
    except Exception as e:
        return f"Error processing file: {e}", 500

if __name__ == '__main__':
    app.run(debug=True)
