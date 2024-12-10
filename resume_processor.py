import os
import pandas as pd
from downloader import download_file_from_url
from extractor import (
    extract_text, extract_skills, extract_sections,
    extract_experience, extract_about_me
)
from concurrent.futures import ThreadPoolExecutor


def process_resume(file_path, resume_url):
    """
    Process a single resume file and return extracted data.
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        raise Exception("Downloaded file is missing or empty")

    text = extract_text(file_path)
    if not text:
        raise Exception("No text could be extracted from the resume")

    sections = extract_sections(text)
    skills = extract_skills(text)
    experience = extract_experience(sections)
    about_me = extract_about_me(sections, text)

    resume_data = {
        'Skills': skills,
        'Experience': experience,
        'About Me': about_me
    }
    return resume_data


def process_file_from_url(resume_url):
    """
    Process a single resume URL.
    """
    downloaded_file = download_file_from_url(resume_url)
    extracted_data = process_resume(downloaded_file, resume_url)
    return extracted_data


def process_links(links, max_workers=20):
    """
    Processes a list of resume URLs concurrently and returns extracted data for each.
    Uses a larger ThreadPoolExecutor to speed up processing.
    """
    extracted_resumes = []

    def process_link(resume_url):
        try:
            extracted_data = process_file_from_url(resume_url)
            extracted_data['Resume URL'] = resume_url
            extracted_resumes.append(extracted_data)
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_link, links)

    return extracted_resumes


def process_folder_resumes(folder_path, max_workers=20):
    """
    Processes all resumes in a folder concurrently and returns extracted data.
    """
    extracted_resumes = []

    def process_file(file_path):
        try:
            extracted_data = process_resume(file_path, file_path)
            extracted_data['File Path'] = file_path
            extracted_resumes.append(extracted_data)
        except Exception:
            pass

    resume_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(('.pdf', '.doc', '.docx')):
                resume_files.append(os.path.join(root, file))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_file, resume_files)

    return extracted_resumes


def process_file_resume(file_path):
    """
    Processes a single resume file from a local path.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError("File not found")
    extracted_data = process_resume(file_path, file_path)
    extracted_data['File Path'] = file_path
    return extracted_data


def process_resumes(df, max_workers=20):
    """
    Processes resumes from a pandas DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing resume links.

    Returns:
    pandas.DataFrame: The updated DataFrame with extracted skills and resume_work_description.
    """
    try:
        # Ensure necessary columns exist in the DataFrame
        if 'resume' not in df.columns and 'resume.url' not in df.columns:
            raise ValueError("DataFrame does not contain 'resume' or 'resume.url' columns.")

        records = df.to_dict(orient='records')

        def process_row(row):
            try:
                resume_url = None
                if 'resume.url' in row and pd.notna(row['resume.url']):
                    resume_url = row['resume.url']
                elif 'resume' in row and pd.notna(row['resume']):
                    resume = row['resume']
                    if isinstance(resume, dict) and 'url' in resume:
                        resume_url = resume['url']
                    elif isinstance(resume, dict) and 'link' in resume:
                        resume_url = resume['link']
                    elif isinstance(resume, str):
                        resume_url = resume
                    else:
                        resume_url = None

                if not resume_url:
                    # No resume link found, skip processing
                    return row

                downloaded_file = download_file_from_url(resume_url)
                extracted_data = process_resume(downloaded_file, resume_url)

                # Update 'skills' field
                if extracted_data.get('Skills'):
                    existing_skills = row.get('skills', [])
                    # Handle different data types for existing_skills
                    if isinstance(existing_skills, float) and pd.isna(existing_skills):
                        existing_skills = []
                    elif isinstance(existing_skills, str):
                        existing_skills = [s.strip() for s in existing_skills.split(',') if s.strip()]
                    elif not isinstance(existing_skills, list):
                        existing_skills = []
                    updated_skills = list(set(existing_skills + extracted_data['Skills']))
                    row['skills'] = updated_skills

                # Create a new column 'resume_work_description'
                if extracted_data.get('Experience') or extracted_data.get('About Me'):
                    work_description = ''
                    if extracted_data.get('Experience'):
                        work_description += extracted_data['Experience'] + '\n'
                    if extracted_data.get('About Me'):
                        work_description += extracted_data['About Me']
                    row['resume_work_description'] = work_description.strip()

                return row
            except Exception:
                return row

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_row, records))

        updated_df = pd.DataFrame(results)
        return updated_df
    except Exception as e:
        raise Exception(f"Error processing resumes: {e}")
