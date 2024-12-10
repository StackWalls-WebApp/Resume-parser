# extractor.py
import os
import re
import nltk
import spacy
import docx2txt
from pdfminer.high_level import extract_text as pdf_extract_text

# Load spaCy model and NLTK data once
nltk.download('punkt', quiet=True)
nlp = spacy.load('en_core_web_sm')

def extract_text_from_pdf(file_path):
    try:
        text = pdf_extract_text(file_path)
        return text
    except Exception as e:
        raise Exception(f"Error extracting text from PDF: {e}")

def extract_text_from_docx(file_path):
    try:
        text = docx2txt.process(file_path)
        return text
    except Exception as e:
        raise Exception(f"Error extracting text from DOCX: {e}")

def extract_text(file_path):
    if file_path.lower().endswith('.pdf'):
        return extract_text_from_pdf(file_path)
    elif file_path.lower().endswith(('.docx', '.doc')):
        return extract_text_from_docx(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

def extract_skills(text):
    skills_file = 'skills.txt'
    if not os.path.exists(skills_file):
        return []
    with open(skills_file, 'r', encoding='utf-8') as f:
        skills_db = [line.strip().lower() for line in f]
    skills_found = set()
    text_lower = text.lower()
    for skill in skills_db:
        pattern = r'(?i)\b' + re.escape(skill.lower()) + r'\b'
        matches = re.findall(pattern, text_lower)
        if matches:
            skills_found.add(skill)
    return list(skills_found)

def extract_sections(text):
    section_headings = [
        'about me', 'summary', 'objective', 'profile',
        'experience', 'work experience', 'employment history', 'professional experience', 'work history',
        'education', 'educational background', 'academic background',
        'skills', 'technical skills', 'expertise', 'key skills',
        'projects', 'certifications', 'achievements', 'publications', 'hobbies', 'interests', 'languages',
        'contact me', 'contact', 'personal details', 'my contact'
    ]
    pattern = r'(?i)^\s*(%s)\s*$' % '|'.join([re.escape(heading) for heading in section_headings])
    lines = text.split('\n')
    sections = {}
    current_section = None
    content = []
    for line in lines:
        line_strip = line.strip()
        if re.match(pattern, line_strip):
            if current_section and content:
                sections[current_section] = '\n'.join(content).strip()
                content = []
            current_section = line_strip.lower()
        elif current_section:
            content.append(line)
    if current_section and content:
        sections[current_section] = '\n'.join(content).strip()
    return sections

def extract_experience(sections):
    experience_sections = [
        'experience', 'work experience', 'employment history', 'professional experience', 'work history'
    ]
    for key in sections.keys():
        if any(section in key for section in experience_sections):
            return sections[key]
    return None

def extract_about_me(sections, text):
    about_me_sections = ['about me', 'summary', 'objective', 'profile']
    for key in sections.keys():
        if any(section in key for section in about_me_sections):
            return sections[key]
    # If no explicit about me section, fallback to the first paragraph
    paragraphs = text.strip().split('\n\n')
    if paragraphs:
        about_me = paragraphs[0]
        if len(about_me) < 1000:
            return about_me.strip()
    return None
