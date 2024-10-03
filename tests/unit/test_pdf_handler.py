import sys
import os
import io

# Add the project root directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from backend.pdf_handler import extract_demanda_information

def test_extract_demanda_information():
    with open('DEMANDA (1).pdf', 'rb') as pdf_file:
        pdf_stream = io.BytesIO(pdf_file.read())
        info = extract_demanda_information(pdf_stream)
        assert info['name'] == 'Expected Name'
