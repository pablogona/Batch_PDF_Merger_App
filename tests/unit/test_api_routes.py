import pytest
from app import app

@pytest.fixture
def client():
    app.config['TESTING'] = True
    return app.test_client()

def test_process_pdfs_no_files(client):
    response = client.post('/api/process-pdfs')
    assert response.status_code == 400
    data = response.get_json()
    assert data['status'] == 'error'
    assert data['message'] == 'Missing files'
