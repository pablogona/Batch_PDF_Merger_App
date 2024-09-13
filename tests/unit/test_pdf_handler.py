from backend.pdf_handler import normalize_text

def test_normalize_text():
    assert normalize_text('IÑIGO') == 'inigo'
    assert normalize_text('InIGO') == 'inigo'
    assert normalize_text('iñigo') == 'inigo'
