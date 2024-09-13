def test_get_or_create_folder(mocker):
    mock_drive_service = mocker.Mock()
    mock_drive_service.files().list().execute.return_value = {'files': []}
    mock_drive_service.files().create().execute.return_value = {'id': 'folder_id'}

    from backend.drive_sheets import get_or_create_folder
    folder_id = get_or_create_folder('Test Folder', mock_drive_service)
    assert folder_id == 'folder_id'
