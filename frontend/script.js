document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const step1 = document.getElementById('step-1');
    const step2 = document.getElementById('step-2');
    const step3 = document.getElementById('step-3');
    const progressSection = document.getElementById('progress-section');
    const resultsSection = document.getElementById('results-section');
    const errorSection = document.getElementById('error-section');

    const selectExcelBtn = document.getElementById('select-excel-btn');
    const selectSheetsBtn = document.getElementById('select-sheets-btn');
    const selectPdfFolderBtn = document.getElementById('select-pdf-folder-btn');
    const startProcessingBtn = document.getElementById('start-processing-btn');
    const processAgainBtn = document.getElementById('process-again-btn');

    const selectedExcelFileDiv = document.getElementById('selected-excel-file');
    const selectedPdfFolderDiv = document.getElementById('selected-pdf-folder');
    const progressBar = document.getElementById('progress-bar');
    const progressMessage = document.getElementById('progress-message');
    const resultsDiv = document.getElementById('results');
    const errorMessagesDiv = document.getElementById('error-messages');

    let selectedExcelFile = null;
    let selectedSheetsFileId = null;
    let selectedPdfFiles = [];
    let pdfFilesData = [];

    // Load Google APIs
    let pickerApiLoaded = false;
    let oauthToken;

    function onApiLoad() {
        gapi.load('picker', {'callback': onPickerApiLoad});
    }

    function onPickerApiLoad() {
        pickerApiLoaded = true;
    }

    function onAuthApiLoad() {
        window.gapi.auth.authorize(
            {
                'client_id': 'YOUR_CLIENT_ID',
                'scope': ['https://www.googleapis.com/auth/drive.readonly'],
                'immediate': false
            },
            handleAuthResult);
    }

    function handleAuthResult(authResult) {
        if (authResult && !authResult.error) {
            oauthToken = authResult.access_token;
            createPicker();
        }
    }

    function createPicker() {
        if (pickerApiLoaded && oauthToken) {
            const picker = new google.picker.PickerBuilder()
                .addView(google.picker.ViewId.SPREADSHEETS)
                .setOAuthToken(oauthToken)
                .setDeveloperKey('YOUR_DEVELOPER_KEY')
                .setCallback(pickerCallback)
                .build();
            picker.setVisible(true);
        }
    }

    function pickerCallback(data) {
        if (data.action === google.picker.Action.PICKED) {
            const file = data.docs[0];
            selectedSheetsFileId = file.id;
            selectedExcelFileDiv.textContent = `Archivo seleccionado: ${file.name}`;
            selectedExcelFileDiv.classList.remove('hidden');
            step2.classList.remove('hidden');
        }
    }

    // Event Listeners
    selectExcelBtn.addEventListener('click', () => {
        const fileInput = document.createElement('input');
        fileInput.type = 'file';
        fileInput.accept = '.xls,.xlsx';
        fileInput.onchange = (event) => {
            const file = event.target.files[0];
            if (file) {
                selectedExcelFile = file;
                selectedExcelFileDiv.textContent = `Archivo seleccionado: ${file.name}`;
                selectedExcelFileDiv.classList.remove('hidden');
                step2.classList.remove('hidden');
            }
        };
        fileInput.click();
    });

    selectSheetsBtn.addEventListener('click', () => {
        gapi.load('auth', {'callback': onAuthApiLoad});
    });

    selectPdfFolderBtn.addEventListener('click', async () => {
        if ('showOpenFilePicker' in window) {
            try {
                const handles = await window.showOpenFilePicker({
                    multiple: true,
                    types: [{
                        description: 'PDF Files',
                        accept: {
                            'application/pdf': ['.pdf']
                        }
                    }]
                });
                selectedPdfFiles = handles;
                selectedPdfFolderDiv.textContent = `Archivos seleccionados: ${handles.length} PDFs`;
                selectedPdfFolderDiv.classList.remove('hidden');
                step3.classList.remove('hidden');

                // Read files into data
                pdfFilesData = [];
                for (const handle of handles) {
                    const file = await handle.getFile();
                    pdfFilesData.push(file);
                }

            } catch (error) {
                console.error('Error selecting files:', error);
            }
        } else {
            alert('Tu navegador no soporta esta funcionalidad. Por favor, usa Chrome o Edge.');
        }
    });

    startProcessingBtn.addEventListener('click', async () => {
        if ((!selectedExcelFile && !selectedSheetsFileId) || pdfFilesData.length === 0) {
            alert('Por favor, selecciona el archivo Excel/Google Sheets y los PDFs.');
            return;
        }

        step1.classList.add('hidden');
        step2.classList.add('hidden');
        step3.classList.add('hidden');
        progressSection.classList.remove('hidden');

        // Prepare data to send to backend
        const formData = new FormData();
        if (selectedExcelFile) {
            formData.append('excelFile', selectedExcelFile);
        } else {
            formData.append('sheetsFileId', selectedSheetsFileId);
        }

        pdfFilesData.forEach((file) => {
            formData.append('pdfFiles', file);
        });

        // Send data to backend
        try {
            const response = await fetch('/api/process-pdfs', {
                method: 'POST',
                body: formData,
            });
            const result = await response.json();

            progressSection.classList.add('hidden');
            if (result.status === 'success') {
                resultsSection.classList.remove('hidden');
                resultsDiv.innerHTML = `<p>Procesamiento completado. Archivos guardados en Google Drive.</p>`;
                document.getElementById('download-excel-btn').classList.remove('hidden');
                document.getElementById('view-drive-btn').classList.remove('hidden');
            } else {
                errorSection.classList.remove('hidden');
                errorMessagesDiv.textContent = result.message || 'Error durante el procesamiento.';
            }
        } catch (error) {
            console.error('Error processing PDFs:', error);
            progressSection.classList.add('hidden');
            errorSection.classList.remove('hidden');
            errorMessagesDiv.textContent = 'Error durante el procesamiento.';
        }
    });

    processAgainBtn.addEventListener('click', () => {
        // Reset UI
        selectedExcelFile = null;
        selectedSheetsFileId = null;
        pdfFilesData = [];
        selectedExcelFileDiv.textContent = '';
        selectedPdfFolderDiv.textContent = '';
        selectedExcelFileDiv.classList.add('hidden');
        selectedPdfFolderDiv.classList.add('hidden');
        resultsSection.classList.add('hidden');
        errorSection.classList.add('hidden');
        step1.classList.remove('hidden');
    });
});
