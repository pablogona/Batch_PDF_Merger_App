document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const step1 = document.getElementById('step-1');
    const step2 = document.getElementById('step-2');
    const step3 = document.getElementById('step-3');
    const progressSection = document.getElementById('progress-section');
    const resultsSection = document.getElementById('results-section');

    const selectExcelBtn = document.getElementById('select-excel-btn');
    const selectSheetsBtn = document.getElementById('select-sheets-btn');
    const selectPdfFilesBtn = document.getElementById('select-pdf-files-btn');
    const selectFolderBtn = document.getElementById('select-folder-btn');
    const startProcessingBtn = document.getElementById('start-processing-btn');
    const processAgainBtn = document.getElementById('process-again-btn');

    const selectedExcelFileDiv = document.getElementById('selected-excel-file');
    const selectedPdfFolderDiv = document.getElementById('selected-pdf-folder');
    const progressBar = document.getElementById('progress-bar');
    const resultsDiv = document.getElementById('results');

    let selectedExcelFile = null;
    let selectedSheetsFileId = null;
    let selectedPdfFiles = [];
    let pdfFilesData = [];

    // Load Google APIs
    let pickerApiLoaded = false;
    let oauthToken;

    function onApiLoad() {
        gapi.load('picker', { 'callback': onPickerApiLoad });
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
            handleAuthResult
        );
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

    // Start polling the server for progress updates
    function startProgressPolling(taskId) {
        const interval = 1000; // milliseconds
        const progressInterval = setInterval(async () => {
            try {
                const response = await fetch(`/api/progress/${taskId}`);
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                const data = await response.json();
                const progress = data.progress;
                progressBar.style.width = progress + '%';

                if (data.status === 'completed') {
                    clearInterval(progressInterval);
                    progressSection.classList.add('hidden');
                    resultsSection.classList.remove('hidden');

                    if (data.result.status === 'success') {
                        let message = `<p>${data.result.message}</p>`;

                        if (data.result.errors && data.result.errors.length > 0) {
                            const errorCount = data.result.errors.length;
                            if (errorCount <= 10) {
                                message += `<p>Algunos PDFs no pudieron ser procesados:</p><ul>`;
                                data.result.errors.forEach(error => {
                                    message += `<li>${error.file_name}: ${error.message}</li>`;
                                });
                                message += `</ul>`;
                            } else {
                                message += `<p>Advertencia: ${errorCount} PDFs no pudieron ser procesados y fueron guardados en la carpeta 'PDFs con Error'.</p>`;
                            }
                        }

                        resultsDiv.innerHTML = message;
                        document.getElementById('download-excel-btn').classList.remove('hidden');
                        document.getElementById('view-drive-btn').classList.remove('hidden');
                    } else {
                        resultsDiv.innerHTML = `<p>Error crítico durante el procesamiento: ${data.result.message}</p>`;
                        document.getElementById('download-excel-btn').classList.add('hidden');
                        document.getElementById('view-drive-btn').classList.add('hidden');
                    }

                    // Always show "Procesar Nuevamente" button
                    processAgainBtn.classList.remove('hidden');
                }
            } catch (error) {
                clearInterval(progressInterval);
                console.error('Error fetching progress:', error);
                progressSection.classList.add('hidden');
                resultsSection.classList.remove('hidden');
                resultsDiv.innerHTML = `<p>Error crítico durante el procesamiento: ${error.message || 'Error desconocido'}</p>`;
                document.getElementById('download-excel-btn').classList.add('hidden');
                document.getElementById('view-drive-btn').classList.add('hidden');

                // Show only "Procesar Nuevamente" button
                processAgainBtn.classList.remove('hidden');
            }
        }, interval);
    }
    
    
    async function showResults(taskId) {
        try {
            const response = await fetch(`/api/task-result/${taskId}`);
            const result = await response.json();
            if (result.status === 'success') {
                resultsDiv.innerHTML = `<p>${result.message}</p>`;
            } else {
                resultsDiv.innerHTML = `<p>Error: ${result.message}</p>`;
            }
        } catch (error) {
            resultsDiv.innerHTML = `<p>Error fetching results: ${error.message}</p>`;
        }
    }
    
    

    // Event for selecting Excel files
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

    // Event for selecting Google Sheets
    selectSheetsBtn.addEventListener('click', () => {
        gapi.load('auth', { 'callback': onAuthApiLoad });
    });

    // Event to select PDF files
    selectPdfFilesBtn.addEventListener('click', async () => {
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

    // Event to select folder
    selectFolderBtn.addEventListener('click', async () => {
        if ('showDirectoryPicker' in window) {
            try {
                const folderHandle = await window.showDirectoryPicker();
                selectedPdfFolderDiv.textContent = `Carpeta seleccionada: ${folderHandle.name}`;
                selectedPdfFolderDiv.classList.remove('hidden');
                step3.classList.remove('hidden');

                // Read files from the folder
                pdfFilesData = [];
                for await (const [name, handle] of folderHandle.entries()) {
                    if (handle.kind === 'file' && handle.name.endsWith('.pdf')) {
                        const file = await handle.getFile();
                        pdfFilesData.push(file);
                    }
                }

                // Update the UI with the number of PDFs found
                selectedPdfFolderDiv.textContent = `Carpeta seleccionada: ${folderHandle.name} (${pdfFilesData.length} PDFs encontrados)`;
            } catch (error) {
                console.error('Error selecting folder:', error);
            }
        } else {
            alert('Tu navegador no soporta esta funcionalidad. Por favor, usa Chrome o Edge.');
        }
    });

    // Start processing event
    startProcessingBtn.addEventListener('click', async () => {
        if ((!selectedExcelFile && !selectedSheetsFileId) || pdfFilesData.length === 0) {
            alert('Por favor, selecciona el archivo Excel/Google Sheets y los PDFs.');
            return;
        }

        step1.classList.add('hidden');
        step2.classList.add('hidden');
        step3.classList.add('hidden');
        progressSection.classList.remove('hidden');
        resultsSection.classList.add('hidden'); // Hide results section at the start

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

        // Send data to backend and start polling
        try {
            const response = await fetch('/api/process-pdfs', {
                method: 'POST',
                body: formData,
            });
            const result = await response.json();

            if (result.status === 'success') {
                startProgressPolling(result.task_id); // Start polling progress with task_id

                // Hide results section
                resultsSection.classList.add('hidden');
                resultsDiv.innerHTML = '';
            } else {
                progressSection.classList.add('hidden');
                resultsSection.classList.remove('hidden');
                resultsDiv.innerHTML = `<p>Error durante el procesamiento: ${result.message || 'Error desconocido.'}</p>`;
            }
        } catch (error) {
            console.error('Error processing PDFs:', error);
            progressSection.classList.add('hidden');
            resultsSection.classList.remove('hidden');
            resultsDiv.innerHTML = `<p>Hubo un error durante el procesamiento de los archivos. Inténtalo de nuevo.</p>`;
        }
    });

    // Reset the form and process again
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
        step1.classList.remove('hidden');
    });
});
