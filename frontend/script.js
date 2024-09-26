// script.js

document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const step1 = document.getElementById('step-1');
    const step2 = document.getElementById('step-2');
    const step3 = document.getElementById('step-3');
    const progressSection = document.getElementById('progress-section');
    const resultsSection = document.getElementById('results-section');

    const selectExcelBtn = document.getElementById('select-excel-btn');
    const selectDriveFolderBtn = document.getElementById('select-drive-folder-btn');  // Button for Google Drive folder selection
    const startProcessingBtn = document.getElementById('start-processing-btn');
    const processAgainBtn = document.getElementById('process-again-btn');

    const selectedExcelFileDiv = document.getElementById('selected-excel-file');
    const selectedPdfFolderDiv = document.getElementById('selected-pdf-folder');
    const progressBar = document.getElementById('progress-bar');
    const progressMessage = document.getElementById('progress-message'); // Added for progress messages
    const resultsDiv = document.getElementById('results');

    let selectedExcelFile = null;
    let selectedSheetsFileId = null;
    let selectedPdfFiles = [];
    let pdfFilesData = [];
    let selectedFolderId = null;  // For storing Google Drive folder ID

    // Google API Initialization Flags
    let gapiInited = false;
    let gisInited = false;
    let pickerInited = false;
    let tokenClient;
    let accessToken = '';

    // Function to initialize the Google API client
    async function initializeGapiClient() {
        await gapi.client.init({
            apiKey: 'AIzaSyDeAxY8kuxZUOZyHv7fE2j6T82p2YGo_ww', // Replace with your actual API key
            discoveryDocs: ['https://www.googleapis.com/discovery/v1/apis/drive/v3/rest'],
        });
        gapiInited = true;
        maybeEnableButtons();
    }

    // Function to initialize the Google Identity Services (GIS)
    function initializeGis() {
        tokenClient = google.accounts.oauth2.initTokenClient({
            client_id: '639342449120-dnbdrqic8g3spmu572oq6fcfqhr0ivqi.apps.googleusercontent.com', // Replace with your actual Client ID
            scope: 'https://www.googleapis.com/auth/drive',
            callback: (response) => {
                if (response.error !== undefined) {
                    console.error('Token Client Error:', response);
                    return;
                }
                accessToken = response.access_token;
                createPicker();
            },
        });
        gisInited = true;
        maybeEnableButtons();
    }

    // Function to enable buttons after GAPI and GIS are initialized
    function maybeEnableButtons() {
        if (gapiInited && gisInited) {
            selectDriveFolderBtn.disabled = false;
            // Enable other buttons if needed
        }
    }

    // Function to handle authentication button click
    function handleAuthClick() {
        tokenClient.requestAccessToken({ prompt: 'consent' });
    }

    // Function to create and display the Google Picker
    function createPicker() {
        if (accessToken) {
            const docsView = new google.picker.DocsView()
                .setIncludeFolders(true)
                .setSelectFolderEnabled(true)
                .setMimeTypes('application/vnd.google-apps.folder')
                .setParent('root'); // Optional: set the root as the starting folder

            const picker = new google.picker.PickerBuilder()
                .addView(docsView)
                .setOAuthToken(accessToken)
                .setDeveloperKey('AIzaSyDeAxY8kuxZUOZyHv7fE2j6T82p2YGo_ww') // Replace with your actual API key
                .setCallback(pickerCallback)
                .build();
            picker.setVisible(true);
        } else {
            console.error('Access token is missing.');
        }
    }

    // Callback function after folder selection in Picker
    function pickerCallback(data) {
        if (data.action === google.picker.Action.PICKED) {
            const folder = data.docs[0];
            selectedFolderId = folder.id;
            selectedPdfFolderDiv.textContent = `Carpeta seleccionada: ${folder.name}`;
            selectedPdfFolderDiv.classList.remove('hidden');
            step3.classList.remove('hidden');
        } else if (data.action === google.picker.Action.CANCEL) {
            console.log('Picker canceled');
        }
    }

    // Function to start polling the server for progress updates
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
    
                // Update progress message based on the progress value
                if (progress < 10) {
                    progressMessage.textContent = 'Inicializando...';
                } else if (progress < 20) {
                    progressMessage.textContent = 'Descargando PDFs...';
                } else if (progress < 60) {
                    progressMessage.textContent = 'Procesando PDFs...';
                } else if (progress < 100) {
                    progressMessage.textContent = 'Unificando PDFs...';
                } else {
                    progressMessage.textContent = 'Proceso completado.';
                }
    
                if (data.status === 'completed') {
                    clearInterval(progressInterval);
                    progressSection.classList.add('hidden');
                    resultsSection.classList.remove('hidden');
    
                    if (data.result && data.result.status === 'success') {
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
                    } else {
                        let errorMessage = (data.result && data.result.message) || 'Error desconocido';
                        resultsDiv.innerHTML = `<p>Error crítico durante el procesamiento: ${errorMessage}</p>`;
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
    
                // Show only "Procesar Nuevamente" button
                processAgainBtn.classList.remove('hidden');
            }
        }, interval);
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

    // Event to select Google Drive folder
    selectDriveFolderBtn.addEventListener('click', () => {
        if (!accessToken) {
            handleAuthClick();
        } else {
            createPicker();
        }
    });

    // Start processing event
    startProcessingBtn.addEventListener('click', async () => {
        if (!selectedExcelFile || !selectedFolderId) {
            alert('Por favor, selecciona el archivo Excel y una carpeta en Google Drive.');
            return;
        }

        step1.classList.add('hidden');
        step2.classList.add('hidden');
        step3.classList.add('hidden');
        progressSection.classList.remove('hidden');
        resultsSection.classList.add('hidden'); // Hide results section at the start

        // Prepare data to send to backend
        const formData = new FormData();
        formData.append('excelFile', selectedExcelFile);
        formData.append('folderId', selectedFolderId);  // Send Google Drive folder ID to backend

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
        selectedFolderId = null;  // Reset the Google Drive folder ID
        selectedExcelFileDiv.textContent = '';
        selectedPdfFolderDiv.textContent = '';
        selectedExcelFileDiv.classList.add('hidden');
        selectedPdfFolderDiv.classList.add('hidden');
        resultsSection.classList.add('hidden');
        step1.classList.remove('hidden');

        // Reset progress bar and message
        progressBar.style.width = '0%';
        progressMessage.textContent = '';
        processAgainBtn.classList.add('hidden');
    });

    // Initialize the Google API client and GIS
    window.addEventListener('load', () => {
        // Load the Google API client library
        gapiLoaded();
        // Initialize the Google Identity Services
        initializeGis();
    });

    // Function to load the Google API client
    function gapiLoaded() {
        gapi.load('client:picker', initializeGapiClient);
    }
});
