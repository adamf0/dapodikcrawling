let currentJobId = null;
let eventSource = null;
let statusInterval = null;
let explorerOffset = 0;
const explorerLimit = 50;
let lastProcessedSekolahs = 0;
// DOM Elements
const systemStatusIndicator = document.getElementById('system-status-indicator');
const systemStatusText = document.getElementById('system-status-text');

const statProvinces = document.getElementById('stat-provinces');
const statKabupatens = document.getElementById('stat-kabupatens');
const statKecamatans = document.getElementById('stat-kecamatans');
const statSekolahs = document.getElementById('stat-sekolahs');
const statMadrasahs = document.getElementById('stat-madrasahs');

// const inputTargetProvs = document.getElementById('target-prov-ids');
const inputCrawlStep = document.getElementById('crawl-step');
const inputSemesterId = document.getElementById('semester-id');
const inputConcurrency = document.getElementById('concurrency');
const inputDelay = document.getElementById('delay');
const inputForceRecrawl = document.getElementById('force-recrawl');
const btnStart = document.getElementById('btn-start');
const btnCancel = document.getElementById('btn-cancel');
const btnTruncate = document.getElementById('btn-truncate');

const terminal = document.getElementById('terminal');
const progressWrapper = document.getElementById('job-progress-wrapper');
const progressStepText = document.getElementById('progress-step-text');
const progressPercentage = document.getElementById('progress-percentage');
const progressBar = document.getElementById('progress-bar');
const progressDetailText = document.getElementById('progress-detail-text');

// Explorer DOM
const filterProvince = document.getElementById('filter-province');
const filterKabupaten = document.getElementById('filter-kabupaten');
const filterKecamatan = document.getElementById('filter-kecamatan');
const filterBentuk = document.getElementById('filter-bentuk');
const filterStatus = document.getElementById('filter-status');
const filterSearch = document.getElementById('filter-search');
const schoolTableBody = document.getElementById('school-table-body');
const paginationInfo = document.getElementById('pagination-info');
const btnPrev = document.getElementById('pagination-prev');
const btnNext = document.getElementById('pagination-next');
const btnExport = document.getElementById('btn-export');

document.addEventListener('DOMContentLoaded', () => {
    fetchStats();
    loadProvinces();
    checkRunningJobs();
    setupEventListeners();
    fetchSchools(); // load initial page
});

function setupEventListeners() {
    btnStart.addEventListener('click', startCrawl);
    btnCancel.addEventListener('click', cancelCrawl);
    if (btnTruncate) btnTruncate.addEventListener('click', truncateDatabase);
    btnExport.addEventListener('click', exportToCSV);
    
    filterProvince.addEventListener('change', handleProvinceChange);
    filterKabupaten.addEventListener('change', handleKabupatenChange);
    filterKecamatan.addEventListener('change', () => { explorerOffset = 0; fetchSchools(); });
    filterBentuk.addEventListener('change', () => { explorerOffset = 0; fetchSchools(); });
    filterStatus.addEventListener('change', () => { explorerOffset = 0; fetchSchools(); });
    
    let searchTimeout = null;
    filterSearch.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            explorerOffset = 0;
            fetchSchools();
        }, 400);
    });

    btnPrev.addEventListener('click', () => {
        if (explorerOffset >= explorerLimit) {
            explorerOffset -= explorerLimit;
            fetchSchools();
        }
    });

    btnNext.addEventListener('click', () => {
        explorerOffset += explorerLimit;
        fetchSchools();
    });
}

async function fetchStats() {
    try {
        const response = await fetch('/api/stats');
        const data = await response.json();
        
        statProvinces.textContent = data.totals.provinces.toLocaleString();
        statKabupatens.textContent = data.totals.kabupatens.toLocaleString();
        statKecamatans.textContent = data.totals.kecamatans.toLocaleString();
        statSekolahs.textContent = data.totals.sekolahs.toLocaleString();
        if (statMadrasahs && data.totals.madrasahs !== undefined) {
            statMadrasahs.textContent = data.totals.madrasahs.toLocaleString();
        }
    } catch (err) {
        console.error('Failed to fetch stats:', err);
    }
}

async function checkRunningJobs() {
    try {
        const response = await fetch('/api/crawl/jobs');
        const jobs = await response.json();
        const activeJob = jobs.find(j => j.status === 'running' || j.status === 'pending');
        
        if (activeJob) {
            connectToJob(activeJob.id);
        } else {
            setSystemStatus('idle');
        }
    } catch (err) {
        console.error('Failed to check running jobs:', err);
        setSystemStatus('idle');
    }
}

function connectToJob(jobId) {
    currentJobId = jobId;
    setSystemStatus('running');
    
    btnStart.disabled = true;
    btnCancel.style.display = 'block';
    progressWrapper.style.display = 'block';
    
    lastProcessedSekolahs = 0;
    startLogStream(jobId);
    
    if (statusInterval) clearInterval(statusInterval);
    statusInterval = setInterval(() => pollJobStatus(jobId), 1000);
}

function setSystemStatus(status) {
    systemStatusIndicator.className = 'status-indicator ' + status;
    systemStatusText.textContent = status.charAt(0).toUpperCase() + status.slice(1);
}

async function startCrawl() {
    const checkedBentuk = Array.from(document.querySelectorAll('.bentuk-cb:checked')).map(cb => cb.value);
    
    if (checkedBentuk.length === 0) {
        alert('Please select at least one Bentuk Pendidikan (SD/SMP/SMA).');
        return;
    }

    const stepValue = inputCrawlStep ? inputCrawlStep.value : 'all';
    
    const payload = {
        step: stepValue,
        bentuk_pendidikan_list: checkedBentuk,
        semester_id: inputSemesterId.value.trim(),
        concurrency_limit: parseInt(inputConcurrency.value) || 3,
        delay: parseFloat(inputDelay.value) || 0.5,
        force_recrawl: inputForceRecrawl.checked
    };

    appendConsoleLine(`Initiating crawl request for stage: ${stepValue}...`, 'system');
    btnStart.disabled = true;

    try {
        const response = await fetch('/api/crawl/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || 'Failed to start crawl job.');
        }

        const res = await response.json();
        appendConsoleLine(`Crawl job started for stage '${stepValue}'. Job ID: ${res.job_id}`, 'system');
        connectToJob(res.job_id);
    } catch (err) {
        appendConsoleLine(`Error starting job: ${err.message}`, 'error');
        btnStart.disabled = false;
    }
}

async function truncateDatabase() {
    if (!confirm('Are you sure you want to TRUNCATE all data in the database? This cannot be undone!')) {
        return;
    }
    try {
        const response = await fetch('/api/db/truncate', { method: 'POST' });
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || 'Failed to truncate database.');
        }
        appendConsoleLine('Database successfully truncated!', 'system');
        fetchStats();
        loadProvinces();
        fetchSchools();
        alert('All database tables truncated successfully!');
    } catch (err) {
        alert('Truncate failed: ' + err.message);
        appendConsoleLine('Truncate error: ' + err.message, 'error');
    }
}

async function cancelCrawl() {
    if (!currentJobId) return;
    
    appendConsoleLine('Sending cancellation command...', 'system');
    btnCancel.disabled = true;
    
    try {
        const response = await fetch(`/api/crawl/cancel/${currentJobId}`, { method: 'POST' });
        if (!response.ok) {
            const err = await response.json();
            throw new Error(err.detail || 'Failed to cancel job.');
        }
        appendConsoleLine('Cancellation command processed.', 'system');
    } catch (err) {
        appendConsoleLine(`Cancellation error: ${err.message}`, 'error');
    } finally {
        btnCancel.disabled = false;
    }
}

function startLogStream(jobId) {
    if (eventSource) eventSource.close();
    
    eventSource = new EventSource(`/api/crawl/stream/${jobId}`);
    terminal.innerHTML = '';
    
    eventSource.onmessage = (event) => {
        const lineText = event.data;
        
        if (lineText.startsWith('[SYSTEM] Job finished') || lineText.startsWith('[EOF]')) {
            appendConsoleLine(lineText, 'system');
            handleJobFinished();
            return;
        }
        
        if (lineText.includes('[RATE-LIMIT]')) {
            appendConsoleLine(lineText, 'warning');
        } else if (lineText.includes('failed') || lineText.includes('Error')) {
            appendConsoleLine(lineText, 'error');
        } else {
            appendConsoleLine(lineText);
        }
    };
    
    eventSource.onerror = (err) => {
        console.error('SSE Connection Error:', err);
        eventSource.close();
    };
}

function appendConsoleLine(text, type = '') {
    const div = document.createElement('div');
    div.className = 'console-line ' + type;
    div.textContent = text;
    terminal.appendChild(div);
    terminal.scrollTop = terminal.scrollHeight;
}

async function pollJobStatus(jobId) {
    try {
        const response = await fetch(`/api/crawl/status/${jobId}`);
        if (!response.ok) return;
        const job = await response.json();
        
        updateProgressUI(job);
        fetchStats(); // Update stats cards in real-time (Provinces, Kabupatens, Kecamatans, Stored counts)
        
        if (job.status === 'completed' || job.status === 'failed' || job.status === 'cancelled') {
            handleJobFinished();
        }
    } catch (err) {
        console.error('Error polling status:', err);
    }
}

function updateProgressUI(job) {
    const step = job.current_step;
    progressStepText.textContent = `Current Step: ${step.toUpperCase()}`;
    
    let pct = 0;
    let detail = '';
    
    if (step === 'provinces') {
        const total = job.provinces.total || 1;
        const processed = job.provinces.processed;
        pct = Math.round((processed / total) * 100);
        detail = `Fetched ${processed} of ${total} targeted provinces`;
    } else if (step === 'chaining') {
        // Calculate dynamic progress
        const totalSec = job.sekolahs.total || 1;
        const processedSec = job.sekolahs.processed;
        pct = Math.round((processedSec / totalSec) * 100);
        detail = `Processed ${processedSec.toLocaleString()} of ${totalSec.toLocaleString()} districts`;
        
        // Refresh school grid if a new district completes crawling
        if (processedSec !== lastProcessedSekolahs) {
            lastProcessedSekolahs = processedSec;
            fetchSchools();
        }
    } else {
        pct = job.status === 'completed' ? 100 : 0;
        detail = `Status: ${job.status.toUpperCase()}`;
    }
    
    progressPercentage.textContent = `${pct}%`;
    progressBar.style.width = `${pct}%`;
    progressDetailText.textContent = detail;
}

function handleJobFinished() {
    if (statusInterval) {
        clearInterval(statusInterval);
        statusInterval = null;
    }
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
    
    setSystemStatus('idle');
    btnStart.disabled = false;
    btnCancel.style.display = 'none';
    currentJobId = null;
    
    fetchStats();
    loadProvinces();
    fetchSchools();
}

// ----------------------------------------------------
// DATABASE EXPLORER FUNCTIONS
// ----------------------------------------------------

async function loadProvinces() {
    try {
        const response = await fetch('/api/provinces');
        const list = await response.json();
        
        const prevVal = filterProvince.value;
        
        filterProvince.innerHTML = '<option value="">-- Select Province --</option>';
        list.forEach(p => {
            const opt = document.createElement('option');
            opt.value = p.kode_wilayah;
            opt.textContent = p.nama;
            filterProvince.appendChild(opt);
        });
        
        if (prevVal) filterProvince.value = prevVal;
    } catch (err) {
        console.error('Failed to load provinces list:', err);
    }
}

async function handleProvinceChange() {
    explorerOffset = 0;
    const provId = filterProvince.value;
    
    if (!provId) {
        filterKabupaten.innerHTML = '<option value="">-- Select Regency --</option>';
        filterKabupaten.disabled = true;
        filterKecamatan.innerHTML = '<option value="">-- Select District --</option>';
        filterKecamatan.disabled = true;
        fetchSchools();
        return;
    }
    
    filterKabupaten.innerHTML = '<option value="">Loading...</option>';
    filterKabupaten.disabled = true;
    
    try {
        const response = await fetch(`/api/kabupatens?provinsi_id=${provId}`);
        const list = await response.json();
        
        filterKabupaten.innerHTML = '<option value="">-- Select Regency --</option>';
        list.forEach(k => {
            const opt = document.createElement('option');
            opt.value = k.kode_wilayah;
            opt.textContent = k.nama;
            filterKabupaten.appendChild(opt);
        });
        filterKabupaten.disabled = false;
        
        filterKecamatan.innerHTML = '<option value="">-- Select District --</option>';
        filterKecamatan.disabled = true;
        
        fetchSchools();
    } catch (err) {
        console.error('Failed to load regencies:', err);
    }
}

async function handleKabupatenChange() {
    explorerOffset = 0;
    const kabId = filterKabupaten.value;
    
    if (!kabId) {
        filterKecamatan.innerHTML = '<option value="">-- Select District --</option>';
        filterKecamatan.disabled = true;
        fetchSchools();
        return;
    }
    
    filterKecamatan.innerHTML = '<option value="">Loading...</option>';
    filterKecamatan.disabled = true;
    
    try {
        const response = await fetch(`/api/kecamatans?kabupaten_id=${kabId}`);
        const list = await response.json();
        
        filterKecamatan.innerHTML = '<option value="">-- Select District --</option>';
        list.forEach(kc => {
            const opt = document.createElement('option');
            opt.value = kc.kode_wilayah;
            opt.textContent = kc.nama;
            filterKecamatan.appendChild(opt);
        });
        filterKecamatan.disabled = false;
        
        fetchSchools();
    } catch (err) {
        console.error('Failed to load districts:', err);
    }
}

async function fetchSchools() {
    const kecId = filterKecamatan.value;
    const bentuk = filterBentuk.value;
    const status = filterStatus.value;
    const search = filterSearch.value.trim();
    
    let query = `/api/sekolahs?limit=${explorerLimit}&offset=${explorerOffset}`;
    if (kecId) query += `&kecamatan_id=${kecId}`;
    if (bentuk) query += `&bentuk_pendidikan=${bentuk}`;
    if (status) query += `&status_sekolah=${status}`;
    if (search) query += `&search=${encodeURIComponent(search)}`;
    
    schoolTableBody.innerHTML = '<tr><td colspan="17" style="text-align: center; color: var(--text-muted); padding: 2rem;">Loading data from SQLite...</td></tr>';
    
    try {
        const response = await fetch(query);
        const data = await response.json();
        
        renderSchoolsTable(data.items);
        updatePaginationUI(data.total);
    } catch (err) {
        schoolTableBody.innerHTML = '<tr><td colspan="17" style="text-align: center; color: var(--danger); padding: 2rem;">Error loading data.</td></tr>';
        console.error('Failed to fetch schools:', err);
    }
}

function renderSchoolsTable(schools) {
    if (schools.length === 0) {
        schoolTableBody.innerHTML = '<tr><td colspan="17" style="text-align: center; color: var(--text-muted); padding: 3rem;">No schools stored. Start crawling first!</td></tr>';
        return;
    }
    
    schoolTableBody.innerHTML = '';
    schools.forEach(s => {
        const tr = document.createElement('tr');
        const badgeStatusClass = s.status_sekolah.toLowerCase() === 'negeri' ? 'badge-status-negeri' : 'badge-status-swasta';
        
        tr.innerHTML = `
            <td style="font-family: 'JetBrains Mono', monospace;">${s.npsn || '-'}</td>
            <td style="font-weight: 500;">${s.nama}</td>
            <td style="text-align: center;"><span class="badge" style="background: rgba(255, 242, 200, 0.1); color: var(--warning);">${s.akreditasi || '-'}</span></td>
            <td>${s.kepsek || '-'}</td>
            <td>${s.operator || '-'}</td>
            <td><span class="badge badge-bentuk">${s.bentuk_pendidikan}</span></td>
            <td><span class="badge ${badgeStatusClass}">${s.status_sekolah}</span></td>
            <td>${s.desa_kelurahan || '-'}</td>
            <td>${s.kecamatan_scraped || s.kecamatan_id}</td>
            <td>${s.kabupaten_scraped || '-'}</td>
            <td>${s.provinsi_scraped || '-'}</td>
            <td>${s.kode_pos || '-'}</td>
            <td>${s.kurikulum || '-'}</td>
            <td>${s.ptk.toLocaleString()}</td>
            <td>${s.pd.toLocaleString()}</td>
            <td>${s.rombel.toLocaleString()}</td>
            <td style="font-size: 0.85rem; color: var(--text-muted);">${s.sinkron_terakhir || '-'}</td>
        `;
        schoolTableBody.appendChild(tr);
    });
}

function updatePaginationUI(total) {
    const end = Math.min(explorerOffset + explorerLimit, total);
    const start = total === 0 ? 0 : explorerOffset + 1;
    
    paginationInfo.textContent = `Showing ${start.toLocaleString()} - ${end.toLocaleString()} of ${total.toLocaleString()} schools`;
    
    btnPrev.disabled = explorerOffset === 0;
    btnNext.disabled = end >= total;
}

function exportToCSV() {
    window.open('/api/export', '_blank');
}
