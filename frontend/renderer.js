// frontend/renderer.js

const API = 'http://localhost:8000'

let selectedFiles = []
let analysisData  = null
let graphData     = null


// ─────────────────────────────────────────────
// Выбор файлов
// ─────────────────────────────────────────────

document.getElementById('file-input').addEventListener('change', (e) => {
  addFiles(Array.from(e.target.files))
})

// Drag & Drop
const dropZone = document.getElementById('drop-zone')

dropZone.addEventListener('dragover', (e) => {
  e.preventDefault()
  dropZone.classList.add('active')
})

dropZone.addEventListener('dragleave', () => {
  dropZone.classList.remove('active')
})

dropZone.addEventListener('drop', (e) => {
  e.preventDefault()
  dropZone.classList.remove('active')
  addFiles(Array.from(e.dataTransfer.files))
})

function addFiles(files) {
  const allowed = ['.pdf', '.docx', '.xlsx']
  files.forEach(file => {
    const ext = '.' + file.name.split('.').pop().toLowerCase()
    if (!allowed.includes(ext)) {
      showToast(`⚠️ Файл ${file.name} не поддерживается`, 'error')
      return
    }
    if (!selectedFiles.find(f => f.name === file.name)) {
      selectedFiles.push(file)
    }
  })
  renderFileList()
}

function renderFileList() {
  const list    = document.getElementById('file-list')
  const section = document.getElementById('file-list-section')
  const counter = document.getElementById('file-count')
  const btn     = document.getElementById('btn-analyze')

  if (selectedFiles.length === 0) {
    section.style.display = 'none'
    btn.disabled = true
    return
  }

  section.style.display = 'block'
  counter.textContent = selectedFiles.length
  btn.disabled = false

  const icons = { pdf: '📄', docx: '📝', xlsx: '📊' }

  list.innerHTML = selectedFiles.map((file, i) => {
    const ext = file.name.split('.').pop().toLowerCase()
    return `
      <div class="file-item">
        <span class="file-icon">${icons[ext] || '📁'}</span>
        <span class="file-name">${file.name}</span>
        <span class="file-remove" onclick="removeFile(${i})">✕</span>
      </div>
    `
  }).join('')
}

function removeFile(index) {
  selectedFiles.splice(index, 1)
  renderFileList()
}

function clearAll() {
  selectedFiles = []
  analysisData  = null
  graphData     = null
  renderFileList()
  resetResults()
  document.getElementById('file-input').value = ''
}


// ─────────────────────────────────────────────
// Запуск анализа
// ─────────────────────────────────────────────

async function runAnalyze() {
  if (selectedFiles.length === 0) return

  const btn = document.getElementById('btn-analyze')
  btn.disabled = true
  btn.textContent = '⏳ Анализ...'
  setProgress(10)

  try {
    // 1. Загружаем файлы
    const formData = new FormData()
    selectedFiles.forEach(f => formData.append('files', f))

    setProgress(30)
    const uploadRes = await fetch(`${API}/upload`, {
      method: 'POST',
      body: formData
    })

    if (!uploadRes.ok) throw new Error(`Upload failed: ${uploadRes.status}`)
    const uploadData = await uploadRes.json()
    setProgress(50)

    // 2. Формируем данные для /analyze
    const documents = uploadData.files.map(f => ({
      filename:   f.original_name,
      department: 'Основной отдел',   // TODO: выбор отдела в UI
      contractor: 'Не указан',
      items:      f.items || []
    }))

    // 3. Запускаем анализ
    const analyzeRes = await fetch(`${API}/analyze`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(documents)
    })

    if (!analyzeRes.ok) throw new Error(`Analyze failed: ${analyzeRes.status}`)
    const analyzeData = await analyzeRes.json()
    setProgress(75)

    // 4. Получаем результаты
    const [resultsRes, graphRes] = await Promise.all([
      fetch(`${API}/results`),
      fetch(`${API}/graph`)
    ])

    analysisData = await resultsRes.json()
    graphData    = await graphRes.json()
    setProgress(100)

    // 5. Отображаем
    renderResults(analysisData.results)
    renderGraph(graphData)
    renderRaw({ analyze: analyzeData, results: analysisData, graph: graphData })

    showToast('✅ Анализ завершён', 'success')

  } catch (err) {
    showToast(`❌ Ошибка: ${err.message}`, 'error')
    console.error(err)
  } finally {
    btn.disabled = false
    btn.textContent = '⚡ Запустить анализ'
    setTimeout(() => setProgress(0), 800)
  }
}


// ─────────────────────────────────────────────
// Отображение результатов
// ─────────────────────────────────────────────

function renderResults(results) {
  if (!results || results.length === 0) return

  const body  = document.getElementById('results-body')
  const table = document.getElementById('results-table')
  const empty = document.getElementById('empty-results')

  const scoreColors = {
    LOW:      '#3dd68c',
    MEDIUM:   '#f7a24f',
    HIGH:     '#f75a5a',
    CRITICAL: '#b45af7',
  }

  body.innerHTML = results.map(r => {
    const color = scoreColors[r.risk_level] || '#6b7280'
    return `
      <tr>
        <td><strong>${r.name}</strong></td>
        <td>
          <div class="score-bar">
            <div class="score-track">
              <div class="score-fill" style="width:${r.score}%; background:${color}"></div>
            </div>
            <span class="score-num" style="color:${color}">${r.score}</span>
          </div>
        </td>
        <td><span class="badge badge-${r.risk_level}">${r.risk_level}</span></td>
        <td><div class="explanation">${r.explanation || '—'}</div></td>
      </tr>
    `
  }).join('')

  empty.style.display = 'none'
  table.style.display = 'table'
}

function renderGraph(data) {
  const output = document.getElementById('graph-output')
  const empty  = document.getElementById('empty-graph')
  output.textContent = JSON.stringify(data, null, 2)
  empty.style.display  = 'none'
  output.style.display = 'block'
}

function renderRaw(data) {
  const output = document.getElementById('raw-output')
  const empty  = document.getElementById('empty-raw')
  output.textContent = JSON.stringify(data, null, 2)
  empty.style.display  = 'none'
  output.style.display = 'block'
}

function resetResults() {
  document.getElementById('results-body').innerHTML = ''
  document.getElementById('results-table').style.display = 'none'
  document.getElementById('empty-results').style.display = 'flex'
  document.getElementById('graph-output').style.display  = 'none'
  document.getElementById('empty-graph').style.display   = 'flex'
  document.getElementById('raw-output').style.display    = 'none'
  document.getElementById('empty-raw').style.display     = 'flex'
}


// ─────────────────────────────────────────────
// Вкладки
// ─────────────────────────────────────────────

function switchTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'))
  el.classList.add('active')
  document.getElementById('tab-' + name).classList.add('active')
}


// ─────────────────────────────────────────────
// Прогресс
// ─────────────────────────────────────────────

function setProgress(pct) {
  const bar  = document.getElementById('progress-bar')
  const fill = document.getElementById('progress-fill')
  if (pct === 0) {
    bar.classList.remove('active')
    fill.style.width = '0%'
  } else {
    bar.classList.add('active')
    fill.style.width = pct + '%'
  }
}


// ─────────────────────────────────────────────
// Уведомления
// ─────────────────────────────────────────────

let toastTimer = null

function showToast(msg, type = '') {
  const toast = document.getElementById('toast')
  toast.textContent = msg
  toast.className = `toast ${type} show`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => toast.classList.remove('show'), 3500)
}