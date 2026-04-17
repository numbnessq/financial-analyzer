// frontend/renderer.js

const API = 'http://127.0.0.1:8000'
let selectedFiles = []
let cyInstance    = null
let allResults    = []
let sortCol       = 'score'
let sortDir       = -1
let filterRisk    = 'ALL'


// ─── Файлы ────────────────────────────────────

document.getElementById('file-input').addEventListener('change', e => addFiles(Array.from(e.target.files)))

const dropZone = document.getElementById('drop-zone')
dropZone.addEventListener('dragover',  e => { e.preventDefault(); dropZone.classList.add('active') })
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('active'))
dropZone.addEventListener('drop', e => {
  e.preventDefault(); dropZone.classList.remove('active')
  addFiles(Array.from(e.dataTransfer.files))
})

function addFiles(files) {
  const allowed = ['.pdf', '.docx', '.xlsx']
  files.forEach(file => {
    const ext = '.' + file.name.split('.').pop().toLowerCase()
    if (!allowed.includes(ext)) { showToast(`⚠ ${file.name} не поддерживается`, 'error'); return }
    if (!selectedFiles.find(f => f.name === file.name)) selectedFiles.push(file)
  })
  renderFileList()
}

function renderFileList() {
  const section = document.getElementById('file-list-section')
  const counter = document.getElementById('file-count')
  const btn     = document.getElementById('btn-analyze')
  const list    = document.getElementById('file-list')
  if (!selectedFiles.length) { section.style.display = 'none'; btn.disabled = true; return }
  section.style.display = 'block'
  counter.textContent   = selectedFiles.length
  btn.disabled          = false
  const icons = { pdf: '▤', docx: '▤', xlsx: '▦' }
  list.innerHTML = selectedFiles.map((f, i) => {
    const ext = f.name.split('.').pop().toLowerCase()
    return `<div class="file-item">
      <span class="file-icon">${icons[ext] || '▤'}</span>
      <span class="file-name">${f.name}</span>
      <span class="file-remove" onclick="removeFile(${i})">✕</span>
    </div>`
  }).join('')
}

function removeFile(i) { selectedFiles.splice(i, 1); renderFileList() }

function clearAll() {
  selectedFiles = []; allResults = []
  renderFileList(); resetResults()
  document.getElementById('file-input').value = ''
  document.getElementById('btn-report').disabled = true
  if (cyInstance) { cyInstance.destroy(); cyInstance = null }
}


// ─── Анализ ───────────────────────────────────

async function runAnalyze() {
  if (!selectedFiles.length) return
  const btn = document.getElementById('btn-analyze')
  btn.disabled = true; btn.textContent = '... ЗАГРУЗКА'
  setProgress(5)

  try {
    // 1. Загружаем файлы — быстро, без AI
    const formData = new FormData()
    selectedFiles.forEach(f => formData.append('files', f))

    const uploadRes = await fetch(`${API}/upload`, { method: 'POST', body: formData })
    if (!uploadRes.ok) throw new Error(`Upload: ${uploadRes.status}`)
    const uploadData = await uploadRes.json()
    setProgress(20)

    // 2. Передаём raw_text чтобы backend мог запустить AI
    const documents = (uploadData.files || []).map(f => ({
      filename:    f.original_name || '',
      department:  '',
      contractor:  '',
      source_file: f.original_name || '',
      raw_text:    f.raw_text || '',
      items:       f.items || [],
    }))

    // 3. Запускаем анализ — backend сразу возвращает job_id
    btn.textContent = '... AI АНАЛИЗ'
    const analyzeRes = await fetch(`${API}/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(documents)
    })
    if (!analyzeRes.ok) throw new Error(`Analyze: ${analyzeRes.status}`)
    const { job_id } = await analyzeRes.json()

    // 4. Опрашиваем статус задачи каждые 2 секунды
    await pollJob(job_id, btn)

  } catch (err) {
    showToast(`✕ ${err.message}`, 'error')
    console.error(err)
    btn.disabled = false; btn.textContent = '▶ ЗАПУСТИТЬ АНАЛИЗ'
    setProgress(0)
  }
}

async function pollJob(job_id, btn) {
  return new Promise((resolve, reject) => {
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${API}/job/${job_id}`)
        if (!res.ok) { clearInterval(interval); reject(new Error('Ошибка статуса задачи')); return }
        const job = await res.json()

        setProgress(job.progress || 0)
        if (btn) btn.textContent = `... ${job.message || 'ОБРАБОТКА'}`

        if (job.status === 'done') {
          clearInterval(interval)
          await loadResults()
          resolve()
        } else if (job.status === 'error') {
          clearInterval(interval)
          reject(new Error(job.message || 'Ошибка анализа'))
        }
      } catch (err) {
        clearInterval(interval)
        reject(err)
      }
    }, 2000)
  })
}

async function loadResults() {
  const btn = document.getElementById('btn-analyze')
  try {
    const [resultsRes, graphRes] = await Promise.all([
      fetch(`${API}/results`),
      fetch(`${API}/graph`)
    ])
    const resultsData = await resultsRes.json()
    const graphData   = await graphRes.json()

    allResults = resultsData.results || []
    showResultsContent()
    renderResults()
    renderGraph(graphData)
    renderRaw({ results: resultsData, graph: graphData })

    document.getElementById('btn-report').disabled = false

    const anomalies = allResults.filter(r => r.score >= 20).length
    showToast(`✓ Готово — ${allResults.length} позиций, аномалий: ${anomalies}`, 'success')
  } finally {
    btn.disabled = false; btn.textContent = '▶ ЗАПУСТИТЬ АНАЛИЗ'
    setTimeout(() => setProgress(0), 800)
  }
}


// ─── Скачать отчёт ────────────────────────────

async function downloadReport() {
  const btn = document.getElementById('btn-report')
  btn.disabled = true
  btn.textContent = '... ФОРМИРУЕТСЯ'
  try {
    // Backend сохраняет файл в ~/Downloads и возвращает путь
    const res = await fetch(`${API}/report/save`)
    if (!res.ok) {
      const err = await res.json().catch(() => ({}))
      throw new Error(err.detail || `Ошибка сервера: ${res.status}`)
    }
    const { path, filename } = await res.json()

    // Открываем файл если Tauri доступен
    if (window.__TAURI__?.shell?.open) {
      await window.__TAURI__.shell.open(path)
    }

    showToast(`✓ Отчёт сохранён в Downloads: ${filename}`, 'success')
  } catch (err) {
    showToast(`✕ ${err.message}`, 'error')
    console.error(err)
  } finally {
    btn.disabled = false
    btn.textContent = '↓ СКАЧАТЬ ОТЧЁТ .DOCX'
  }
}

// ─── Таблица ──────────────────────────────────

function showResultsContent() {
  document.getElementById('empty-results').style.display   = 'none'
  document.getElementById('results-content').style.display = 'flex'
}

function setFilter(risk) {
  filterRisk = risk
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'))
  document.querySelector(`[data-risk="${risk}"]`).classList.add('active')
  renderResults()
}

function sortBy(col) {
  if (sortCol === col) sortDir *= -1
  else { sortCol = col; sortDir = -1 }
  document.querySelectorAll('.th-sort').forEach(th => {
    th.classList.remove('asc', 'desc')
    if (th.dataset.col === col) th.classList.add(sortDir === -1 ? 'desc' : 'asc')
  })
  renderResults()
}

function renderResults() {
  if (!allResults.length) return
  const body   = document.getElementById('results-body')
  const RISK_O = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 }
  const COLORS = { LOW: '#2ea84a', MEDIUM: '#d4882a', HIGH: '#cc3333', CRITICAL: '#7c3aed' }

  let data = filterRisk === 'ALL' ? [...allResults] : allResults.filter(r => r.risk_level === filterRisk)

  data.sort((a, b) => {
    let av = a[sortCol], bv = b[sortCol]
    if (sortCol === 'risk_level') { av = RISK_O[av] ?? 9; bv = RISK_O[bv] ?? 9 }
    if (typeof av === 'string') return av.localeCompare(bv) * sortDir
    return (av < bv ? -1 : av > bv ? 1 : 0) * sortDir
  })

  body.innerHTML = data.map(r => {
    const color = COLORS[r.risk_level] || '#8b90a0'
    const name  = r.item || r.name || '—'
    const depts = (r.departments || []).join(' · ') || ''
    const cls   = r.risk_level === 'CRITICAL' ? 'row-critical' : r.risk_level === 'HIGH' ? 'row-high' : ''

    return `<tr class="${cls}">
      <td style="white-space:nowrap">
        <div class="score-bar">
          <div class="score-track"><div class="score-fill" style="width:${r.score}%;background:${color}"></div></div>
          <span class="score-num" style="color:${color}">${r.score}</span>
        </div>
      </td>
      <td>
        <div class="item-name">${name}</div>
        ${depts ? `<div class="item-depts">${depts}</div>` : ''}
      </td>
      <td style="white-space:nowrap"><span class="badge badge-${r.risk_level}">${r.risk_level}</span></td>
      <td><div class="explanation">${r.explanation || '—'}</div></td>
    </tr>`
  }).join('')
}


// ─── Граф ─────────────────────────────────────

function renderGraph(data) {
  const cyEl      = document.getElementById('cy')
  const emptyEl   = document.getElementById('empty-graph')
  const legendEl  = document.getElementById('graph-legend')
  const controlEl = document.getElementById('graph-controls')

  if (!data?.nodes?.length) return

  emptyEl.style.display   = 'none'
  cyEl.style.display      = 'block'
  legendEl.style.display  = 'flex'
  controlEl.style.display = 'flex'

  if (cyInstance) cyInstance.destroy()

  const sortO = { department: 0, item: 1, contractor: 2 }
  const sorted = [...data.nodes].sort((a, b) => (sortO[a.type] ?? 3) - (sortO[b.type] ?? 3))

  const elements = [
    ...sorted.map(n => ({ data: {
      id: n.id, label: n.label, type: n.type, color: n.color || '#3b4258',
      risk_score: n.risk_score, risk_level: n.risk_level,
      departments: n.departments || []
    }})),
    ...data.edges.map(e => ({ data: {
      id: `${e.source}__${e.target}`,
      source: e.source, target: e.target,
      etype: e.type || '', weight: e.weight || 1
    }}))
  ]

  cyInstance = cytoscape({
    container: cyEl, elements,
    style: [
      { selector: 'node', style: {
        'background-color': 'data(color)', 'label': 'data(label)',
        'color': '#8b90a0', 'font-size': '9px', 'font-family': 'JetBrains Mono, monospace',
        'font-weight': '500',
        'text-valign': 'bottom', 'text-halign': 'center', 'text-margin-y': 5,
        'text-wrap': 'wrap', 'text-max-width': '100px',
        'border-width': 1, 'border-color': 'rgba(255,255,255,0.08)',
      }},
      { selector: 'node[type="department"]', style: {
        shape: 'ellipse', width: 52, height: 52,
        'background-color': '#1a2a4a',
        'border-color': '#3b7de8', 'border-width': 1.5,
        'color': '#6a9ee8',
      }},
      { selector: 'node[type="item"]', style: {
        shape: 'round-rectangle', width: 54, height: 26,
        'border-color': 'data(color)', 'border-width': 1,
        'background-color': '#0f1117',
        'color': '#8b90a0',
      }},
      { selector: 'node[type="contractor"]', style: {
        shape: 'diamond', width: 44, height: 44,
        'background-color': '#0f2a22',
        'border-color': '#1a7a5e', 'border-width': 1.5,
        'color': '#3aaa80',
      }},
      { selector: 'node:selected', style: {
        'border-color': '#3b7de8', 'border-width': 2,
      }},
      { selector: 'edge', style: {
        width: 1,
        'line-color': '#1e2130',
        'target-arrow-color': '#1e2130',
        'target-arrow-shape': 'triangle',
        'arrow-scale': 0.7,
        'curve-style': 'bezier',
        opacity: 1,
      }},
      { selector: 'edge[etype="supplies"]', style: {
        'line-color': '#0f2a22', 'target-arrow-color': '#1a7a5e',
      }},
      { selector: 'edge:selected', style: {
        'line-color': '#3b7de8', 'target-arrow-color': '#3b7de8',
      }},
    ],
    layout: {
      name: 'breadthfirst', directed: true,
      padding: 40, spacingFactor: 1.8,
      animate: true, animationDuration: 400, fit: true,
      roots: data.nodes.filter(n => n.type === 'department').map(n => n.id),
    },
    wheelSensitivity: 0.3, minZoom: 0.08, maxZoom: 4,
  })

  const tooltip = document.getElementById('cy-tooltip')
  cyInstance.on('mouseover', 'node', e => {
    const d = e.target.data(); const pos = e.renderedPosition
    let html = `<strong style="color:#c8cdd8">${d.label}</strong><br>Тип: ${d.type}`
    if (d.type === 'item' && d.risk_score != null)
      html += `<br>Риск: <span style="color:${d.color}">${d.risk_score}/100 ${d.risk_level}</span>`
    if (d.departments?.length)
      html += `<br>Отделы: ${d.departments.join(', ')}`
    tooltip.innerHTML = html; tooltip.style.display = 'block'
    tooltip.style.left = (pos.x + 12) + 'px'; tooltip.style.top = (pos.y - 6) + 'px'
  })
  cyInstance.on('mouseout',  'node', () => tooltip.style.display = 'none')
  cyInstance.on('mousemove', e => {
    if (tooltip.style.display === 'block') {
      const p = e.renderedPosition
      tooltip.style.left = (p.x + 12) + 'px'; tooltip.style.top = (p.y - 6) + 'px'
    }
  })
}

function fitGraph()        { if (cyInstance) cyInstance.fit(undefined, 40) }
function zoomGraph(factor) {
  if (!cyInstance) return
  cyInstance.zoom({ level: cyInstance.zoom() * factor,
    renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } })
}


// ─── Raw JSON ─────────────────────────────────

function renderRaw(data) {
  const out = document.getElementById('raw-output'); const empty = document.getElementById('empty-raw')
  out.textContent = JSON.stringify(data, null, 2)
  empty.style.display = 'none'; out.style.display = 'block'
}

function resetResults() {
  allResults = []
  document.getElementById('results-body').innerHTML       = ''
  document.getElementById('results-content').style.display = 'none'
  document.getElementById('empty-results').style.display  = 'flex'
  document.getElementById('empty-graph').style.display    = 'flex'
  document.getElementById('cy').style.display             = 'none'
  document.getElementById('graph-legend').style.display   = 'none'
  document.getElementById('graph-controls').style.display = 'none'
  document.getElementById('raw-output').style.display     = 'none'
  document.getElementById('empty-raw').style.display      = 'flex'
}


// ─── Вкладки ──────────────────────────────────

function switchTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
  document.querySelectorAll('.tab-content').forEach(t => { t.classList.remove('active'); t.style.display = 'none' })
  el.classList.add('active')
  const tab = document.getElementById('tab-' + name)
  tab.classList.add('active'); tab.style.display = 'flex'
  if (name === 'graph' && cyInstance) setTimeout(() => cyInstance.resize(), 30)
}


// ─── Утилиты ──────────────────────────────────

function setProgress(pct) {
  const bar = document.getElementById('progress-bar'); const fill = document.getElementById('progress-fill')
  if (pct === 0) { bar.classList.remove('active'); fill.style.width = '0%' }
  else           { bar.classList.add('active');    fill.style.width = pct + '%' }
}

let toastTimer = null
function showToast(msg, type = '') {
  const toast = document.getElementById('toast')
  toast.textContent = msg; toast.className = `toast ${type} show`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => toast.classList.remove('show'), 4000)
}