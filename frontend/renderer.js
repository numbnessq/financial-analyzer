// frontend/renderer.js

const API = 'http://localhost:8000'
let selectedFiles  = []
let cyInstance     = null
let allResults     = []
let sortCol        = 'score'
let sortDir        = -1   // -1 = desc
let filterRisk     = 'ALL'


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
    if (!allowed.includes(ext)) { showToast(`⚠️ ${file.name} не поддерживается`, 'error'); return }
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
  const icons = { pdf: '📄', docx: '📝', xlsx: '📊' }
  list.innerHTML = selectedFiles.map((f, i) => {
    const ext = f.name.split('.').pop().toLowerCase()
    return `<div class="file-item">
      <span class="file-icon">${icons[ext] || '📁'}</span>
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
  if (cyInstance) { cyInstance.destroy(); cyInstance = null }
}


// ─── Анализ ───────────────────────────────────

async function runAnalyze() {
  if (!selectedFiles.length) return
  const btn = document.getElementById('btn-analyze')
  btn.disabled = true; btn.textContent = '⏳ Анализ...'
  setProgress(10)

  try {
    const formData = new FormData()
    selectedFiles.forEach(f => formData.append('files', f))
    setProgress(25)

    const uploadRes = await fetch(`${API}/upload`, { method: 'POST', body: formData })
    if (!uploadRes.ok) throw new Error(`Upload: ${uploadRes.status}`)
    const uploadData = await uploadRes.json()
    setProgress(45)

    const documents = (uploadData.files || []).map(f => ({
      filename:   f.original_name || '',
      department: f.detected_department || '',
      contractor: f.contractor || '',
      source_file: f.original_name || '',
      items: f.items || []
    }))

    const analyzeRes = await fetch(`${API}/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(documents)
    })
    if (!analyzeRes.ok) throw new Error(`Analyze: ${analyzeRes.status}`)
    setProgress(70)

    const [resultsRes, graphRes] = await Promise.all([fetch(`${API}/results`), fetch(`${API}/graph`)])
    const resultsData = await resultsRes.json()
    const graphData   = await graphRes.json()
    setProgress(100)

    allResults = resultsData.results || []
    renderResults()
    renderGraph(graphData)
    renderRaw({ results: resultsData, graph: graphData })
    showToast(`✅ Готово — ${allResults.length} позиций, аномалий: ${allResults.filter(r => r.score >= 20).length}`, 'success')

  } catch (err) {
    showToast(`❌ ${err.message}`, 'error')
    console.error(err)
  } finally {
    btn.disabled = false; btn.textContent = '⚡ Запустить анализ'
    setTimeout(() => setProgress(0), 800)
  }
}


// ─── Таблица: сортировка и фильтр ─────────────

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

  const table = document.getElementById('results-table')
  const empty = document.getElementById('empty-results')
  const body  = document.getElementById('results-body')

  const RISK_ORDER = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 }
  const colors     = { LOW: '#3dd68c', MEDIUM: '#f7a24f', HIGH: '#f75a5a', CRITICAL: '#b45af7' }

  let filtered = filterRisk === 'ALL'
    ? [...allResults]
    : allResults.filter(r => r.risk_level === filterRisk)

  filtered.sort((a, b) => {
    let av = a[sortCol], bv = b[sortCol]
    if (sortCol === 'risk_level') { av = RISK_ORDER[av] ?? 9; bv = RISK_ORDER[bv] ?? 9 }
    if (sortCol === 'name' || sortCol === 'item') { av = String(av || ''); bv = String(bv || '') }
    return av < bv ? sortDir : av > bv ? -sortDir : 0
  })

  body.innerHTML = filtered.map(r => {
    const color    = colors[r.risk_level] || '#6b7280'
    const name     = r.item || r.name || '—'
    const depts    = (r.departments || []).join(', ') || '—'
    const isCrit   = r.risk_level === 'CRITICAL'

    return `<tr class="${isCrit ? 'row-critical' : ''}">
      <td class="td-name">
        <div class="item-name">${name}</div>
        <div class="item-depts">${depts}</div>
      </td>
      <td class="td-score" style="white-space:nowrap">
        <div class="score-bar">
          <div class="score-track"><div class="score-fill" style="width:${r.score}%;background:${color}"></div></div>
          <span class="score-num" style="color:${color}">${r.score}</span>
        </div>
      </td>
      <td class="td-level" style="white-space:nowrap">
        <span class="badge badge-${r.risk_level}">${r.risk_level}</span>
      </td>
      <td class="td-explanation">${r.explanation || '—'}</td>
    </tr>`
  }).join('')

  empty.style.display = 'none'
  table.style.display = 'table'
}


// ─── Граф (Cytoscape breadthfirst) ────────────

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

  const sortOrder = { department: 0, item: 1, contractor: 2 }
  const sorted    = [...data.nodes].sort((a, b) => (sortOrder[a.type] ?? 3) - (sortOrder[b.type] ?? 3))

  const elements = [
    ...sorted.map(n => ({ data: { id: n.id, label: n.label, type: n.type, color: n.color || '#607d8b',
      risk_score: n.risk_score, risk_level: n.risk_level, departments: n.departments || [] } })),
    ...data.edges.map(e => ({ data: { id: `${e.source}__${e.target}`, source: e.source,
      target: e.target, etype: e.type || '', weight: e.weight || 1 } }))
  ]

  cyInstance = cytoscape({
    container: cyEl, elements,
    style: [
      { selector: 'node', style: {
        'background-color': 'data(color)', 'label': 'data(label)',
        'color': '#e8eaf2', 'font-size': '10px', 'font-family': 'JetBrains Mono, monospace',
        'text-valign': 'bottom', 'text-halign': 'center', 'text-margin-y': 6,
        'text-wrap': 'wrap', 'text-max-width': '120px',
        'border-width': 2, 'border-color': 'rgba(255,255,255,0.1)',
      }},
      { selector: 'node[type="department"]', style: { shape: 'ellipse', width: 60, height: 60 }},
      { selector: 'node[type="item"]',       style: { shape: 'round-rectangle', width: 56, height: 32 }},
      { selector: 'node[type="contractor"]', style: { shape: 'diamond', width: 50, height: 50 }},
      { selector: 'node:selected',           style: { 'border-color': '#4f8ef7', 'border-width': 3 }},
      { selector: 'edge', style: {
        width: 1.5, 'line-color': '#6b7280', 'target-arrow-color': '#6b7280',
        'target-arrow-shape': 'triangle', 'arrow-scale': 0.9, 'curve-style': 'bezier', opacity: 0.8,
      }},
      { selector: 'edge[etype="supplies"]', style: {
        'line-color': '#009688', 'target-arrow-color': '#009688',
      }},
      { selector: 'edge:selected', style: { 'line-color': '#fff', 'target-arrow-color': '#fff', opacity: 1 }},
    ],
    layout: {
      name: 'breadthfirst', directed: true, padding: 40, spacingFactor: 1.8,
      animate: true, animationDuration: 500, fit: true,
      roots: data.nodes.filter(n => n.type === 'department').map(n => n.id),
    },
    wheelSensitivity: 0.3, minZoom: 0.1, maxZoom: 4,
  })

  const tooltip = document.getElementById('cy-tooltip')
  cyInstance.on('mouseover', 'node', e => {
    const d = e.target.data(); const pos = e.renderedPosition
    let html = `<strong>${d.label}</strong><br>Тип: ${d.type}`
    if (d.type === 'item' && d.risk_score != null)
      html += `<br>Риск: <span style="color:${d.color}">${d.risk_score}/100 ${d.risk_level}</span>`
    if (d.departments?.length > 1)
      html += `<br>Отделы: ${d.departments.join(', ')}`
    tooltip.innerHTML = html; tooltip.style.display = 'block'
    tooltip.style.left = (pos.x + 14) + 'px'; tooltip.style.top = (pos.y - 8) + 'px'
  })
  cyInstance.on('mouseout',  'node', () => tooltip.style.display = 'none')
  cyInstance.on('mousemove', e => {
    if (tooltip.style.display === 'block') {
      const p = e.renderedPosition
      tooltip.style.left = (p.x + 14) + 'px'; tooltip.style.top = (p.y - 8) + 'px'
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
  document.getElementById('results-body').innerHTML       = ''
  document.getElementById('results-table').style.display  = 'none'
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