// frontend/app.js
// Главная логика UI

import { uploadFiles, analyze, getResults, getGraph } from './api.js'
import { renderGraph, fitGraph, zoomIn, zoomOut, destroyGraph, resizeGraph } from './graph.js'

let selectedFiles = []
let allResults    = []
let sortCol       = 'score'
let sortDir       = -1
let filterRisk    = 'ALL'


// ─── Инициализация ────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  setupDropZone()
  setupTabs()
  document.getElementById('file-input').addEventListener('change', e =>
    addFiles(Array.from(e.target.files)))
  document.getElementById('btn-analyze').addEventListener('click', runAnalyze)
  document.getElementById('btn-clear').addEventListener('click', clearAll)
  document.getElementById('btn-fit').addEventListener('click', fitGraph)
  document.getElementById('btn-zoom-in').addEventListener('click', zoomIn)
  document.getElementById('btn-zoom-out').addEventListener('click', zoomOut)
})


// ─── Drag & Drop ──────────────────────────────────────────────────

function setupDropZone() {
  const zone = document.getElementById('drop-zone')
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('active') })
  zone.addEventListener('dragleave', () => zone.classList.remove('active'))
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('active')
    addFiles(Array.from(e.dataTransfer.files))
  })
  zone.addEventListener('click', () => document.getElementById('file-input').click())
}


// ─── Файлы ────────────────────────────────────────────────────────

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
      <span class="file-remove" data-idx="${i}">✕</span>
    </div>`
  }).join('')

  list.querySelectorAll('.file-remove').forEach(el =>
    el.addEventListener('click', () => { selectedFiles.splice(+el.dataset.idx, 1); renderFileList() })
  )
}

function clearAll() {
  selectedFiles = []; allResults = []
  renderFileList(); resetResults()
  document.getElementById('file-input').value = ''
  destroyGraph()
}


// ─── Анализ ───────────────────────────────────────────────────────

async function runAnalyze() {
  if (!selectedFiles.length) return
  const btn = document.getElementById('btn-analyze')
  btn.disabled = true; btn.textContent = '... АНАЛИЗ'
  setProgress(10)

  try {
    const uploadData = await uploadFiles(selectedFiles)
    setProgress(40)

    const documents = (uploadData.files || []).map(f => ({
      filename:    f.original_name || '',
      department:  '',
      contractor:  '',
      source_file: f.original_name || '',
      items:       f.items || [],
    }))

    await analyze(documents)
    setProgress(70)

    const [resultsData, graphData] = await Promise.all([getResults(), getGraph()])
    setProgress(100)

    allResults = resultsData.results || []
    showResultsContent()
    renderTable()
    renderGraph(graphData)

    const anomalies = allResults.filter(r => r.score >= 20).length
    showToast(`✓ ${allResults.length} позиций · аномалий: ${anomalies}`, 'success')

  } catch (err) {
    showToast(`✕ ${err.message}`, 'error')
    console.error(err)
  } finally {
    btn.disabled = false; btn.textContent = '▶ ЗАПУСТИТЬ АНАЛИЗ'
    setTimeout(() => setProgress(0), 800)
  }
}


// ─── Таблица ──────────────────────────────────────────────────────

function showResultsContent() {
  document.getElementById('empty-results').style.display   = 'none'
  document.getElementById('results-content').style.display = 'flex'
}

window.setFilter = function(risk) {
  filterRisk = risk
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'))
  document.querySelector(`[data-risk="${risk}"]`).classList.add('active')
  renderTable()
}

window.sortBy = function(col) {
  if (sortCol === col) sortDir *= -1
  else { sortCol = col; sortDir = -1 }
  document.querySelectorAll('.th-sort').forEach(th => {
    th.classList.remove('asc', 'desc')
    if (th.dataset.col === col) th.classList.add(sortDir === -1 ? 'desc' : 'asc')
  })
  renderTable()
}

function renderTable() {
  if (!allResults.length) return
  const body   = document.getElementById('results-body')
  const RISK_O = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 }
  const COLORS = { LOW: '#2ea84a', MEDIUM: '#d4882a', HIGH: '#cc3333', CRITICAL: '#7c3aed' }

  let data = filterRisk === 'ALL' ? [...allResults] : allResults.filter(r => r.risk_level === filterRisk)

  data.sort((a, b) => {
    let av = a[sortCol], bv = b[sortCol]
    if (sortCol === 'risk_level') { av = RISK_O[av] ?? 9; bv = RISK_O[bv] ?? 9 }
    if (typeof av === 'string') return av.localeCompare(bv, 'ru') * sortDir
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
      <td><span class="badge badge-${r.risk_level}">${r.risk_level}</span></td>
      <td><div class="explanation">${r.explanation || '—'}</div></td>
    </tr>`
  }).join('')
}


// ─── Вкладки ──────────────────────────────────────────────────────

function setupTabs() {
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      const name = tab.dataset.tab
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
      document.querySelectorAll('.tab-content').forEach(t => { t.classList.remove('active'); t.style.display = 'none' })
      tab.classList.add('active')
      const content = document.getElementById(`tab-${name}`)
      content.classList.add('active'); content.style.display = 'flex'
      if (name === 'graph') setTimeout(resizeGraph, 30)
    })
  })
}


// ─── Reset ────────────────────────────────────────────────────────

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


// ─── Утилиты ──────────────────────────────────────────────────────

function setProgress(pct) {
  const bar  = document.getElementById('progress-bar')
  const fill = document.getElementById('progress-fill')
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