// frontend/app.js
import { uploadFiles, analyze, pollJob, getResults, getGraph } from './api.js'
import { renderGraph, fitGraph, zoomIn, zoomOut, destroyGraph, resizeGraph } from './graph.js'

let selectedFiles = []
let allResults    = []
let sortCol       = 'score'
let sortDir       = -1
let filterRisk    = 'ALL'

const BASE = 'http://127.0.0.1:8000'


// ─── Инициализация ────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  setupDropZone()
  document.getElementById('file-input').addEventListener('change', e =>
    addFiles(Array.from(e.target.files)))
  document.getElementById('btn-analyze').addEventListener('click', runAnalyze)
  document.getElementById('btn-report').addEventListener('click', downloadReport)
})


// ─── Tabs ─────────────────────────────────────────────────────────

window.switchTab = function(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
  document.querySelectorAll('.tab-content').forEach(t => {
    t.classList.remove('active')
    t.style.display = 'none'
  })
  if (el) el.classList.add('active')
  const content = document.getElementById(`tab-${name}`)
  if (content) { content.classList.add('active'); content.style.display = 'flex' }
  if (name === 'graph') setTimeout(resizeGraph, 30)
}


// ─── Drag & Drop ──────────────────────────────────────────────────

function setupDropZone() {
  const zone = document.getElementById('drop-zone')
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('active') })
  zone.addEventListener('dragleave', () => zone.classList.remove('active'))
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('active')
    addFiles(Array.from(e.dataTransfer.files))
  })
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

window.clearAll = function() {
  selectedFiles = []; allResults = []
  renderFileList(); resetResults()
  document.getElementById('file-input').value = ''
  document.getElementById('btn-report').disabled = true
  destroyGraph()
}


// ─── Анализ ───────────────────────────────────────────────────────

async function runAnalyze() {
  if (!selectedFiles.length) return
  const btn = document.getElementById('btn-analyze')
  btn.disabled = true; btn.textContent = '... АНАЛИЗ'
  setProgress(5)

  try {
    const uploadData = await uploadFiles(selectedFiles)
    setProgress(20)

    const documents = (uploadData.files || []).map(f => ({
      filename:    f.original_name || '',
      department:  '',
      contractor:  '',
      source_file: f.original_name || '',
      raw_text:    f.raw_text || '',
      items:       f.items   || [],
    }))

    const { job_id } = await analyze(documents)
    setProgress(30)

    await pollJob(job_id, (progress, message) => {
      setProgress(30 + Math.round(progress * 0.6))
      btn.textContent = message || '... АНАЛИЗ'
    })

    setProgress(92)

    const [resultsData, graphData] = await Promise.all([getResults(), getGraph()])
    setProgress(100)

    allResults = resultsData.results || []
    showResultsContent()
    renderTable()
    renderGraph(graphData)

    document.getElementById('btn-report').disabled = false

    const rawOut   = document.getElementById('raw-output')
    const emptyRaw = document.getElementById('empty-raw')
    if (rawOut && emptyRaw) {
      rawOut.textContent = JSON.stringify(resultsData, null, 2)
      rawOut.style.display = 'block'
      emptyRaw.style.display = 'none'
    }

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


// ─── Скачивание отчёта ────────────────────────────────────────────

async function downloadReport() {
  const btn = document.getElementById('btn-report')
  btn.disabled = true; btn.textContent = '... ГЕНЕРАЦИЯ'
  try {
    const r = await fetch(`${BASE}/report`)
    if (!r.ok) {
      const err = await r.json().catch(() => ({ detail: r.status }))
      throw new Error(err.detail || `Ошибка ${r.status}`)
    }
    const blob = await r.blob()
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    const now  = new Date()
    const ts   = now.toISOString().slice(0,19).replace('T','_').replaceAll(':','')
    a.href     = url
    a.download = `report_${ts}.docx`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    showToast('✓ Отчёт скачан', 'success')
  } catch (err) {
    showToast(`✕ ${err.message}`, 'error')
  } finally {
    btn.disabled = false; btn.textContent = '↓ СКАЧАТЬ ОТЧЁТ .DOCX'
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

// Краткие метки для объяснений
const EXPL_MAP = [
  ['Размытая формулировка',        '⚠ Размытая формулировка'],
  ['Возможное дробление',          '✂ Дробление'],
  ['Единственный поставщик',       '🔒 Один поставщик'],
  ['Подозрительно круглая сумма',  '🔢 Круглая сумма'],
  ['Единственное упоминание',      'ℹ Одна запись'],
  ['Количество = 0',               '⚠ Кол-во = 0'],
  ['Указана цена',                 '⚠ Нет объёма'],
  ['Указан объём',                 '⚠ Нет цены'],
  ['Сумма',                        '✗ Сумма ≠ цена×кол'],
  ['Объём расходится >50%',        '📊 Расхождение >50%'],
  ['Объём расходится >20%',        '📊 Расхождение >20%'],
  ['Разные единицы',               '📐 Разные ед.'],
  ['Цена',                         '📈 Откл. цены'],
  ['Закупается в',                 '📋 Дубль по отделам'],
  ['Подозрительный контрагент',    '🚫 Стоп-лист'],
  ['Частые закупки',               '🕐 Частые закупки'],
  ['Высокая центральность',        '🕸 Центр. узел'],
]

function shortenExplanation(explanation) {
  if (!explanation || explanation === '—') return '—'
  return explanation.split(' | ').map(part => {
    for (const [key, label] of EXPL_MAP) {
      if (part.startsWith(key)) return label
    }
    return part.length > 35 ? part.slice(0, 32) + '…' : part
  }).join(' · ')
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
    const expl  = shortenExplanation(r.explanation)
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
      <td><div class="explanation" title="${(r.explanation||'').replace(/"/g,"'")}">${expl}</div></td>
    </tr>`
  }).join('')
}


// ─── Reset ────────────────────────────────────────────────────────

function resetResults() {
  allResults = []
  document.getElementById('results-body').innerHTML        = ''
  document.getElementById('results-content').style.display = 'none'
  document.getElementById('empty-results').style.display   = 'flex'
  document.getElementById('empty-graph').style.display     = 'flex'
  ;['cy','graph-legend','graph-controls','raw-output'].forEach(id => {
    const el = document.getElementById(id)
    if (el) el.style.display = 'none'
  })
  const er = document.getElementById('empty-raw')
  if (er) er.style.display = 'flex'
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