// frontend/renderer.js

const API = 'http://localhost:8000'

let selectedFiles = []
let cyInstance    = null  // Cytoscape instance


// ─────────────────────────────────────────────
// Файлы
// ─────────────────────────────────────────────

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
  const list    = document.getElementById('file-list')
  const section = document.getElementById('file-list-section')
  const counter = document.getElementById('file-count')
  const btn     = document.getElementById('btn-analyze')
  if (!selectedFiles.length) { section.style.display = 'none'; btn.disabled = true; return }
  section.style.display = 'block'
  counter.textContent   = selectedFiles.length
  btn.disabled          = false
  const icons = { pdf: '📄', docx: '📝', xlsx: '📊' }
  list.innerHTML = selectedFiles.map((file, i) => {
    const ext = file.name.split('.').pop().toLowerCase()
    return `<div class="file-item">
      <span class="file-icon">${icons[ext] || '📁'}</span>
      <span class="file-name">${file.name}</span>
      <span class="file-remove" onclick="removeFile(${i})">✕</span>
    </div>`
  }).join('')
}

function removeFile(i) { selectedFiles.splice(i, 1); renderFileList() }

function clearAll() {
  selectedFiles = []
  renderFileList()
  resetResults()
  document.getElementById('file-input').value = ''
  if (cyInstance) { cyInstance.destroy(); cyInstance = null }
}


// ─────────────────────────────────────────────
// Анализ
// ─────────────────────────────────────────────

async function runAnalyze() {
  if (!selectedFiles.length) return
  const btn = document.getElementById('btn-analyze')
  btn.disabled = true; btn.textContent = '⏳ Анализ...'
  setProgress(10)

  try {
    // 1. Upload
    const formData = new FormData()
    selectedFiles.forEach(f => formData.append('files', f))
    setProgress(30)
    const uploadRes = await fetch(`${API}/upload`, { method: 'POST', body: formData })
    if (!uploadRes.ok) throw new Error(`Upload: ${uploadRes.status}`)
    const uploadData = await uploadRes.json()
    setProgress(50)

    // 2. Analyze
    const documents = uploadData.files.map(f => ({
      filename:   f.original_name,
      department: 'Основной отдел',
      contractor: 'Не указан',
      items:      f.items || []
    }))
    const analyzeRes = await fetch(`${API}/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(documents)
    })
    if (!analyzeRes.ok) throw new Error(`Analyze: ${analyzeRes.status}`)
    setProgress(75)

    // 3. Results + Graph
    const [resultsRes, graphRes] = await Promise.all([
      fetch(`${API}/results`),
      fetch(`${API}/graph`)
    ])
    const resultsData = await resultsRes.json()
    const graphData   = await graphRes.json()
    setProgress(100)

    renderResults(resultsData.results)
    renderGraph(graphData)
    renderRaw({ results: resultsData, graph: graphData })
    showToast('✅ Анализ завершён', 'success')

  } catch (err) {
    showToast(`❌ ${err.message}`, 'error')
  } finally {
    btn.disabled = false; btn.textContent = '⚡ Запустить анализ'
    setTimeout(() => setProgress(0), 800)
  }
}


// ─────────────────────────────────────────────
// Таблица результатов
// ─────────────────────────────────────────────

function renderResults(results) {
  if (!results?.length) return
  const body  = document.getElementById('results-body')
  const table = document.getElementById('results-table')
  const empty = document.getElementById('empty-results')
  const colors = { LOW: '#3dd68c', MEDIUM: '#f7a24f', HIGH: '#f75a5a', CRITICAL: '#b45af7' }

  body.innerHTML = results.map(r => {
    const color = colors[r.risk_level] || '#6b7280'
    return `<tr>
      <td><strong>${r.name}</strong></td>
      <td>
        <div class="score-bar">
          <div class="score-track"><div class="score-fill" style="width:${r.score}%;background:${color}"></div></div>
          <span class="score-num" style="color:${color}">${r.score}</span>
        </div>
      </td>
      <td><span class="badge badge-${r.risk_level}">${r.risk_level}</span></td>
      <td><div class="explanation">${r.explanation || '—'}</div></td>
    </tr>`
  }).join('')

  empty.style.display = 'none'
  table.style.display = 'table'
}


// ─────────────────────────────────────────────
// Граф — Cytoscape.js
// ─────────────────────────────────────────────

function renderGraph(data) {
  const cyEl      = document.getElementById('cy')
  const emptyEl   = document.getElementById('empty-graph')
  const legendEl  = document.getElementById('graph-legend')
  const controlEl = document.getElementById('graph-controls')

  if (!data?.nodes?.length) return

  // Показываем контейнер
  emptyEl.style.display   = 'none'
  cyEl.style.display      = 'block'
  legendEl.style.display  = 'flex'
  controlEl.style.display = 'flex'

  // Уничтожаем старый граф
  if (cyInstance) cyInstance.destroy()

  // Конвертируем данные в формат Cytoscape
  const elements = []

  data.nodes.forEach(node => {
    elements.push({
      data: {
        id:          node.id,
        label:       node.label,
        type:        node.type,
        color:       node.color || '#607d8b',
        risk_score:  node.risk_score,
        risk_level:  node.risk_level,
      }
    })
  })

  data.edges.forEach(edge => {
    elements.push({
      data: {
        id:     `${edge.source}--${edge.target}`,
        source: edge.source,
        target: edge.target,
        label:  edge.label || '',
        weight: edge.weight || 1,
      }
    })
  })

  // Размер узла по типу
  const nodeSize = (type) => {
    if (type === 'department') return 50
    if (type === 'contractor') return 40
    return 30
  }

  cyInstance = cytoscape({
    container: cyEl,
    elements,

    style: [
      {
        selector: 'node',
        style: {
          'background-color':   'data(color)',
          'label':              'data(label)',
          'color':              '#e8eaf2',
          'font-size':          '11px',
          'font-family':        'JetBrains Mono, monospace',
          'text-valign':        'bottom',
          'text-halign':        'center',
          'text-margin-y':      6,
          'text-wrap':          'wrap',
          'text-max-width':     '120px',
          'width':              (ele) => nodeSize(ele.data('type')),
          'height':             (ele) => nodeSize(ele.data('type')),
          'border-width':       2,
          'border-color':       'rgba(255,255,255,0.15)',
          'transition-property': 'background-color border-color',
          'transition-duration': '0.2s',
        }
      },
      {
        selector: 'node:selected',
        style: {
          'border-color': '#4f8ef7',
          'border-width':  3,
        }
      },
      {
        selector: 'node[type = "department"]',
        style: {
          'shape': 'round-rectangle',
        }
      },
      {
        selector: 'node[type = "contractor"]',
        style: {
          'shape': 'diamond',
        }
      },
      {
        selector: 'edge',
        style: {
          'width':              1.5,
          'line-color':         '#252a38',
          'target-arrow-color': '#252a38',
          'target-arrow-shape': 'triangle',
          'curve-style':        'bezier',
          'opacity':             0.7,
        }
      },
      {
        selector: 'edge:selected',
        style: {
          'line-color':         '#4f8ef7',
          'target-arrow-color': '#4f8ef7',
          'opacity':             1,
        }
      },
    ],

    layout: {
      name:             'cose',
      animate:          true,
      animationDuration: 600,
      nodeRepulsion:    8000,
      idealEdgeLength:  120,
      gravity:          0.25,
      fit:              true,
      padding:          40,
    },

    wheelSensitivity: 0.3,
  })

  // Tooltip при наведении
  const tooltip = document.getElementById('cy-tooltip')

  cyInstance.on('mouseover', 'node', (e) => {
    const node  = e.target
    const pos   = e.renderedPosition
    const data  = node.data()
    let content = `<strong>${data.label}</strong><br>Тип: ${data.type}`
    if (data.risk_score != null) {
      content += `<br>Риск: ${data.risk_score}/100 <span style="color:${data.color}">${data.risk_level}</span>`
    }
    tooltip.innerHTML       = content
    tooltip.style.display   = 'block'
    tooltip.style.left      = (pos.x + 16) + 'px'
    tooltip.style.top       = (pos.y - 10) + 'px'
  })

  cyInstance.on('mouseout', 'node', () => {
    tooltip.style.display = 'none'
  })

  cyInstance.on('mousemove', (e) => {
    const pos = e.renderedPosition
    if (tooltip.style.display === 'block') {
      tooltip.style.left = (pos.x + 16) + 'px'
      tooltip.style.top  = (pos.y - 10) + 'px'
    }
  })
}

function fitGraph()         { if (cyInstance) cyInstance.fit(undefined, 40) }
function zoomGraph(factor)  { if (cyInstance) cyInstance.zoom({ level: cyInstance.zoom() * factor, renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } }) }


// ─────────────────────────────────────────────
// Raw JSON
// ─────────────────────────────────────────────

function renderRaw(data) {
  const out   = document.getElementById('raw-output')
  const empty = document.getElementById('empty-raw')
  out.textContent   = JSON.stringify(data, null, 2)
  empty.style.display = 'none'
  out.style.display   = 'block'
}

function resetResults() {
  document.getElementById('results-body').innerHTML      = ''
  document.getElementById('results-table').style.display = 'none'
  document.getElementById('empty-results').style.display = 'flex'
  document.getElementById('empty-graph').style.display   = 'flex'
  document.getElementById('cy').style.display            = 'none'
  document.getElementById('graph-legend').style.display  = 'none'
  document.getElementById('graph-controls').style.display= 'none'
  document.getElementById('raw-output').style.display    = 'none'
  document.getElementById('empty-raw').style.display     = 'flex'
}


// ─────────────────────────────────────────────
// Вкладки
// ─────────────────────────────────────────────

function switchTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'))
  document.querySelectorAll('.tab-content').forEach(t => {
    t.classList.remove('active')
    t.style.display = 'none'
  })
  el.classList.add('active')
  const tab = document.getElementById('tab-' + name)
  tab.classList.add('active')
  tab.style.display = 'flex'

  // После показа вкладки графа — пересчитать размер
  if (name === 'graph' && cyInstance) {
    setTimeout(() => cyInstance.resize(), 50)
  }
}


// ─────────────────────────────────────────────
// Прогресс и тосты
// ─────────────────────────────────────────────

function setProgress(pct) {
  const bar  = document.getElementById('progress-bar')
  const fill = document.getElementById('progress-fill')
  if (pct === 0) { bar.classList.remove('active'); fill.style.width = '0%' }
  else           { bar.classList.add('active'); fill.style.width = pct + '%' }
}

let toastTimer = null
function showToast(msg, type = '') {
  const toast = document.getElementById('toast')
  toast.textContent = msg
  toast.className   = `toast ${type} show`
  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => toast.classList.remove('show'), 3500)
}