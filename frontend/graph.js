// frontend/graph.js
// Визуализация графа через Cytoscape.js

let cyInstance = null

const RISK_COLORS = {
  LOW:      '#2ea84a',
  MEDIUM:   '#d4882a',
  HIGH:     '#cc3333',
  CRITICAL: '#7c3aed',
}

export function renderGraph(data, containerId = 'cy') {
  const container = document.getElementById(containerId)
  if (!container || !data?.nodes?.length) return

  if (cyInstance) cyInstance.destroy()

  const sortOrder = { department: 0, item: 1, contractor: 2 }
  const sorted    = [...data.nodes].sort((a, b) =>
    (sortOrder[a.type] ?? 3) - (sortOrder[b.type] ?? 3)
  )

  const elements = [
    ...sorted.map(n => ({ data: {
      id: n.id, label: n.label, type: n.type,
      color: n.color || '#3b4258',
      risk_score: n.risk_score, risk_level: n.risk_level,
      departments: n.departments || [],
    }})),
    ...data.edges.map(e => ({ data: {
      id:     `${e.source}__${e.target}`,
      source: e.source, target: e.target,
      etype:  e.type || '', weight: e.weight || 1,
    }})),
  ]

  cyInstance = cytoscape({
    container,
    elements,
    style: [
      { selector: 'node', style: {
        'background-color': 'data(color)',
        'label':            'data(label)',
        'color':            '#8b90a0',
        'font-size':        '9px',
        'font-family':      'JetBrains Mono, monospace',
        'font-weight':      '500',
        'text-valign':      'bottom',
        'text-halign':      'center',
        'text-margin-y':    5,
        'text-wrap':        'wrap',
        'text-max-width':   '100px',
        'border-width':     1,
        'border-color':     'rgba(255,255,255,0.08)',
      }},
      { selector: 'node[type="department"]', style: {
        shape: 'ellipse', width: 52, height: 52,
        'background-color': '#1a2a4a',
        'border-color':     '#3b7de8', 'border-width': 1.5,
        'color':            '#6a9ee8',
      }},
      { selector: 'node[type="item"]', style: {
        shape: 'round-rectangle', width: 54, height: 26,
        'background-color': '#0f1117',
        'border-color':     'data(color)', 'border-width': 1,
      }},
      { selector: 'node[type="contractor"]', style: {
        shape: 'diamond', width: 44, height: 44,
        'background-color': '#0f2a22',
        'border-color':     '#1a7a5e', 'border-width': 1.5,
        'color':            '#3aaa80',
      }},
      { selector: 'node:selected', style: {
        'border-color': '#3b7de8', 'border-width': 2,
      }},
      { selector: 'edge', style: {
        width: 1, 'line-color': '#1e2130',
        'target-arrow-color': '#1e2130',
        'target-arrow-shape': 'triangle',
        'arrow-scale': 0.7, 'curve-style': 'bezier',
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

  // Tooltip
  const tooltip = document.getElementById('cy-tooltip')
  if (tooltip) {
    cyInstance.on('mouseover', 'node', e => {
      const d   = e.target.data()
      const pos = e.renderedPosition
      let html  = `<strong>${d.label}</strong><br>Тип: ${d.type}`
      if (d.type === 'item' && d.risk_score != null)
        html += `<br>Риск: ${d.risk_score}/100 ${d.risk_level}`
      if (d.departments?.length)
        html += `<br>Отделы: ${d.departments.join(', ')}`
      tooltip.innerHTML     = html
      tooltip.style.display = 'block'
      tooltip.style.left    = (pos.x + 12) + 'px'
      tooltip.style.top     = (pos.y - 6)  + 'px'
    })
    cyInstance.on('mouseout',  'node', () => tooltip.style.display = 'none')
    cyInstance.on('mousemove', e => {
      if (tooltip.style.display === 'block') {
        const p = e.renderedPosition
        tooltip.style.left = (p.x + 12) + 'px'
        tooltip.style.top  = (p.y - 6)  + 'px'
      }
    })
  }

  return cyInstance
}

export function fitGraph()  { if (cyInstance) cyInstance.fit(undefined, 40) }
export function zoomIn()    { if (cyInstance) cyInstance.zoom({ level: cyInstance.zoom() * 1.2, renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } }) }
export function zoomOut()   { if (cyInstance) cyInstance.zoom({ level: cyInstance.zoom() * 0.8, renderedPosition: { x: cyInstance.width() / 2, y: cyInstance.height() / 2 } }) }
export function destroyGraph() { if (cyInstance) { cyInstance.destroy(); cyInstance = null } }
export function resizeGraph()  { if (cyInstance) cyInstance.resize() }