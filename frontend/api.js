// frontend/api.js
// Слой работы с FastAPI backend

const BASE = 'http://127.0.0.1:8000'

export async function ping() {
  const r = await fetch(`${BASE}/ping`)
  return r.json()
}

export async function uploadFiles(files) {
  const form = new FormData()
  files.forEach(f => form.append('files', f))
  const r = await fetch(`${BASE}/upload`, { method: 'POST', body: form })
  if (!r.ok) throw new Error(`Upload failed: ${r.status}`)
  return r.json()
}

export async function analyze(documents) {
  const r = await fetch(`${BASE}/analyze`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(documents),
  })
  if (!r.ok) throw new Error(`Analyze failed: ${r.status}`)
  return r.json()
}

export async function getResults() {
  const r = await fetch(`${BASE}/results`)
  if (!r.ok) throw new Error(`Results failed: ${r.status}`)
  return r.json()
}

export async function getGraph() {
  const r = await fetch(`${BASE}/graph`)
  if (!r.ok) throw new Error(`Graph failed: ${r.status}`)
  return r.json()
}