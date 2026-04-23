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
  return r.json()  // { job_id, status }
}

/**
 * Polling job до завершения.
 * onProgress(progress: 0-100, message: string) — callback для UI.
 * Возвращает итоговый объект job когда status === 'done'.
 * Бросает ошибку если status === 'error'.
 */
export async function pollJob(job_id, onProgress = null) {
  const INTERVAL_MS = 1000   // опрос каждую секунду
  const TIMEOUT_MS  = 300000 // максимум 5 минут

  const start = Date.now()

  while (true) {
    if (Date.now() - start > TIMEOUT_MS) {
      throw new Error('Анализ занял слишком много времени (>5 мин). Попробуй снова.')
    }

    const r = await fetch(`${BASE}/job/${job_id}`)
    if (!r.ok) throw new Error(`Job status failed: ${r.status}`)

    const job = await r.json()

    if (onProgress) {
      onProgress(job.progress || 0, job.message || '')
    }

    if (job.status === 'done') {
      return job
    }

    if (job.status === 'error') {
      throw new Error(job.message || 'Ошибка анализа на сервере')
    }

    // queued / extracting / analyzing / building_graph → ждём
    await new Promise(resolve => setTimeout(resolve, INTERVAL_MS))
  }
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