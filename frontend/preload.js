// frontend/preload.js
const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('api', {
  // Все запросы к backend идут через этот объект
  uploadFiles:  (formData) => ipcRenderer.invoke('upload-files', formData),
  analyze:      (data)     => ipcRenderer.invoke('analyze', data),
  getResults:   ()         => ipcRenderer.invoke('get-results'),
  getGraph:     ()         => ipcRenderer.invoke('get-graph'),
})