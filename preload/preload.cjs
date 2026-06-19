'use strict';
const { contextBridge, ipcRenderer } = require('electron');
contextBridge.exposeInMainWorld('qqExport', {
  openWebUi: () => ipcRenderer.invoke('qq_export_open_web'),
  status: () => ipcRenderer.invoke('qq_export_status')
});
