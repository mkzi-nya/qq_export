
function settingButton(text, id, type = 'primary') {
  return `<setting-button id="${id}" data-type="${type}">${text}</setting-button>`;
}
function settingItem(title, subtitle, action) {
  return `<setting-item><div><setting-text>${title}</setting-text>${subtitle ? `<setting-text data-type="secondary">${subtitle}</setting-text>` : ''}</div><div>${action || ''}</div></setting-item>`;
}
function settingSection(items) {
  return `<setting-section><setting-panel><setting-list>${items.join('')}</setting-list></setting-panel></setting-section>`;
}
async function onSettingWindowCreated(view) {
  if (!view) return;
  const doc = new DOMParser().parseFromString([
    '<div>',
    settingSection([
      settingItem('qq_export', '', settingButton('打开导出网页', 'qq-export-open', 'primary')),
      settingItem('本地网页', '', settingButton('刷新状态', 'qq-export-status', 'secondary'))
    ]),
    '</div>'
  ].join(''), 'text/html');
  view.append(...doc.body.children);
  const openBtn = view.querySelector('#qq-export-open');
  const statusBtn = view.querySelector('#qq-export-status');
  const updateStatus = async () => {
    try { const res = await window.qqExport.status(); statusBtn.textContent = res.url || '未启动'; }
    catch { statusBtn.textContent = '状态获取失败'; }
  };
  openBtn?.addEventListener('click', async () => {
    openBtn.textContent = '正在打开...';
    try { const res = await window.qqExport.openWebUi(); openBtn.textContent = '已打开'; statusBtn.textContent = res.url || ''; }
    catch (e) { openBtn.textContent = '打开失败'; console.error('[qq_export] open failed', e); }
  });
  statusBtn?.addEventListener('click', updateStatus);
  updateStatus();
}
export { onSettingWindowCreated };
