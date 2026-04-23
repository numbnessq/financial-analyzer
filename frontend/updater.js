// Вызывается при загрузке приложения
async function checkForUpdates() {
  // Tauri __TAURI__ доступен только внутри приложения, не в браузере
  if (!window.__TAURI__) return;

  const { checkUpdate, installUpdate, onUpdaterEvent } =
    window.__TAURI__.updater;
  const { relaunch } = window.__TAURI__.process;

  try {
    const { shouldUpdate, manifest } = await checkUpdate();
    if (!shouldUpdate) return;

    const ok = confirm(
      `Доступно обновление ${manifest.version}\n\n${manifest.body}\n\nУстановить сейчас?`
    );
    if (!ok) return;

    await installUpdate();
    await relaunch();
  } catch (err) {
    console.warn("[updater] check failed:", err);
  }
}

// Запуск через 3 сек после загрузки (не мешаем старту)
window.addEventListener("DOMContentLoaded", () => {
  setTimeout(checkForUpdates, 3000);
});