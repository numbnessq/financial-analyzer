// tauri/src-tauri/src/main.rs

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::process::{Command, Child};
use std::sync::Mutex;
use tauri::Manager;

struct BackendProcess(Mutex<Option<Child>>);

fn start_python_backend() -> Option<Child> {
    // Определяем команду в зависимости от ОС
    let python_cmd = if cfg!(target_os = "windows") { "python" } else { "python3" };

    let result = Command::new(python_cmd)
        .arg("scripts/start_backend.py")
        .spawn();

    match result {
        Ok(child) => {
            println!("[backend] Python процесс запущен (PID: {})", child.id());
            Some(child)
        }
        Err(e) => {
            eprintln!("[backend] Ошибка запуска Python: {}", e);
            None
        }
    }
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            // Запускаем backend при старте приложения
            let child = start_python_backend();
            app.manage(BackendProcess(Mutex::new(child)));

            // Ждём пока backend поднимется (макс 5 сек)
            let client = reqwest::blocking::Client::new();
            for attempt in 1..=10 {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if client.get("http://127.0.0.1:8000/ping").send().is_ok() {
                    println!("[backend] Готов (попытка {})", attempt);
                    break;
                }
                println!("[backend] Ожидание... попытка {}/10", attempt);
            }

            Ok(())
        })
        .on_window_event(|event| {
            // Убиваем backend при закрытии окна
            if let tauri::WindowEvent::Destroyed = event.event() {
                if let Some(state) = event.window().try_state::<BackendProcess>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(mut child) = guard.take() {
                            let _ = child.kill();
                            println!("[backend] Процесс остановлен");
                        }
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("Ошибка запуска Tauri");
}