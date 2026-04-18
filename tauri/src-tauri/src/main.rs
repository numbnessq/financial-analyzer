#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;
use tauri::{Manager, RunEvent};
use tauri::api::process::{Command, CommandChild};

struct BackendProcess(Mutex<Option<CommandChild>>);

fn wait_for_backend(port: u16, retries: u32) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..retries {
        if TcpStream::connect(&addr).is_ok() { return; }
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("[tauri] WARNING: backend not ready after waiting");
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            // В dev-режиме бэкенд запускает beforeDevCommand — не трогаем
            #[cfg(not(debug_assertions))]
            {
                let (_, child) = Command::new_sidecar("backend")
                    .expect("sidecar 'backend' not configured")
                    .spawn()
                    .expect("failed to spawn backend sidecar");

                app.manage(BackendProcess(Mutex::new(Some(child))));
                wait_for_backend(8000, 60); // ждём до 18 сек
            }
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let RunEvent::ExitRequested { .. } = event {
                if let Some(state) = app_handle.try_state::<BackendProcess>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                }
            }
        });
}