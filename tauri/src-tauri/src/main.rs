#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::RunEvent;

#[cfg(not(debug_assertions))]
use {
    std::sync::Mutex,
    std::time::Duration,
    tauri::Manager,
    tauri::api::process::{Command, CommandChild},
};

#[cfg(not(debug_assertions))]
struct BackendProcess(Mutex<Option<CommandChild>>);

/// HTTP healthcheck через /ping — гарантирует что FastAPI реально готов
#[cfg(not(debug_assertions))]
fn wait_for_backend(retries: u32) -> bool {
    for i in 0..retries {
        std::thread::sleep(Duration::from_millis(500));
        match ureq::get("http://127.0.0.1:8000/ping").call() {
            Ok(resp) if resp.status() == 200 => {
                eprintln!("[tauri] backend ready after {}ms", (i + 1) * 500);
                return true;
            }
            _ => {
                eprintln!("[tauri] waiting for backend... attempt {}/{}", i + 1, retries);
            }
        }
    }
    false
}

fn main() {
    tauri::Builder::default()
        .setup(|_app| {
            #[cfg(not(debug_assertions))]
            {
                // Папка данных приложения — стабильный путь для uploads/
                let app_dir = _app
                    .path_resolver()
                    .app_data_dir()
                    .expect("failed to get app data dir");
                std::fs::create_dir_all(&app_dir).ok();

                let app_dir_str = app_dir.to_string_lossy().to_string();
                eprintln!("[tauri] app_data_dir: {}", app_dir_str);

                let (mut rx, child) = Command::new_sidecar("backend")
                    .expect("sidecar 'backend' not configured")
                    .args(["--app-dir", &app_dir_str])
                    .spawn()
                    .expect("failed to spawn backend sidecar");

                // Логируем stdout/stderr бинаря
                std::thread::spawn(move || {
                    use tauri::api::process::CommandEvent;
                    while let Some(event) = rx.blocking_recv() {
                        match event {
                            CommandEvent::Stdout(line)   => eprintln!("[backend] {}", line),
                            CommandEvent::Stderr(line)   => eprintln!("[backend:err] {}", line),
                            CommandEvent::Error(e)       => eprintln!("[backend:crash] {}", e),
                            CommandEvent::Terminated(s)  => {
                                eprintln!("[backend] terminated: {:?}", s);
                                break;
                            }
                            _ => {}
                        }
                    }
                });

                _app.manage(BackendProcess(Mutex::new(Some(child))));

                if !wait_for_backend(60) {
                    eprintln!("[tauri] FATAL: backend did not start in 30s");
                    std::process::exit(1);
                }
            }
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|_app_handle, event| {
            if let RunEvent::ExitRequested { .. } = event {
                #[cfg(not(debug_assertions))]
                if let Some(state) = _app_handle.try_state::<BackendProcess>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(mut child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                }
            }
        });
}