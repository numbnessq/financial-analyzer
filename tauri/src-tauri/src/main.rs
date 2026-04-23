#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri::RunEvent;

#[cfg(not(debug_assertions))]
use {
    std::sync::Mutex,
    std::net::TcpStream,
    std::time::Duration,
    tauri::Manager,
    tauri::api::process::{Command, CommandChild},
};

#[cfg(not(debug_assertions))]
struct BackendProcess(Mutex<Option<CommandChild>>);

#[cfg(not(debug_assertions))]
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
        .setup(|_app| {
            #[cfg(not(debug_assertions))]
            {
                let (_, child) = Command::new_sidecar("backend")
                    .expect("sidecar 'backend' not configured")
                    .spawn()
                    .expect("failed to spawn backend sidecar");
                _app.manage(BackendProcess(Mutex::new(Some(child))));
                wait_for_backend(8000, 60);
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