use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex};

use crate::store::AppState;

static SERVER_STATE: once_cell::sync::Lazy<Arc<Mutex<ServerState>>> =
    once_cell::sync::Lazy::new(|| Arc::new(Mutex::new(ServerState::default())));

#[derive(Default)]
struct ServerState {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<()>>,
}

pub fn create_app(state: AppState) -> Router {
    crate::api::usage::usage_routes(state)
}

pub async fn start_usage_api_server(state: AppState) -> Result<(), String> {
    let mut server_state = SERVER_STATE.lock().await;

    if server_state.handle.is_some() {
        log::info!("Usage API server already running");
        return Ok(());
    }

    let addr: SocketAddr = "127.0.0.1:15722".parse().unwrap();

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("Failed to bind to {}: {}", addr, e))?;

    let (tx, mut rx) = watch::channel(());
    let app = create_app(state);

    let handle = tokio::spawn(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = rx.changed().await;
        });
        if let Err(e) = serve.await {
            log::error!("Usage API server error: {e}");
        }
    });

    server_state.handle = Some(handle);
    server_state.shutdown_tx = Some(tx);

    log::info!("INFO: Usage API server started on {}", addr);
    Ok(())
}

pub async fn stop_usage_api_server() {
    let mut server_state = SERVER_STATE.lock().await;

    if let Some(tx) = server_state.shutdown_tx.take() {
        let _ = tx.send(());
    }

    if let Some(handle) = server_state.handle.take() {
        let _ = handle.await;
    }

    log::info!("Usage API server stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::time::Duration;

    async fn wait_for_server() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        for _ in 0..50 {
            if client
                .get("http://127.0.0.1:15722/providers")
                .send()
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Server did not start in time");
    }

    #[tokio::test]
    #[serial]
    async fn test_server_starts() {
        stop_usage_api_server().await;

        let app_state = AppState::new(Arc::new(crate::database::Database::memory().unwrap()));
        start_usage_api_server(app_state)
            .await
            .expect("server should start");

        wait_for_server().await;

        let client = reqwest::Client::new();
        let res = client
            .get("http://127.0.0.1:15722/providers")
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        assert!(res.is_ok(), "should connect to server");
        let response = res.unwrap();
        assert_eq!(response.status(), 200);

        stop_usage_api_server().await;
    }

    #[tokio::test]
    #[serial]
    async fn test_server_shutdown() {
        stop_usage_api_server().await;

        let app_state = AppState::new(Arc::new(crate::database::Database::memory().unwrap()));
        start_usage_api_server(app_state)
            .await
            .expect("server should start");

        wait_for_server().await;

        stop_usage_api_server().await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = reqwest::Client::new();
        let res = client
            .get("http://127.0.0.1:15722/providers")
            .timeout(Duration::from_secs(2))
            .send()
            .await;

        assert!(res.is_err(), "server should be shut down");

        stop_usage_api_server().await;
    }
}
