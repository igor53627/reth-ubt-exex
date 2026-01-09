//! JSON-RPC server wiring (IPC + HTTP).

use std::path::{Path, PathBuf};

use eyre::Result;
use jsonrpsee::server::{serve_with_graceful_shutdown, stop_channel, Server, ServerBuilder};
use jsonrpsee::Methods;
use reth_tasks::TaskExecutor;
use tokio::net::UnixListener;
use tracing::{info, warn};

use crate::rpc::{UbtApiServer, UbtRpc};

#[derive(Debug, Clone)]
pub struct RpcServerConfig {
    pub http_addr: Option<String>,
    pub ipc_path: Option<PathBuf>,
}

pub async fn start_rpc_servers(
    executor: TaskExecutor,
    rpc: UbtRpc,
    config: RpcServerConfig,
) -> Result<()> {
    if let Some(ipc_path) = config.ipc_path {
        info!(path = %ipc_path.display(), "UBT IPC RPC enabled");
        let methods = rpc.clone().into_rpc().into();
        let executor = executor.clone();
        let ipc_path = ipc_path.clone();
        executor.spawn_critical("ubt-rpc-ipc", async move {
            if let Err(err) = run_ipc_server(&ipc_path, methods).await {
                warn!(path = %ipc_path.display(), error = %err, "IPC server failed");
            }
        });
    } else {
        info!("UBT IPC RPC disabled");
    }

    if let Some(http_addr) = config.http_addr {
        info!(addr = %http_addr, "UBT HTTP RPC enabled");
        let methods = rpc.into_rpc().into();
        let executor = executor.clone();
        let http_addr = http_addr.clone();
        executor.spawn_critical("ubt-rpc-http", async move {
            if let Err(err) = run_http_server(&http_addr, methods).await {
                warn!(addr = %http_addr, error = %err, "HTTP RPC server failed");
            }
        });
    } else {
        info!("UBT HTTP RPC disabled");
    }

    Ok(())
}

async fn run_http_server(addr: &str, methods: Methods) -> Result<()> {
    let server = ServerBuilder::default().build(addr).await?;
    let handle = server.start(methods);
    handle.stopped().await;
    Ok(())
}

async fn run_ipc_server(path: &Path, methods: Methods) -> Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let listener = UnixListener::bind(path)?;
    let (stop_handle, server_handle) = stop_channel();
    let rpc_svc = Server::builder().http_only().to_service_builder().build(methods, stop_handle.clone());

    tokio::spawn(async move {
        server_handle.stopped().await;
    });

    loop {
        let stream = tokio::select! {
            res = listener.accept() => match res {
                Ok((stream, _)) => stream,
                Err(err) => {
                    warn!(error = %err, "IPC accept failed");
                    continue;
                }
            },
            _ = stop_handle.clone().shutdown() => {
                break;
            }
        };

        let rpc_svc = rpc_svc.clone();
        let stop_handle = stop_handle.clone();

        tokio::spawn(async move {
            let svc = rpc_svc.clone();
            if let Err(err) = serve_with_graceful_shutdown(stream, svc, stop_handle.clone().shutdown()).await {
                warn!(error = %err, "IPC connection failed");
            }
        });
    }

    Ok(())
}
