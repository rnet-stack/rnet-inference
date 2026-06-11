use anyhow::Result;
use inode::p2p::{cmds::io_loop, node::InferenceNode, types::Mode};
use std::env;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("trace"))
        .without_time()
        .with_target(false)
        .compact()
        .init();

    dotenvy::dotenv().ok();

    let mut mode = Mode::Provider;
    let args = env::args().skip(1);

    for arg in args {
        if arg == "bootstrap" {
            mode = Mode::Bootstrap;
        }
    }

    let inode = InferenceNode::new(mode).await;
    io_loop(inode).await.unwrap();

    Ok(())
}
