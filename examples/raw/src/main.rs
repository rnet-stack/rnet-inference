use std::io::{self, Write};

use anyhow::Result;
use inode::p2p::{cmds::io_loop, node::InferenceNode, types::Mode};
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

    print!("Intiate at BOOTSTRAP node (Y/n): ");
    io::stdout().flush().unwrap();

    let mut io = String::new();
    io::stdin().read_line(&mut io).unwrap();

    let mut mode = Mode::Bootstrap;
    if io.trim().to_lowercase() != "" {
        mode = Mode::Provider;
    }

    let inode = InferenceNode::new(mode).await;
    io_loop(inode).await.unwrap();

    Ok(())
}
