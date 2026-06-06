use anyhow::Result;
use rnet_p2p::identity::multiaddr::Multiaddr;
use rnet_p2p::identity::traits::{
    core::INode,
    protocols::{INodeFloodsubAPI, INodePingAPI},
};
use std::collections::HashMap;
use std::{io::Write, sync::Arc, time::Duration};
use tokio::io::{self, AsyncBufReadExt};

use crate::p2p::node::InferenceNode;
use crate::p2p::types::IMsgType;

const CLI_DELAY: Duration = Duration::from_nanos(1000);
const FLOODSUB: &str = "rnet/floodsub/0.0.1";
const COMMANDS: &[&str] = &[
    "help                       => print all the commands",
    "local                      => get local peer-info",
    "connect <maddr>            => connect with a new peer",
    "ping <maddr> <count>       => exchange ping with a peer",
    "peers                      => list the connected peers",
    "\n",
    "fsub <maddr>               => open a new floodsub stream with the peer",
    "join <topic>               => subscribe to a new-topic",
    "leave <topic>              => unsubscribe to a new-topic",
    "publish <topic> <msg>      => publish a msg to a topic",
    "topics                     => list the subscribed topics",
    "fpeers                     => list the connected Floodsub peers",
    "bootmesh                   => map of topics -> peer (BOOTSTRAP)",
    "mesh                       => map of topics -> peer",
    "\n",
    "slm                        => converse with the AI",
];

pub fn print_commands() {
    for cmd in COMMANDS {
        println!("      {}", cmd);
    }
}

pub async fn handle_cmd(line: &str, inode: &Arc<InferenceNode>) -> Result<()> {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap();

    match cmd {
        "help" => {
            print_commands();
        }

        "local" => {
            let peer_info = inode.local.clone();
            println!("{}", peer_info);
        }

        "connect" => {
            let maddr = Multiaddr::new(parts.next().unwrap()).unwrap();
            inode.p2p.connect(&maddr).await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "ping" => {
            let maddr = parts.next().unwrap();
            let count: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
            inode.p2p.ping(Some(count), maddr).await.unwrap();
        }

        "fsub" => {
            let maddr = parts.next().unwrap();
            inode
                .p2p
                .new_stream(maddr, vec![FLOODSUB.to_string()])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "join" => {
            let topic = parts.next().unwrap().to_string();
            inode.p2p.floodsub_subscribe(topic).await.unwrap();
        }

        "leave" => {
            let topic = parts.next().unwrap().to_string();
            inode.p2p.floodsub_unsubscribe(vec![topic]).await.unwrap();
        }

        "publish" => {
            let topic = parts.next().unwrap().to_string();
            let msg = parts.collect::<Vec<_>>().join(" ");

            // Wrap this into IMsgType::General
            let mpc_general = IMsgType::General(msg);
            let payload = bincode::serialize(&mpc_general).unwrap();

            inode.p2p.floodsub_publish(topic, payload).await.unwrap();
        }

        "topics" => {
            let topics = inode.p2p.floodsub_topics().await.unwrap_or(vec![]);
            println!("{:?}", topics);
        }

        "fpeers" => {
            let fpeers = inode.p2p.floodsub_peers().await.unwrap_or(vec![]);
            fpeers.iter().for_each(|x| {
                println!("{}", x);
            });
        }

        "bootmesh" => {
            let bootmesh = inode.bootmesh.lock().await.clone();

            bootmesh.iter().for_each(|(topic, peers)| {
                println!("- {}", topic);
                peers.iter().for_each(|peer| println!("  - {}", peer));
            });
        }

        "peers" => {
            let gpeers = inode.p2p.get_peers().await;
            gpeers.iter().for_each(|peer| println!("{}", peer));
        }

        "mesh" => {
            let mesh = inode.p2p.floodsub_mesh().await.unwrap_or(HashMap::new());

            mesh.iter().for_each(|(topic, peers)| {
                println!("- {}", topic);
                peers.iter().for_each(|peer| println!("  - {}", peer));
            });
        }

        "slm" => {
            let prompt = parts.collect::<Vec<_>>().join(" ");

            let res = inode.slm.converse(prompt.as_ref()).await.unwrap();
            println!("{}", res);
        }

        _ => println!("Unknown command"),
    }
    Ok(())
}

pub async fn io_loop(inode: Arc<InferenceNode>) -> Result<()> {
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    println!("\n inode CLI ready. Commands:");
    print_commands();

    loop {
        print!("\nCommand => ");
        std::io::stdout().flush().unwrap();

        let line = match lines.next_line().await? {
            Some(line) => line,
            None => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        handle_cmd(line, &inode).await?;
        tokio::time::sleep(CLI_DELAY).await;
    }

    Ok(())
}
