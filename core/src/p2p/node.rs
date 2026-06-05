use anyhow::Result;
use rnet_p2p::{
    identity::{
        events::{FloodsubMsgType, GlobalEvent},
        multiaddr::Multiaddr,
        traits::{core::INode, protocols::INodeFloodsubAPI},
    },
    node::{inner::NodeInner, node::Node, protocol::InnerProtocolOpt},
    protocols::FLOODSUB,
};
use std::{collections::HashMap, env, sync::Arc, time::Duration};
use tokio::sync::{mpsc::Receiver, Mutex};
use tracing::{debug, info};

use crate::p2p::types::{IMsgType, Mode};

pub type BOOTMESH = Arc<Mutex<HashMap<String, Vec<String>>>>;
pub const PROVIDER_MESH: &str = "swarm/mesh";

pub struct InferenceNode {
    pub p2p: Arc<Node>,
    pub mode: Mode,
    pub local: Multiaddr,

    pub bootmesh: BOOTMESH,
}

impl InferenceNode {
    pub async fn new(mode: Mode) -> Arc<InferenceNode> {
        debug!("Running as {:?} node", mode);

        let (p2p_tx, event_rx, listen_addr) = match mode {
            Mode::Bootstrap => {
                let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/8888").unwrap();
                let key_hex = env::var("BOOTSTRAP_PVT_KEY").unwrap();
                let (host_mpsc_tx, global_rx) = NodeInner::new(
                    &mut listen_addr,
                    vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
                    Some(key_hex),
                )
                .await
                .unwrap();

                (host_mpsc_tx, global_rx, listen_addr)
            }
            _ => {
                let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/0").unwrap();
                let (host_mpsc_tx, global_rx) = NodeInner::new(
                    &mut listen_addr,
                    vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
                    None,
                )
                .await
                .unwrap();

                (host_mpsc_tx, global_rx, listen_addr)
            }
        };

        let inode = Arc::new(InferenceNode {
            p2p: p2p_tx.clone(),
            mode,
            local: listen_addr,
            bootmesh: Arc::new(Mutex::new(HashMap::new())),
        });

        let events = inode.clone();
        tokio::spawn(async move {
            events.event_handler(event_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        let helper = inode.clone();
        inode.initiate(helper).await.unwrap();

        inode
    }

    pub async fn initiate(&self, mesh_mpc_node: Arc<InferenceNode>) -> Result<()> {
        self.p2p
            .floodsub_subscribe(PROVIDER_MESH.to_string())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        match self.mode {
            Mode::Bootstrap => {
                tokio::spawn(async move {
                    mesh_mpc_node.periodic_mesh_update().await.unwrap();
                });
            }
            Mode::Provider => {
                // CONNECT TO BOOTSTRAP NODE
                let bootstrap_node = Multiaddr::new(&env::var("BOOTSTRAP_NODE").unwrap()).unwrap();
                info!("BOOTSTRAP node found, CONNECTING...\n");
                self.p2p
                    .new_stream(&bootstrap_node.to_string(), vec![FLOODSUB.to_string()])
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
        }

        Ok(())
    }

    pub async fn event_handler(&self, mut event_rx: Receiver<Vec<u8>>) -> Result<()> {
        loop {
            let notification = event_rx.recv().await.unwrap();
            let decoded = bincode::deserialize::<GlobalEvent>(&notification).unwrap();

            match decoded {
                GlobalEvent::Floodsub(event) => match event.msg_type {
                    FloodsubMsgType::Publish => {
                        let topic = event.topic.clone();
                        let source = event.source.unwrap();
                        let decoded_msg =
                            bincode::deserialize::<IMsgType>(&event.msg.unwrap()).unwrap();

                        match decoded_msg {
                            IMsgType::General(msg) => {
                                debug!("FloodsubEvent: {topic} - {source}: {msg}")
                            }
                            IMsgType::Service(_payload) => {}
                            IMsgType::Bootmesh(mesh) => {
                                let mut bootmesh = self.bootmesh.lock().await;
                                *bootmesh = mesh;

                                debug!("BOOTMESH");
                            }
                        }
                    }
                    _ => {}
                },
                GlobalEvent::Ping(event) => debug!("{:?}", event),
            }
        }
    }

    pub async fn periodic_mesh_update(&self) -> Result<()> {
        loop {
            let bootmesh = {
                let bootmesh = self.bootmesh.lock().await;
                bootmesh.clone()
            };

            let latest_mesh = self.p2p.floodsub_mesh().await.unwrap_or(HashMap::new());

            if bootmesh == latest_mesh {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            {
                let mut bootmesh = self.bootmesh.lock().await;
                *bootmesh = latest_mesh.clone();
            }

            self.broadcast_bootmesh(latest_mesh).await.unwrap();
        }
    }

    pub async fn broadcast_bootmesh(&self, mesh: HashMap<String, Vec<String>>) -> Result<()> {
        let fsub_msg = IMsgType::Bootmesh(mesh);
        let payload = bincode::serialize(&fsub_msg).unwrap();

        // Wait 2 seconds for the new node to settle down
        tokio::time::sleep(Duration::from_secs(2)).await;

        debug!("BOOTMESH updated");

        self.p2p
            .floodsub_publish(PROVIDER_MESH.to_string(), payload)
            .await
            .unwrap();

        Ok(())
    }
}
