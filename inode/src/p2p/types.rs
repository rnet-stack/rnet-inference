use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::p2p::service::IPayload;

#[derive(Debug)]
pub enum Mode {
    Provider,
    Bootstrap,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IMsgType {
    General(String),
    Service(IPayload),
    Bootmesh(HashMap<String, Vec<String>>),
}
