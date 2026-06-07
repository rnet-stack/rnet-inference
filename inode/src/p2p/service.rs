use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use rand::{rng, seq::IteratorRandom};
use rnet_p2p::{
    identity::traits::{core::INode, protocols::INodeFloodsubAPI},
    node::node::Node,
    protocols::FLOODSUB,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::{
    common::{generate_random, last_n_chars, random_split},
    p2p::{node::PROVIDER_MESH, types::IMsgType},
    slm::core::SlmClient,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct IPayload {
    stage: IStage,
    leader: String,
    source: String,
    task_id: String,

    prompt: Option<String>,
    res: Option<String>,
    generator: Option<String>,
    verify_score: Option<String>,
    exec: Option<Vec<String>>,
    verifiers: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IStage {
    Adv,
    Exec,
    Verf,
    Final,
}

pub struct SessionStorage {
    pub prompt: String,
    pub winner: Option<TaskWinner>,
    pub execs: Vec<String>,
    pub verifiers: Vec<String>,
    pub rtc_score_count: u32,
    pub responses: HashMap<String, (String, Vec<f32>)>,
}

#[derive(Debug)]
pub struct TaskWinner {
    pub generator: String,
    pub prompt: String,
    pub response: String,
    pub score: f32,
}

pub struct IService {
    pub local: String,
    pub p2p: Arc<Node>,
    pub slm: SlmClient,
    pub bootmesh: Arc<Mutex<HashMap<String, Vec<String>>>>,

    pub sessions: Arc<Mutex<HashMap<String, SessionStorage>>>,
}

impl IService {
    pub fn new(
        local: String,
        p2p: Arc<Node>,
        slm: SlmClient,
        bootmesh: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ) -> Self {
        Self {
            local,
            p2p,
            slm,
            bootmesh,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn adv(&self, topic: String, ipayload: Option<IPayload>) -> Result<()> {
        match ipayload.is_none() {
            true => {
                self.p2p.floodsub_subscribe(topic.clone()).await.unwrap();

                let adv_payload = IPayload {
                    leader: self.local.clone(),
                    source: self.local.clone(),
                    task_id: topic.clone(),
                    stage: IStage::Adv,

                    prompt: None,
                    res: None,
                    generator: None,
                    verify_score: None,
                    exec: None,
                    verifiers: None,
                };

                let ipayload = IMsgType::Service(adv_payload);
                let fsub_payload = bincode::serialize(&ipayload).unwrap();

                self.p2p
                    .floodsub_publish(PROVIDER_MESH.to_string(), fsub_payload)
                    .await
                    .unwrap();

                let mut sessions = self.sessions.lock().await;
                sessions.insert(
                    topic,
                    SessionStorage {
                        prompt: String::new(),
                        winner: None,
                        execs: vec![],
                        verifiers: vec![],
                        rtc_score_count: 0,
                        responses: HashMap::new(),
                    },
                );
            }

            false => {
                let ipayload = ipayload.unwrap();
                let task_id = ipayload.task_id;

                // Ask if the node wants to participate
                // let mut io = String::new();
                // print!("Participate in Exec/Vrify => [{task_id}] - (Y/n): ");

                // io::stdout().flush().unwrap();
                // io::stdin().read_line(&mut io).unwrap();
                // io = io.trim().to_lowercase();

                // if io != "" && io != "y" {
                //     return Ok(());
                // }

                let nonce = (4..6).choose(&mut rng()).unwrap();
                tokio::time::sleep(Duration::from_secs(nonce)).await;

                // ---------------------------

                info!("Exec/Vrify session starting in: {}\n", task_id);

                // connect with the leader
                debug!("Connecting with the leader: {}", ipayload.source);
                self.p2p
                    .new_stream(&ipayload.source, vec![FLOODSUB.to_string()])
                    .await
                    .unwrap();

                // TODO: connect with a random peer too
                self.p2p.floodsub_subscribe(task_id).await.unwrap();
            }
        }

        Ok(())
    }

    pub async fn ack(
        &self,
        topic: String,
        ipayload: Option<IPayload>,
        prompt: Option<String>,
    ) -> Result<()> {
        match ipayload.is_none() {
            true => {
                let mesh = self.p2p.floodsub_mesh().await.unwrap();

                let participants = mesh.get(&topic).unwrap().clone();
                let (execs, verifiers) = random_split(participants);

                println!("Execs: {:?}", execs);
                println!("Verifiers: {:?}", verifiers);

                let ack_payload = IPayload {
                    stage: IStage::Exec,
                    leader: self.local.clone(),
                    source: self.local.clone(),
                    task_id: topic.clone(),

                    prompt: prompt.clone(),
                    res: None,
                    generator: None,
                    verify_score: None,
                    exec: Some(execs.clone()),
                    verifiers: Some(verifiers.clone()),
                };

                let ipayload = IMsgType::Service(ack_payload);
                let fsub_payload = bincode::serialize(&ipayload).unwrap();

                self.p2p
                    .floodsub_publish(topic.clone(), fsub_payload)
                    .await
                    .unwrap();

                // --------------
                let mut sessions = self.sessions.lock().await;
                let task = sessions.get_mut(&topic).unwrap();

                task.execs = execs;
                task.verifiers = verifiers;
                task.prompt = prompt.unwrap().clone();
                // --------------
            }

            false => {
                let ipayload = ipayload.unwrap();

                match ipayload.exec.clone().unwrap().contains(&self.local) {
                    true => {
                        info!("Selected as EXECUTOR node\n");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        self.execution(ipayload).await.unwrap();
                    }
                    false => {
                        info!("Selected as VERIFIER node, waiting for executors...\n");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn execution(&self, ipayload: IPayload) -> Result<()> {
        let prompt = ipayload.prompt.unwrap();
        let task_id = ipayload.task_id;

        let res = self.slm.converse(prompt.as_ref()).await.unwrap();

        let exec_payload = IPayload {
            stage: IStage::Verf,
            leader: ipayload.leader,
            source: self.local.clone(),
            task_id: task_id.clone(),

            prompt: Some(prompt),
            res: Some(res),
            generator: Some(self.local.clone()),
            verify_score: None,
            exec: None,
            verifiers: ipayload.verifiers,
        };

        let ipayload = IMsgType::Service(exec_payload);
        let fsub_payload = bincode::serialize(&ipayload).unwrap();

        self.p2p
            .floodsub_publish(task_id, fsub_payload)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn verification(&self, ipayload: IPayload) -> Result<()> {
        match ipayload.verifiers.unwrap().contains(&self.local) {
            true => {
                debug!("verifying...");
                let prompt = ipayload.prompt.unwrap();
                let res = ipayload.res.unwrap();
                let task_id = ipayload.task_id;

                let score = self.slm.verify(&prompt, &res).await.unwrap();

                let ver_payload = IPayload {
                    stage: IStage::Final,
                    leader: ipayload.leader,
                    source: self.local.clone(),
                    task_id: task_id.clone(),

                    prompt: Some(prompt),
                    res: Some(res),
                    generator: Some(ipayload.source),
                    verify_score: Some(score),
                    exec: None,
                    verifiers: None,
                };

                let ipayload = IMsgType::Service(ver_payload);
                let fsub_payload = bincode::serialize(&ipayload).unwrap();

                // After some jitter
                let nonce = (200..900).choose(&mut rng()).unwrap();
                tokio::time::sleep(Duration::from_millis(nonce)).await;

                self.p2p
                    .floodsub_publish(task_id, fsub_payload)
                    .await
                    .unwrap();
            }
            false => return Ok(()),
        };

        Ok(())
    }

    pub async fn finalize(
        &self,
        ipayload: Option<IPayload>,
        topic: Option<String>,
    ) -> Option<TaskWinner> {
        match ipayload.is_none() {
            false => {
                let ipayload = ipayload.unwrap();

                if ipayload.leader != self.local {
                    return None;
                }

                let source = ipayload.source;
                let generator = ipayload.generator.unwrap();
                let score: f32 = ipayload.verify_score.unwrap().parse().unwrap();
                let task_id = ipayload.task_id;

                let source_last5 = last_n_chars(&source, 5);
                let generator_last5 = last_n_chars(&generator, 5);

                info!(
                    "SCORED: (...{source_last5}) => (...{generator_last5}) - {score}: [{task_id}]"
                );

                let response = ipayload.res.unwrap();

                let mut sessions = self.sessions.lock().await;
                let task = sessions.get_mut(&task_id).unwrap();
                match task.responses.get_mut(&generator) {
                    None => {
                        task.responses.insert(generator, (response, vec![score]));
                    }
                    Some((_, scores)) => scores.push(score),
                };
                task.rtc_score_count += 1;
            }
            true => {
                let topic = topic.unwrap();
                let (prompt, responses) = {
                    let sessions = self.sessions.lock().await;
                    let task = sessions.get(&topic).unwrap();
                    (task.prompt.clone(), task.responses.clone())
                };

                let mut winner_generator = None;
                let mut winning_response = None;
                let mut highest_avg: f32 = -1.0; // Set below your strict 0.00 scoring floor

                for (generator, (res, scores)) in responses {
                    if scores.is_empty() {
                        warn!(
                            "Generator {} had no validation scores. Skipping.",
                            generator
                        );
                        continue;
                    }

                    let sum: f32 = scores.iter().sum();
                    let avg = sum / (scores.len() as f32);

                    if avg > highest_avg {
                        highest_avg = avg;
                        winner_generator = Some(generator);
                        winning_response = Some(res);
                    }
                }

                let winner = TaskWinner {
                    generator: winner_generator.unwrap(),
                    response: winning_response.unwrap(),
                    prompt,
                    score: highest_avg,
                };

                return Some(winner);
            }
        }

        None
    }

    pub async fn handle_incoming(&self, ipayload: IPayload) -> Result<()> {
        match ipayload.stage {
            IStage::Adv => self
                .adv(ipayload.task_id.clone(), Some(ipayload))
                .await
                .unwrap(),

            IStage::Exec => {
                self.ack(ipayload.task_id.clone(), Some(ipayload), None)
                    .await
                    .unwrap();
            }
            IStage::Verf => self.verification(ipayload).await.unwrap(),
            IStage::Final => {
                self.finalize(Some(ipayload), None).await;
            }
        }

        Ok(())
    }

    pub async fn pipeline(&self, provider_count: usize) -> Option<TaskWinner> {
        let task_id = generate_random();

        self.adv(task_id.clone(), None).await.unwrap();

        // TODO: WAIT FOR GETTING ENOUGH PARTIPANTS, AND A TIMEOUT
        loop {
            let mesh = self.p2p.floodsub_mesh().await.unwrap();
            let participants = mesh.get(&task_id).unwrap_or(&Vec::new()).clone();

            if participants.len() != provider_count {
                tokio::time::sleep(Duration::from_secs(3)).await;
                debug!(
                    "Waiting on PARTICIPANTS: {}/{provider_count}",
                    participants.len()
                );
                continue;
            }

            println!();
            debug!("Participants QUOTA filled, moving on to ACK...\n");
            tokio::time::sleep(Duration::from_secs(4)).await;

            break;
        }

        // Move on with ACK phase
        let prompt = "Hey hows it going, let have somemfun talk about decentralized computaion, say when a distributed swarm of LLMs".to_string();
        self.ack(task_id.clone(), None, Some(prompt)).await.unwrap();

        // Wait until we get all the scores
        let (execs, verifiers) = {
            let tasks = self.sessions.lock().await;
            let task = tasks.get(&task_id).unwrap();

            (task.execs.clone(), task.verifiers.clone())
        };
        let all_score_count = execs.len() * verifiers.len();

        loop {
            let rtc_score_count = {
                let tasks = self.sessions.lock().await;
                let task = tasks.get(&task_id).unwrap();

                task.rtc_score_count as usize
            };

            if rtc_score_count != all_score_count {
                debug!("Waiting on SCORES: {rtc_score_count}/{all_score_count}");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            println!();
            debug!("Got all the scores, finalizing the winner...\n");
            tokio::time::sleep(Duration::from_secs(2)).await;

            break;
        }

        self.finalize(None, Some(task_id)).await
    }
}
