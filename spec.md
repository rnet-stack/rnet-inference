# rnet-inference Protocol Specification

**Version:** 0.1.0  
**Status:** Draft  
**Repository:** [rnet-stack/rnet-inference](https://github.com/rnet-stack/rnet-inference)  
**Depends on:** [rnet-stack/rnet-p2p](https://github.com/rnet-stack/rnet-p2p)

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Terminology](#2-terminology)
3. [Network Model](#3-network-model)
   - 3.1 [Transport and Security Layer](#31-transport-and-security-layer)
   - 3.2 [Pub/Sub Layer — Floodsub](#32-pubsub-layer--floodsub)
   - 3.3 [Topic Namespace](#33-topic-namespace)
4. [Node Types](#4-node-types)
   - 4.1 [Bootstrap Node](#41-bootstrap-node)
   - 4.2 [Provider Node](#42-provider-node)
5. [Wire Format](#5-wire-format)
   - 5.1 [Framing](#51-framing)
   - 5.2 [IMsgType Enum](#52-imsgtype-enum)
   - 5.3 [IPayload Struct](#53-ipayload-struct)
   - 5.4 [IStage Enum](#54-istage-enum)
6. [Mesh Formation Protocol](#6-mesh-formation-protocol)
   - 6.1 [Bootstrap Startup](#61-bootstrap-startup)
   - 6.2 [Provider Startup](#62-provider-startup)
   - 6.3 [Periodic Mesh Broadcast](#63-periodic-mesh-broadcast)
7. [Swarm Inference Protocol](#7-swarm-inference-protocol)
   - 7.1 [Session Lifecycle Overview](#71-session-lifecycle-overview)
   - 7.2 [Stage 1 — Adv (Advertise)](#72-stage-1--adv-advertise)
   - 7.3 [Stage 2 — Exec (Execute)](#73-stage-2--exec-execute)
   - 7.4 [Stage 3 — Verf (Verify)](#74-stage-3--verf-verify)
   - 7.5 [Stage 4 — Final (Finalize)](#75-stage-4--final-finalize)
8. [Role Assignment](#8-role-assignment)
9. [SLM Interface](#9-slm-interface)
   - 9.1 [Inference Call](#91-inference-call)
   - 9.2 [Verification Call](#92-verification-call)
   - 9.3 [Output Parsing](#93-output-parsing)
10. [Session State](#10-session-state)
11. [Scoring and Winner Selection](#11-scoring-and-winner-selection)
12. [Timing and Jitter](#12-timing-and-jitter)
13. [Automated Pipeline](#13-automated-pipeline)
14. [Known Limitations and Open Issues](#14-known-limitations-and-open-issues)

---

## 1. Introduction

`rnet-inference` is a peer-to-peer protocol for distributed inference across a swarm of nodes, each running a local Small Language Model (SLM). Rather than routing a prompt to a single trusted model, the protocol distributes the prompt to a set of *executor* nodes that independently generate responses, and then routes those responses to a separate set of *verifier* nodes that score them. The node with the highest average score across all verifiers is declared the winner of the inference session.

The protocol makes no use of a blockchain, an off-chain registry, or any external oracle. All coordination is done via gossip messages over a live floodsub mesh. A single *bootstrap* node maintains topology awareness; all other coordination is peer-to-peer.

The core property the protocol aims for is **redundant, scored inference**: the same prompt is answered by multiple independent models, and the outputs are evaluated by multiple independent judges, using the same underlying model as both generator and evaluator.

---

## 2. Terminology

| Term | Definition |
|---|---|
| **Bootstrap node** | A well-known node with a deterministic peer ID. Tracks the global topic→peer mesh and broadcasts it to all providers. Does not participate in inference. |
| **Provider node** | Any non-bootstrap node. Can be a session leader, an executor, or a verifier. |
| **Leader** | The provider node that initiates an inference session for a given `task_id`. |
| **Executor** | A provider node assigned to generate a response to the prompt. |
| **Verifier** | A provider node assigned to score the responses produced by executors. |
| **Session** | The full lifecycle of a single inference task, identified by a `task_id`. |
| **task_id** | A 7-character alphanumeric string uniquely identifying a session. |
| **Floodsub** | A simple broadcast pub/sub protocol. All subscribers to a topic receive every published message. |
| **PROVIDER_MESH** | The global coordination topic `"swarm/mesh"`, subscribed to by all nodes. |
| **Bootmesh** | The bootstrap node's cached view of the full `topic → [peer multiaddrs]` map. |
| **IPayload** | The envelope struct carried in every `IMsgType::Service` message. |
| **IStage** | The stage discriminant inside `IPayload`. One of `Adv`, `Exec`, `Verf`, `Final`. |

---

## 3. Network Model

### 3.1 Transport and Security Layer

All connections use the `rnet-p2p` stack, which provides:

- **TCP transport** on `ip4/127.0.0.1/tcp/<port>`
- **Noise/DH handshake** using X25519 key exchange
- **Symmetric encryption** using ChaCha20-Poly1305
- **Stream multiplexing** using yamux
- **Protocol negotiation** using multistream-select

Every connection is encrypted and multiplexed from the transport layer up. There is no unencrypted fallback.

### 3.2 Pub/Sub Layer — Floodsub

All inference coordination messages are published via **Floodsub**. Floodsub is a naive broadcast mechanism: every message published to a topic is forwarded to every subscriber of that topic who is reachable from the publisher.

Protocol identifier: `rnet/floodsub/0.0.1`

There is no message deduplication, ordering guarantee, or delivery acknowledgement in Floodsub. The inference protocol is designed to be robust to duplicate delivery within a session.

### 3.3 Topic Namespace

| Topic | Used by | Purpose |
|---|---|---|
| `swarm/mesh` | All nodes | Bootstrap publishes mesh topology; providers subscribe for peer discovery. |
| `<task_id>` | Session participants | All four inference stages (`Adv`, `Exec`, `Verf`, `Final`) for a specific session are published here. |

The `task_id` topic is created dynamically per session. A node joins it by calling `floodsub_subscribe(task_id)`.

---

## 4. Node Types

### 4.1 Bootstrap Node

The bootstrap node has a **deterministic peer ID** derived from a fixed RSA PKCS#8 private key loaded from the `BOOTSTRAP_PVT_KEY` environment variable. This means its multiaddr — for example:

```
/ip4/127.0.0.1/tcp/8888/p2p/6wrGkCFmUe2H8c23pCfPcXktZztSu2eckvqaWP6y4E2w
```

— is stable across restarts, and all provider nodes can hard-code it for initial connection.

**Startup sequence:**
1. Bind to `ip4/127.0.0.1/tcp/8888`.
2. Enable Floodsub and Ping protocols.
3. Subscribe to `swarm/mesh`.
4. Start `periodic_mesh_update()` loop.

**The bootstrap node does not process `IMsgType::Service` messages.** Its `event_handler` discards all incoming events with an early `continue` in `Mode::Bootstrap` check.

### 4.2 Provider Node

Provider nodes bind to **`ip4/127.0.0.1/tcp/0`** — an ephemeral OS-assigned port. Their peer ID is generated fresh each time (no key is persisted). This means provider identities are session-scoped; a restarted provider is a new peer.

**Startup sequence:**
1. Bind to a random local port.
2. Enable Floodsub and Ping.
3. Subscribe to `swarm/mesh`.
4. Wait 2 seconds for the transport to settle.
5. Open a Floodsub stream to the bootstrap node at `BOOTSTRAP_NODE`.
6. Wait another 2 seconds.
7. Enter the CLI event loop.

---

## 5. Wire Format

### 5.1 Framing

All Floodsub message payloads are serialized using **`bincode`** (little-endian, variable-length integers). There is no additional length-prefix or message framing at the application layer — `rnet-p2p` handles framing internally.

### 5.2 IMsgType Enum

Every message published over Floodsub is one of three variants:

```rust
pub enum IMsgType {
    General(String),
    Service(IPayload),
    Bootmesh(HashMap<String, Vec<String>>),
}
```

| Variant | Published by | Contains |
|---|---|---|
| `General` | Any node (manual CLI) | Free-form UTF-8 string. Not used by the inference protocol. |
| `Service` | Leader and providers | An `IPayload` carrying an inference session message. |
| `Bootmesh` | Bootstrap node only | Full `topic → [multiaddr strings]` map of the current mesh state. |

### 5.3 IPayload Struct

```rust
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
```

All `String` address fields are **multiaddr strings** — the full `ip4/host/tcp/port` representation of a node's listen address. Peer identity in this protocol is the listen multiaddr, not a public key fingerprint.

Field population per stage:

| Field | Adv | Exec | Verf | Final |
|---|---|---|---|---|
| `stage` | `Adv` | `Exec` | `Verf` | `Final` |
| `leader` | self | self | carried from Exec | carried from Verf |
| `source` | self | self | executor self | verifier self |
| `task_id` | topic string | topic string | topic string | topic string |
| `prompt` | `None` | set | carried | carried |
| `res` | `None` | `None` | executor's response | carried |
| `generator` | `None` | `None` | executor's maddr | carried |
| `verify_score` | `None` | `None` | `None` | verifier's score string |
| `exec` | `None` | list of executor maddrs | `None` | `None` |
| `verifiers` | `None` | list of verifier maddrs | carried | `None` |

### 5.4 IStage Enum

```rust
pub enum IStage {
    Adv,
    Exec,
    Verf,
    Final,
}
```

`IStage` is the dispatch key in `handle_incoming()`. A receiving node matches on this field to decide which handler to invoke.

---

## 6. Mesh Formation Protocol

Before any inference session can run, provider nodes need a consistent view of who else is online. This is the responsibility of the bootstrap node.

### 6.1 Bootstrap Startup

After subscribing to `swarm/mesh` and starting the protocol stack, the bootstrap node spawns `periodic_mesh_update()` as a background task.

### 6.2 Provider Startup

Each provider opens a Floodsub stream to the bootstrap node's multiaddr. This causes the provider's multiaddr to appear in the bootstrap node's internal `floodsub_mesh()` map under topic `swarm/mesh`.

### 6.3 Periodic Mesh Broadcast

The bootstrap node runs a tight poll loop:

```
loop:
  latest_mesh = floodsub_mesh()
  if latest_mesh == cached_bootmesh:
    sleep(300ms)
    continue
  
  update cached_bootmesh = latest_mesh
  broadcast_bootmesh(latest_mesh)
```

`broadcast_bootmesh` waits 2 seconds (to let newly joined nodes settle), then publishes `IMsgType::Bootmesh(latest_mesh)` to `swarm/mesh`.

Every provider that receives a `Bootmesh` message replaces its local `bootmesh` cache (an `Arc<Mutex<HashMap<String, Vec<String>>>>`) with the received map. The `bootmesh` command in the CLI exposes this cache for inspection.

**Effect:** Every provider has a near-real-time view of all other providers' multiaddrs, indexed by the topics they are subscribed to. This is used during leader session setup to know whom to reach.

---

## 7. Swarm Inference Protocol

### 7.1 Session Lifecycle Overview

```
Leader                  Provider A              Provider B
  │                         │                       │
  │── Adv ────────────────▶│                       │
  │── Adv ──────────────────────────────────────▶  │
  │                         │                       │
  │         (jitter 4-6s)   │                       │
  │                         │── subscribe(task_id)─▶│
  │◀── subscribe(task_id) ──│                       │
  │                         │                       │
  │── Exec ────────────────▶│  (assigned Executor)  │
  │── Exec ──────────────────────────────────────▶  │  (assigned Verifier)
  │                         │                       │
  │              SLM.converse(prompt)               │
  │                         │── Verf ──────────────▶│
  │                         │                       │ SLM.verify(prompt, res)
  │                         │                       │── Final ──▶ Leader
  │                         │                       │
  │  (accumulate scores, pick winner)               │
  │                         │                       │
  ▼ TaskWinner{ generator, response, score }
```

### 7.2 Stage 1 — Adv (Advertise)

**Trigger:** Leader calls `adv(task_id, None)`.

**Leader behaviour:**
1. Calls `floodsub_subscribe(task_id)` — creates the session topic.
2. Publishes `IMsgType::Service(IPayload { stage: Adv, leader: self, source: self, task_id, ... })` to `swarm/mesh`.
3. Inserts an empty `SessionStorage` into `self.sessions[task_id]`.

**Provider behaviour** (on receiving `IStage::Adv`):
1. Extracts `task_id` and `source` (leader's maddr) from the payload.
2. Sleeps for a random jitter of **4–6 seconds** (uniformly sampled from `4..6`).
3. Opens a new Floodsub stream to the leader's multiaddr.
4. Calls `floodsub_subscribe(task_id)` — joins the session topic.

After this stage, the leader's `floodsub_mesh()` for `task_id` will contain all providers who opted in.

> **Note:** The original implementation included an interactive `Y/n` prompt for provider participation. This is currently commented out; all providers auto-join.

### 7.3 Stage 2 — Exec (Execute)

**Trigger:** Leader calls `ack(task_id, None, Some(prompt))`.

**Leader behaviour:**
1. Reads `floodsub_mesh()[task_id]` — the current participant list.
2. Calls `random_split(participants)` to produce `(execs, verifiers)`. See [Section 8](#8-role-assignment).
3. Stores `execs`, `verifiers`, and `prompt` in `sessions[task_id]`.
4. Publishes `IMsgType::Service(IPayload { stage: Exec, prompt, exec, verifiers, ... })` to `task_id`.

**Provider behaviour** (on receiving `IStage::Exec`):
1. Checks `exec.contains(&self.local)`.
2. **If executor:** Sleeps 2 seconds, then calls `execution(ipayload)`.
3. **If verifier:** Logs "Selected as VERIFIER node, waiting for executors..." and sleeps 2 seconds. No further action until a `Verf` message arrives.

**`execution()` logic:**
1. Calls `slm.converse(prompt)` — blocks until the local Ollama model returns a response.
2. Publishes `IMsgType::Service(IPayload { stage: Verf, prompt, res, generator: self.local, verifiers, ... })` to `task_id`.

### 7.4 Stage 3 — Verf (Verify)

**Trigger:** Automatic — every node on `task_id` receives the `Verf` payload published by an executor.

**Provider behaviour** (on receiving `IStage::Verf`):
1. Checks `verifiers.contains(&self.local)`.
2. **If not a verifier for this session:** Returns immediately.
3. **If verifier:**
   1. Calls `slm.verify(prompt, res)` — blocks until the local model returns a score.
   2. Sleeps for a random jitter of **200–900 milliseconds** (uniformly sampled from `200..900`).
   3. Publishes `IMsgType::Service(IPayload { stage: Final, generator, verify_score, res, prompt, ... })` to `task_id`.

The jitter before publishing the `Final` message prevents all verifiers from flooding the topic simultaneously.

### 7.5 Stage 4 — Final (Finalize)

**Trigger:** Automatic — every `Final` payload arriving at the leader is processed by `finalize(Some(ipayload), None)`.

**Leader behaviour** (per incoming `Final` payload):
1. Guards: if `ipayload.leader != self.local`, discards. (Only the designated leader finalizes a session.)
2. Parses `verify_score` as `f32`.
3. Looks up `sessions[task_id].responses[generator]`:
   - If absent: inserts `(response, vec![score])`.
   - If present: appends `score` to the existing score vector.
4. Increments `sessions[task_id].rtc_score_count`.

**Finalization** (called explicitly or by `pipeline` when `rtc_score_count == execs.len() * verifiers.len()`):

`finalize(None, Some(task_id))` iterates over all `responses`:

```
for each (generator, (response, scores)):
    avg = sum(scores) / len(scores)
    if avg > highest_avg:
        winner = generator
        winning_response = response
        highest_avg = avg

return TaskWinner { generator, response, prompt, score: highest_avg }
```

Generators with no scores are skipped with a warning.

---

## 8. Role Assignment

Role assignment is done by the leader via `random_split(participants: Vec<String>)`.

```rust
pub fn random_split(v: Vec<String>) -> (Vec<String>, Vec<String>) {
    assert!(v.len() >= 2);
    let split_idx = rand::rng().random_range(1..v.len());
    let left = v[..split_idx].to_vec();   // executors
    let right = v[split_idx..].to_vec();  // verifiers
    (left, right)
}
```

`split_idx` is sampled uniformly from `[1, len)`. This means:

- `split_idx = 1` → 1 executor, `len - 1` verifiers (minimum executor case)
- `split_idx = len - 1` → `len - 1` executors, 1 verifier (minimum verifier case)
- Expected split is approximately even

The participant list is the raw output of `floodsub_mesh()[task_id]` — it preserves the order peers subscribed, which is non-deterministic. Combined with the random split index, roles are effectively random from every participant's perspective.

A node's role is **self-determined**: it receives the full `exec` and `verifiers` lists and checks `exec.contains(&self.local)`. There is no separate role assignment message.

The leader itself may appear in neither list if it is not in `floodsub_mesh()[task_id]`. This is expected — the leader manages the session but does not necessarily participate as executor or verifier.

---

## 9. SLM Interface

All inference and verification calls go through `SlmClient`, which wraps the [Ollama](https://ollama.com) HTTP API.

**Base URL:** `http://localhost:11434` (hardcoded)  
**Model:** `qwen2.5:1.5b` (hardcoded)  
**Endpoint:** `POST /api/generate`  
**Streaming:** disabled (`"stream": false`)  
**Temperature:** `0.0` for both inference and verification (deterministic outputs)

### 9.1 Inference Call

The system prompt enforces strict JSON output from the generator model:

> *"You are a high-precision inference engine operating in a strict P2P network. Your output will be evaluated by a ruthless cryptographic validator node that scores responses from 0.00 to 0.99."*

Rules enforced by the system prompt:
- No conversational filler
- Maximum conciseness and density
- No hallucinations; state directly if a prompt is nonsensical
- Output **must** be exactly `{"res": "your_dense_accurate_response"}`
- No markdown wrappers, code blocks, or trailing text

The combined prompt sent to the model is:

```
{system_prompt}

prompt: {user_prompt}

JSON Output:
```

Expected response shape: `{"res": "..."}`

### 9.2 Verification Call

The verifier system prompt enforces strict scoring:

> *"You are a highly critical evaluator of AI outputs. Your sole objective is to score the provided 'Response' based on how accurately, concisely, and completely it answers the 'Prompt'. Scale: 0.00 to 0.99."*

Scoring rules:
- **Never** award `1.0`
- Deduct for hallucination
- Score below `0.5` for mediocre responses

Combined prompt:

```
{verify_system_prompt}

Prompt: {original_prompt}

Response to evaluate: {executor_response}

JSON Output:
```

Expected response shape: `{"score": "0.XX"}`

### 9.3 Output Parsing

Both `converse` and `verify` apply the same cleanup pipeline before parsing:

1. Strip leading `` ```json `` prefix if present
2. Strip trailing ` ``` ` suffix if present
3. Trim whitespace
4. Attempt `serde_json` deserialization into `SlmRes` or `SlmVer`

**On parse failure:**
- `converse`: returns `"Invalid response for SLM"` as a string (session continues)
- `verify`: returns `"0.00"` (failing score — penalizes the executor whose response could not be evaluated)

This means a model that produces malformed JSON during verification causes the evaluated response to receive a zero score from that verifier, without halting the session.

---

## 10. Session State

The leader maintains a `sessions: Arc<Mutex<HashMap<String, SessionStorage>>>` map across all active sessions.

```rust
pub struct SessionStorage {
    pub prompt: String,
    pub winner: Option<TaskWinner>,
    pub execs: Vec<String>,
    pub verifiers: Vec<String>,
    pub rtc_score_count: u32,
    pub responses: HashMap<String, (String, Vec<f32>)>,
}
```

| Field | Set at | Description |
|---|---|---|
| `prompt` | `ack()` | The prompt string for this session |
| `winner` | `finalize()` | Populated after winner selection |
| `execs` | `ack()` | Multiaddrs of executor nodes |
| `verifiers` | `ack()` | Multiaddrs of verifier nodes |
| `rtc_score_count` | per `Final` message | Running count of scores received |
| `responses` | per `Final` message | `generator_maddr → (response_text, [scores])` |

`responses` is keyed by the **generator's multiaddr**. When two verifiers score the same executor, their scores are appended to the same entry's `Vec<f32>`.

The total expected score count is `execs.len() * verifiers.len()`. Every executor is scored by every verifier, producing a full cross-product matrix.

---

## 11. Scoring and Winner Selection

The scoring model is a **flat average** across all verifier scores for a given generator.

For a session with `E` executors and `V` verifiers:

- Each executor `e_i` receives a score `s_ij` from each verifier `v_j`
- The average score for executor `e_i` is: `avg_i = (Σ s_ij for j in 1..V) / V`
- The winner is `argmax(avg_i)`

Score values are `f32`. The scoring floor is `0.00` and the ceiling is `0.99` (enforced by the verifier system prompt). The initial `highest_avg` is set to `-1.0` to ensure even a universally zero-scored session produces a winner.

The `TaskWinner` struct returned by `finalize`:

```rust
pub struct TaskWinner {
    pub generator: String,   // winning executor's multiaddr
    pub prompt: String,      // the original prompt
    pub response: String,    // the winning response text
    pub score: f32,          // winning average score
}
```

---

## 12. Timing and Jitter

The protocol introduces deliberate delays at several points to prevent connection storms and publication floods.

| Event | Delay | Randomized | Source |
|---|---|---|---|
| Provider joining after `Adv` | 4–6 seconds | Yes, uniform | `(4..6).choose(&mut rng())` |
| Executor before calling SLM | 2 seconds | No | Fixed `tokio::time::sleep` |
| Verifier before calling SLM | 2 seconds | No | Fixed `tokio::time::sleep` |
| Verifier before publishing `Final` | 200–900ms | Yes, uniform | `(200..900).choose(&mut rng())` |
| Bootstrap before re-broadcasting mesh | 2 seconds | No | Fixed settle window |
| Bootstrap mesh poll interval (no change) | 300ms | No | Fixed poll |
| Bootstrap mesh poll interval (changed) | 2 seconds | No | Settle window before broadcast |
| Node startup settle | 500ms | No | After `NodeInner::new` |
| Post-bootstrap-connect settle | 2 seconds | No | After `new_stream` to bootstrap |
| `pipeline` participant wait poll | 3 seconds | No | Fixed retry interval |
| `pipeline` post-ACK settle | 4 seconds | No | Fixed |
| `pipeline` post-scores settle | 2 seconds | No | Fixed |

---

## 13. Automated Pipeline

`IService::pipeline(provider_count)` drives a full inference session end-to-end without manual CLI intervention.

```
1. Generate a random 7-char task_id
2. Call adv(task_id, None)   -- advertise to the mesh
3. Loop every 3s:
     participants = floodsub_mesh()[task_id]
     if len(participants) == provider_count: break
4. Sleep 4s (settle)
5. Call ack(task_id, None, Some(prompt))   -- assign roles, start execution
6. Compute all_score_count = execs.len() * verifiers.len()
7. Loop every 2s:
     if sessions[task_id].rtc_score_count == all_score_count: break
8. Sleep 2s
9. Call finalize(None, Some(task_id))
10. Return Some(TaskWinner)
```

**Current limitations of `pipeline`:**
- The participant wait loop (step 3) has no timeout — it will block indefinitely if fewer than `provider_count` nodes join.
- The hardcoded prompt `"Hey hows it going, let have somemfun talk about decentralized computaion..."` is a placeholder; there is no API to pass a custom prompt through `pipeline` yet.
- Steps 3 and 7 are polling loops, not event-driven waits.

---

## 14. Known Limitations and Open Issues

**No timeout on session participation.** If a provider advertises a session and fewer than `provider_count` nodes join, `pipeline` polls indefinitely. Manual `adv`/`ack`/`finalize` flows also have no deadline.

**No executor fault handling.** If an executor's SLM call hangs, the `Verf` message is never published, and the leader's score count never reaches `all_score_count`. The session stalls.

**No verifier fault handling.** If a verifier's SLM fails and returns `"0.00"`, it is indistinguishable from a genuine low score. There is no way to detect a non-responding verifier vs. a legitimately bad response.

**`random_split` bias.** The split index is sampled from `[1, len)`. With small participant counts (e.g., 2 nodes), the split is always `1 executor, 1 verifier`. The executor and the verifier are effectively the same node scoring itself, which defeats the independence property.

**Identity is the listen multiaddr.** There are no cryptographic identities for provider nodes. A node's identity is its `ip4/host/tcp/port` string. This is trivially spoofable and changes on restart.

**Hardcoded Ollama endpoint.** The model URL `http://localhost:11434` is set in `SlmClient::new` and cannot be configured per-node without code changes.

**No message authentication.** `IMsgType::Service` payloads carry no signature. Any node on the `task_id` topic can publish a forged `Final` message claiming an inflated score for any generator.

**Single topic for all session traffic.** All four stages (`Adv`, `Exec`, `Verf`, `Final`) share the `task_id` topic. There is no topic separation between "broadcast to all" messages and "leader-only" messages.

**Participant list order is non-deterministic.** `floodsub_mesh()[task_id]` returns the peer list in internal hashmap order. `random_split` operates on this list, so the executor/verifier boundary also depends on the internal ordering.

**`SessionStorage.winner` is never set.** The `finalize` function returns a `TaskWinner` but never writes it back into `sessions[task_id].winner`. This field is a dead slot in the current implementation.