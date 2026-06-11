# rnet-inference

> A peer-to-peer swarm inference engine for Small Language Models, built on top of [`rnet-p2p`](https://github.com/rnet-stack/rnet-p2p).

`rnet-inference` turns a cluster of independent nodes — each running its own SLM — into a self-organizing inference swarm. A prompt is broadcast over a live floodsub mesh, a subset of nodes race to generate responses, a separate subset verifies and scores every response, and the node with the highest average score wins. No central coordinator. No trusted oracle. Just signed messages, gossip, and math.

---

## Overview

`rnet-inference` answers a concrete question: _can a swarm of small, cheap models produce more trustworthy outputs than a single large one, when each model both generates and judges?_

The approach is deliberately minimal — no blockchain, no off-chain registry, no external oracle. Consensus emerges from the mesh itself: nodes self-select into executor or verifier roles, executors race to generate, verifiers score every response, and the leader tallies average scores to pick a winner. The entire session is coordinated over floodsub with bincode-serialized payloads, riding the same transport and multiplexing layers as the underlying `rnet-p2p` stack.

---
### SLM swarm inference, with 3 executor nodes and 1 verifier node

https://github.com/user-attachments/assets/fcdcabbf-6ceb-496a-92d5-7be3a23beeeb

---

## How It Works

A session moves through four stages, all coordinated over a live P2P mesh:

**1. Advertise** — A leader node broadcasts a task to the swarm. Other nodes see it and join in.

**2. Execute** — Joined nodes are randomly split into two groups: _executors_ (who generate a response) and _verifiers_ (who will score those responses). Every executor runs the prompt through its local model and broadcasts the result.

**3. Verify** — Each verifier scores every response it receives on a scale of `0.00` to `0.99`, using the same local model as a judge. Scores are broadcast back to the leader.

**4. Finalize** — The leader averages every score per generator and picks the winner — the node whose response earned the highest average across all verifiers.

No trust required. The swarm self-organizes, and consensus comes from the scores.

---

## Node Roles

There are two types of nodes:

**Bootstrap node** — runs on a fixed address, tracks who's online, and keeps the swarm's peer list in sync. Doesn't participate in inference itself.

**Provider nodes** — the workers. Each one connects to the bootstrap node on startup, then can act as a leader, executor, or verifier depending on the session.

---

## Project Structure

```
rnet-inference/
├── inode/          # Core library — P2P node, inference logic, SLM client
├── examples/
│   ├── raw/        # Runnable binary with interactive CLI
│   └── agentic/    # Autonomous agent mode (in progress)
└── logs/
```

### Configuration

Copy `.env.example` to `.env`. The file contains a pre-generated RSA private key for the bootstrap node and its expected multiaddr:

```env
BOOTSTRAP_PVT_KEY=<rsa_pkcs8_hex>
BOOTSTRAP_NODE=/ip4/127.0.0.1/tcp/8888/p2p/6wrGkCFmUe2H8c23pCfPcXktZztSu2eckvqaWP6y4E2w
```

Provider nodes read `BOOTSTRAP_NODE` on startup to connect to the mesh. The bootstrap node reads `BOOTSTRAP_PVT_KEY` so its peer ID is deterministic and matches the multiaddr above.

#### Setting up local LLM

```
docker run -d --gpus=all -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
docker exec -it ollama ollama run qwen2.5:1.5b


# Test it out via this command
curl http://localhost:11434/api/generate -d '{
  "model": "qwen2.5:1.5b",
  "prompt": "How are you doing",
  "stream": false
}'
```

### Running a Bootstrap Node

```sh
cargo run --bin raw -- bootstrap
```

The bootstrap node will bind to port `8888` and begin tracking the mesh.

### Running Provider Nodes

Open a new terminal for each provider (they bind to ephemeral ports):

```sh
# Terminal 2
cargo run --example raw

# Terminal 3
cargo run --example raw

# Terminal 4
cargo run --example raw
```

Each provider connects to the bootstrap node and subscribes to `swarm/mesh`. After a 2-second settle window, the node drops into the interactive CLI.

### Interactive CLI

```
Command => help

      help                       => print all the commands
      local                      => get local peer-info
      connect <maddr>            => connect with a new peer
      ping <maddr> <count>       => exchange ping with a peer
      peers                      => list the connected peers

      fsub <maddr>               => open a new floodsub stream with the peer
      join <topic>               => subscribe to a new-topic
      leave <topic>              => unsubscribe to a new-topic
      publish <topic> <msg>      => publish a msg to a topic
      topics                     => list the subscribed topics
      fpeers                     => list the connected Floodsub peers
      bootmesh                   => map of topics -> peer (BOOTSTRAP)
      mesh                       => map of topics -> peer

      slm                        => converse with the AI
      adv <topic>                => advertize a exec/verify session
      ack <topic>                => acknowlege the EXECS/VERIFIERS
      finalize <topic>           => finalize the winner response

      pipe <provider_count>      => Test out the automated pipeline
```

**Manual session example** (run from one of the provider nodes):

```sh
# 1. Start advertising a session
Command => adv my-task-01

# 2. Wait for peers to join, then acknowledge and assign roles
Command => ack my-task-01

# 3. After all verifiers have scored, finalize
Command => finalize my-task-01
```

**Local SLM test:**

```sh
Command => slm What is the Byzantine Generals Problem?
```

### Automated Pipeline

The `pipe` command runs a full end-to-end session programmatically — no manual `ack` or `finalize` needed. It blocks until the expected number of providers join, then drives the full four-stage protocol and prints the winning response.

```sh
# Run from the leader node; expects 3 other provider nodes to be online
Command => pipe 3
```

The pipeline waits for `provider_count` participants to appear in the task's mesh slice, assigns roles, waits for `execs.len() × verifiers.len()` scores to arrive, then selects and prints the winner.

---

## Roadmap

The `logs/chores.md` and the `agentic` example stub point at active work-in-progress:

- **Agentic mode** — autonomous provider nodes that join, bid on, and execute sessions without manual CLI interaction.
- **Agent tools** (`inode/src/agent/tools.rs`) — tool-use scaffold for executor nodes to call external APIs during generation.
- **Timeout handling** — `pipeline()` currently loops forever waiting for participants. A configurable deadline with graceful degradation is planned.
- **Dynamic Ollama endpoint** — model URL is currently hardcoded to `http://localhost:11434`; per-node configuration is on the list.
- **Stake / reputation weighting** — scoring is currently a flat average. A weighted scheme that discounts outlier verifiers over time would improve robustness.
