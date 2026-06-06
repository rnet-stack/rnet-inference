use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Deserialize, Serialize)]
pub struct SlmRes {
    pub res: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SlmVer {
    pub score: String,
}

pub struct SlmClient {
    pub url: String,
    pub sys_prompt: String,
}

impl SlmClient {
    pub fn new(url: String) -> Self {
        let sys_prompt = r#"You are a high-precision inference engine operating in a strict P2P network.
Your output will be evaluated by a ruthless cryptographic validator node that scores responses from 0.00 to 0.99. 

To maximize your score, you MUST adhere to these rules:
1. Zero Fluff: Never use conversational filler (e.g., "Here is the answer", "Sure!").
2. High Density: Answer the prompt accurately and comprehensively, but with maximum conciseness.
3. No Hallucinations: Stick strictly to known facts. If a prompt is nonsensical, state that directly.

We will converse in a strict JSON format. 
You will receive the prompt as: {"prompt": "user_input"}
You MUST respond exactly as: {"res": "your_dense_accurate_response"}

Do not include markdown blocks, code wrappers, or any trailing text outside the raw JSON."#.to_string();

        Self { url, sys_prompt }
    }

    pub async fn converse(&self, prompt: &str) -> Result<String> {
        let client = Client::new();
        let combined_prompt = format!("{}\n\nprompt: {}\n\nJSON Output:", self.sys_prompt, prompt);

        let payload = serde_json::json!({
            "model": "qwen2.5:1.5b".to_string(),
            "prompt": combined_prompt,
            "stream": false,
            "options": {
                "temperature": 0.0 // Eliminate non-deterministic randomness
            }
        });

        let response = client
            .post(format!("{}/api/generate", self.url))
            .json(&payload)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        let raw_text = body["response"].as_str().unwrap_or("").trim();

        let clean_json = raw_text
            .strip_prefix("```json")
            .unwrap_or(raw_text)
            .strip_suffix("```")
            .unwrap_or(raw_text)
            .trim();

        let res: SlmRes = match serde_json::from_str(clean_json) {
            Ok(parsed) => parsed,
            Err(_) => {
                warn!("Model generated invalid JSON: {}", raw_text);
                return Ok("Invalid response for SLM".to_string());
            }
        };

        Ok(res.res)
    }

    pub async fn verify(&self, prompt: &str, res: &str) -> Result<String> {
        let client = Client::new();

        // Hyper-strict system prompt tailored for validation
        let verify_sys_prompt = "You are a highly critical evaluator of AI outputs. 
            Your sole objective is to score the provided 'Response' based on how accurately, concisely, and completely it answers the 'Prompt'.
            Scale: 0.00 to 0.99. 
            Rules:
            1. NEVER award a 1.0 or 1.
            2. Deduct points for hallucination. 
            3. If the response is mediocre, score it below 0.5.

            You must output EXACTLY and ONLY valid JSON matching this schema:
            {\"score\": \"0.XX\"}
            Do not include markdown blocks, text wrappers, explanations, or any trailing characters outside the raw JSON structure.";

        let combined_prompt = format!(
            "{}\n\nPrompt: {}\n\nResponse to evaluate: {}\n\nJSON Output:",
            verify_sys_prompt, prompt, res
        );

        let payload = serde_json::json!({
            "model": "qwen2.5:1.5b".to_string(), // Assuming same model for eval
            "prompt": combined_prompt,
            "stream": false,
            "options": {
                "temperature": 0.0 // Ensure deterministic scoring
            }
        });

        let response = client
            .post(format!("{}/api/generate", self.url))
            .json(&payload)
            .send()
            .await?;

        let body: serde_json::Value = response.json().await?;
        let raw_text = body["response"].as_str().unwrap_or("").trim();

        // Strip markdown wrappers if the model ignores the strict prompt
        let clean_json = raw_text
            .strip_prefix("```json")
            .unwrap_or(raw_text)
            .strip_suffix("```")
            .unwrap_or(raw_text)
            .trim();

        let ver: SlmVer = match serde_json::from_str(clean_json) {
            Ok(parsed) => parsed,
            Err(_) => {
                warn!(
                    "Model generated invalid JSON during verification: {}",
                    raw_text
                );
                // Default to a failing score if the validator node fails to follow schema constraints
                return Ok("0.00".to_string());
            }
        };

        Ok(ver.score)
    }
}
