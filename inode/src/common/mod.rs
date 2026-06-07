use rand::{distr::Alphanumeric, rng, RngExt};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn unix_epoch() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();

    duration.as_secs()
}

pub fn generate_entropy() -> (String, String) {
    let secret: String = rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();

    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let hash = hex::encode(hasher.finalize());

    (secret, hash)
}

pub fn generate_random() -> String {
    let secret: String = rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();

    secret
}

pub fn random_split(v: Vec<String>) -> (Vec<String>, Vec<String>) {
    assert!(v.len() >= 2, "Vector must contain at least 2 elements");

    let split_idx = rand::rng().random_range(1..v.len());

    let left = v[..split_idx].to_vec();
    let right = v[split_idx..].to_vec();

    (left, right)
}

pub fn last_n_chars(s: &str, n: usize) -> String {
    s.chars()
        .rev()
        .take(n)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}
