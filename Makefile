lint:
	cargo fmt && cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all --all-features

