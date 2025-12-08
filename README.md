<p align="center">
  <img src="https://i.imgur.com/TzcVUYs.png" alt="Rac-delta logo" width="140"/>
  <img src="https://rust-lang.org/logos/rust-logo-512x512.png" alt="node logo" width="110"/>
</p>

---

# rac-delta 🦝

rac-delta is an open delta-patching protocol made for sync builds of your apps, games, or anything you store in a folder!

This is the official SDK for Rust.

## Benefits of rac-delta

- **Backend agnostic**: it does not depend on a concrete service (S3, Azure, SSH, signed URLs, etc.)
- **Modular**: almost any remote storage can be used via adapters
- **Simple**: an unique index archive (rdindex.json) centralices all information.
- **Efficiency**: supports streaming, concurrency, and uses **Blake3** for fast hashing.
- **Flexibility**: highly customizable, chunk size, concurrency limits, reconstruction strategies...

---

## Installation

```
cargo add rac-delta
```

## Basic usage

In order to use the rac-delta SDK, you will need to create a RacDeltaClient, the main entry point of the SDK and where all the rac-delta operations are invoked.

```rust
  use rac_delta::{
    BaseStorageConfig, RacDeltaClient, RacDeltaConfig, SSHCredentials, SSHStorageConfig,
    StorageConfig,
  };

  let config = RacDeltaConfig {
      chunk_size: 1024 * 1024,
      max_concurrency: Some(6),
      storage: StorageConfig::SSH(SSHStorageConfig {
          base: BaseStorageConfig {
              path_prefix: Some("/root/upload".to_string()),
          },
          host: "localhost".to_string(),
          port: Some(2222),
          credentials: SSHCredentials {
              username: "root".to_string(),
              password: Some("password".to_string()),
              private_key: None,
          },
      }),
  };

  let client = RacDeltaClient::new(config);
```

And a example to perform a upload to the selected storage (SSH in this case)

```rust
  let remote_index_to_use: Option<RDIndex> = None;

  match client.pipelines.upload {
      UploadPipelineBundle::Hash(pipeline) => {
          pipeline
              .execute(
                  Path::new("my/dir"),
                  remote_index_to_use,
                  Some(UploadOptions {
                      require_remote_index: Some(false),
                      force: Some(false),
                      ignore_patterns: None,
                      on_state_change: Some(std::sync::Arc::new(|state| {
                          println!("Upload state: {:?}", state);
                      })),
                      on_progress: Some(std::sync::Arc::new(|phase, progress, speed| {
                          println!(
                              "Phase: {:?}, progress: {:.1}%, speed: {}",
                              phase,
                              progress * 100.0,
                              speed
                                  .map_or("unknown".to_string(), |s| format!("{:.1} bytes/s", s))
                          );
                      })),
                  }),
              )
              .await?;
      }
      UploadPipelineBundle::Url(_p) => {
          // none for SSH
      }
  }
```

## Documentation

For all the details, check the [full documentation](https://raccreative.github.io/rac-delta-docs/).

Available in spanish too!

---

## Contributing

Contributions are welcome!

1. Fork the repository.
2. Create a branch for your feature/fix (`git checkout -b feature/new-feature`).
3. Commit your changes (`git commit -m 'Added new feature'`).
4. Push to your branch (`git push origin feature/new-feature`).
5. Open a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).
