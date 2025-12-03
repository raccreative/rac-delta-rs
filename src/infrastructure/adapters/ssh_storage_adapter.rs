#[cfg(feature = "ssh")]
pub mod ssh_adapter {
    use std::path::Path;

    use async_ssh2_lite::{AsyncSession, AsyncSftp};

    use async_trait::async_trait;

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::sync::Mutex;
    use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

    use crate::{
        BlobInfo, HashStorageAdapter, PutChunkOptions, RDIndex, SSHStorageConfig, StorageAdapter,
        StorageError,
    };

    pub struct SSHStorageAdapter {
        config: SSHStorageConfig,
        session: Mutex<Option<AsyncSession<TcpStream>>>,
    }

    impl SSHStorageAdapter {
        pub fn new(config: SSHStorageConfig) -> Self {
            Self {
                config,
                session: Mutex::new(None),
            }
        }

        fn resolve_chunk_path(&self, hash: &str) -> String {
            self.config.base.path_prefix.as_ref().map_or_else(
                || format!("chunks/{}", hash),
                |prefix| {
                    let p = prefix.trim_end_matches('/');
                    if p.is_empty() {
                        format!("chunks/{}", hash)
                    } else {
                        format!("{}/chunks/{}", p, hash)
                    }
                },
            )
        }

        fn resolve_index_path(&self) -> String {
            self.config.base.path_prefix.as_ref().map_or_else(
                || "rd-index.json".to_string(),
                |p| format!("{}/rd-index.json", p),
            )
        }

        async fn connect(&self) -> Result<AsyncSession<TcpStream>, StorageError> {
            let mut lock = self.session.lock().await;

            if let Some(session) = &*lock {
                return Ok(session.clone());
            }

            let addr = format!("{}:{}", self.config.host, self.config.port.unwrap_or(22));
            let tcp = TcpStream::connect(addr).await?;
            let mut session =
                AsyncSession::new(tcp, None).map_err(|e| StorageError::Other(e.to_string()))?;

            session
                .handshake()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if let Some(ref pk_path) = self.config.credentials.private_key {
                session
                    .userauth_pubkey_file(
                        &self.config.credentials.username,
                        None,
                        std::path::Path::new(pk_path),
                        self.config.credentials.password.as_deref(),
                    )
                    .await
                    .map_err(|e| StorageError::Other(e.to_string()))?;
            } else {
                session
                    .userauth_password(
                        &self.config.credentials.username,
                        self.config.credentials.password.as_deref().unwrap_or(""),
                    )
                    .await
                    .map_err(|e| StorageError::Other(e.to_string()))?;
            }

            if !session.authenticated() {
                return Err(StorageError::Other("SSH authentication failed".to_string()));
            }

            *lock = Some(session.clone());
            Ok(session)
        }

        async fn get_sftp(&self) -> Result<AsyncSftp<TcpStream>, StorageError> {
            let session = self.connect().await?;
            let sftp = session
                .sftp()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(sftp)
        }

        async fn ensure_dir(
            &self,
            sftp: &AsyncSftp<TcpStream>,
            dir: &str,
        ) -> Result<(), StorageError> {
            let parts: Vec<&str> = dir.split('/').filter(|p| !p.is_empty()).collect();
            let mut current = String::new();

            for part in parts {
                current.push('/');
                current.push_str(part);

                let path = Path::new(&current);
                if sftp.stat(path).await.is_err() {
                    // 0o755 = rwxr-xr-x
                    sftp.mkdir(path, 0o755)
                        .await
                        .map_err(|e| StorageError::Io(std::io::Error::from(e)))?;
                }
            }

            Ok(())
        }

        async fn read_stream_to_string<R: AsyncRead + Unpin>(
            &self,
            mut reader: R,
        ) -> Result<String, StorageError> {
            let mut buf = String::new();

            reader.read_to_string(&mut buf).await?;

            Ok(buf)
        }
    }

    #[async_trait]
    impl StorageAdapter for SSHStorageAdapter {
        async fn dispose(&self) {
            let mut lock = self.session.lock().await;

            if let Some(session) = lock.take() {
                let _ = session.disconnect(None, "Dispose", None);
            }
        }
    }

    #[async_trait]
    impl HashStorageAdapter for SSHStorageAdapter {
        async fn get_chunk(
            &self,
            hash: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError> {
            let sftp = self.get_sftp().await?;
            let path_string = self.resolve_chunk_path(hash);

            let path = Path::new(&path_string);

            match sftp.open(&path).await {
                Ok(file) => Ok(Some(Box::new(file.compat()))),
                Err(_) => Ok(None),
            }
        }

        async fn put_chunk(
            &self,
            hash: &str,
            mut data: Box<dyn AsyncRead + Send + Unpin + 'static>,
            _opts: Option<PutChunkOptions>,
        ) -> Result<(), StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_chunk_path(hash);
            let dir = path_string
                .rsplit('/')
                .skip(1)
                .collect::<Vec<_>>()
                .join("/");

            self.ensure_dir(&sftp, &dir).await?;

            let path = Path::new(&path_string);

            let file = sftp
                .create(&path)
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let mut writer = file.compat_write();
            tokio::io::copy(&mut data, &mut writer).await?;

            Ok(())
        }

        async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_chunk_path(hash);
            let path = Path::new(&path_string);

            Ok(sftp.stat(&path).await.is_ok())
        }

        async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_chunk_path(hash);
            let path = Path::new(&path_string);

            match sftp.unlink(&path).await {
                Ok(_) | Err(_) => Ok(()), // ignore ENOENT
            }
        }

        async fn list_chunks(&self) -> Result<Option<Vec<String>>, StorageError> {
            let sftp = self.get_sftp().await?;

            let prefix = format!(
                "{}/chunks",
                self.config.base.path_prefix.clone().unwrap_or_default()
            );

            let final_prefix = Path::new(&prefix);
            let files = sftp
                .readdir(&final_prefix)
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let names = files
                .into_iter()
                .filter_map(|(f, _)| f.to_str().map(|s| s.to_string()))
                .collect::<Vec<_>>();

            Ok(Some(names))
        }

        async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_chunk_path(hash);
            let path = Path::new(&path_string);

            match sftp.stat(&path).await {
                Ok(stat) => Ok(Some(BlobInfo {
                    hash: hash.to_string(),
                    size: stat.size.unwrap_or(0),
                    modified: stat.mtime.map(|t| t as u64),
                    metadata: None,
                })),
                Err(_) => Ok(None),
            }
        }

        async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_index_path();
            let path = Path::new(&path_string);

            match sftp.open(&path).await {
                Ok(file) => {
                    let reader = BufReader::new(file.compat());
                    let s = self.read_stream_to_string(reader).await?;
                    let index: RDIndex = serde_json::from_str(&s)?;
                    Ok(Some(index))
                }
                Err(_) => Ok(None),
            }
        }

        async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
            let sftp = self.get_sftp().await?;

            let path_string = self.resolve_index_path();
            let dir = path_string
                .rsplit('/')
                .skip(1)
                .collect::<Vec<_>>()
                .join("/");

            self.ensure_dir(&sftp, &dir).await?;

            let path = Path::new(&path_string);

            let file = sftp
                .create(&path)
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let mut compat_file = file.compat_write();

            let data = serde_json::to_string_pretty(&index)?;
            compat_file.write_all(data.as_bytes()).await?;
            compat_file.flush().await?;

            Ok(())
        }
    }
}
