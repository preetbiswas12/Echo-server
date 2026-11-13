use super::{KeyValueDatabaseEngine, KvTree};
use crate::{Result, utils::error::Error};
use conduit_config::Config;
use libsql::Builder;
use std::{future::Future, pin::Pin, sync::Arc};
use tracing::debug;

type TupleOfBytes = (Vec<u8>, Vec<u8>);

pub struct Engine {
    pub url: String,
    pub auth_token: Option<String>,
}

impl KeyValueDatabaseEngine for Arc<Engine> {
    fn open(config: &Config) -> Result<Self> {
        let turso = config.turso.as_ref().ok_or_else(|| {
            Error::Initialization(
                "Turso backend selected but no [global.turso] config section found",
            )
        })?;

        let engine = Engine {
            url: turso.url.clone(),
            auth_token: turso.auth_token.clone(),
        };

        // Validate connectivity and ensure auxiliary tables exist (e.g. media chunk storage).
        engine.ensure_aux_tables()?;

        Ok(Arc::new(engine))
    }

    fn open_tree(&self, name: &'static str) -> Result<Arc<dyn KvTree>> {
        ensure_kv_table(&self.url, &self.auth_token, name)?;
        Ok(Arc::new(TursoTable {
            url: self.url.clone(),
            auth_token: self.auth_token.clone(),
            name: name.to_owned(),
        }))
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn cleanup(&self) -> Result<()> {
        Ok(())
    }
}

/// Run an async libsql operation from synchronous Conduit DB traits.
///
/// When called inside an existing Tokio runtime (normal startup/request path),
/// use `block_in_place` + the current handle to avoid "runtime within runtime"
/// panics. Otherwise (e.g. isolated contexts), create a temporary runtime.
fn block_on<F, T>(f: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(f))
    } else {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|_| Error::Initialization("failed to create tokio runtime"))?;
        let result = rt.block_on(f);
        drop(rt);
        result
    }
}

fn token_str(auth_token: &Option<String>) -> String {
    auth_token.as_deref().unwrap_or("").to_string()
}

fn is_safe_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn ensure_kv_table(url: &str, auth_token: &Option<String>, name: &str) -> Result<()> {
    if !is_safe_identifier(name) {
        return Err(Error::Initialization("invalid Turso table name"));
    }
    let url = url.to_owned();
    let token = token_str(auth_token);
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{name}\" (\"key\" BLOB PRIMARY KEY, \"value\" BLOB NOT NULL)"
    );
    block_on(async move {
        let db = Builder::new_remote(url, token)
            .build()
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
        let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
        conn.execute(&sql, libsql::params![])
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
        Ok(())
    })
}

impl Engine {
    fn ensure_aux_tables(&self) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        block_on(async move {
            let db = Builder::new_remote(url, token)
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS media_chunks (file_id TEXT NOT NULL, chunk_index INTEGER NOT NULL, chunk_data BLOB NOT NULL, chunk_hash BLOB, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), PRIMARY KEY (file_id, chunk_index))",
                libsql::params![],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_chunks_created_at ON media_chunks(created_at)",
                libsql::params![],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;

            // Databases created before the chunked media backend existed have a
            // media_chunks table without the chunk_hash column; add it if missing.
            let mut cursor = conn
                .query("PRAGMA table_info(media_chunks)", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let mut has_chunk_hash = false;
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?
            {
                let name: String = row.get(1).map_err(|e| Error::LibSql(e.to_string()))?;
                if name == "chunk_hash" {
                    has_chunk_hash = true;
                }
            }
            drop(cursor);

            if !has_chunk_hash {
                conn.execute(
                    "ALTER TABLE media_chunks ADD COLUMN chunk_hash BLOB",
                    libsql::params![],
                )
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            }

            Ok(())
        })
    }
}

pub struct TursoTable {
    pub url: String,
    pub auth_token: Option<String>,
    pub name: String,
}

impl KvTree for TursoTable {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let key = key.to_vec();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let mut cursor = conn
                .query(
                    &format!("SELECT value FROM {} WHERE key = ?1", name),
                    libsql::params![key],
                )
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            match cursor.next().await.map_err(|e| Error::LibSql(e.to_string()))? {
                Some(row) => {
                    let value: Vec<u8> = row
                        .get(0)
                        .map_err(|e| Error::LibSql(e.to_string()))?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        })
    }

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute(
                &format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)", name),
                libsql::params![key, value],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
            Ok(())
        })
    }

    fn insert_batch<'a>(
        &self,
        iter: &mut dyn Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;

            conn.execute("BEGIN", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            for (key, value) in pairs {
                conn.execute(
                    &format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)", name),
                    libsql::params![key, value],
                )
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            }
            conn.execute("COMMIT", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            Ok(())
        })
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let key = key.to_vec();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute(
                &format!("DELETE FROM {} WHERE key = ?1", name),
                libsql::params![key],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
            Ok(())
        })
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = TupleOfBytes> + 'a> {
        // Fetch all rows upfront into a Vec, then return a standard iterator.
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();

        let rows = block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let mut cursor = conn
                .query(
                    &format!("SELECT key, value FROM {} ORDER BY key ASC", name),
                    libsql::params![],
                )
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let mut rows = Vec::new();
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?
            {
                let key: Vec<u8> = row
                    .get(0)
                    .map_err(|e| Error::LibSql(e.to_string()))?;
                let value: Vec<u8> = row
                    .get(1)
                    .map_err(|e| Error::LibSql(e.to_string()))?;
                rows.push((key, value));
            }
            Ok::<Vec<TupleOfBytes>, Error>(rows)
        });

        match rows {
            Ok(rows) => Box::new(rows.into_iter()),
            Err(e) => {
                debug!("turso iter error: {e}");
                Box::new(Vec::<TupleOfBytes>::new().into_iter())
            }
        }
    }

    fn iter_from<'a>(
        &'a self,
        from: &[u8],
        backwards: bool,
    ) -> Box<dyn Iterator<Item = TupleOfBytes> + 'a> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let from = from.to_vec();

        let rows = block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;

            let sql = if backwards {
                format!(
                    "SELECT key, value FROM {} WHERE key <= ?1 ORDER BY key DESC",
                    name
                )
            } else {
                format!(
                    "SELECT key, value FROM {} WHERE key >= ?1 ORDER BY key ASC",
                    name
                )
            };

            let mut cursor = conn
                .query(&sql, libsql::params![from])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let mut rows = Vec::new();
            while let Some(row) = cursor
                .next()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?
            {
                let key: Vec<u8> = row
                    .get(0)
                    .map_err(|e| Error::LibSql(e.to_string()))?;
                let value: Vec<u8> = row
                    .get(1)
                    .map_err(|e| Error::LibSql(e.to_string()))?;
                rows.push((key, value));
            }
            Ok::<Vec<TupleOfBytes>, Error>(rows)
        });

        match rows {
            Ok(rows) => Box::new(rows.into_iter()),
            Err(e) => {
                debug!("turso iter_from error: {e}");
                Box::new(Vec::<TupleOfBytes>::new().into_iter())
            }
        }
    }

    fn increment(&self, key: &[u8]) -> Result<Vec<u8>> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let key = key.to_vec();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;

            conn.execute("BEGIN", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;

            let old: Option<Vec<u8>> = {
                let mut cursor = conn
                    .query(
                        &format!("SELECT value FROM {} WHERE key = ?1", name),
                        libsql::params![key.clone()],
                    )
                    .await
                    .map_err(|e| Error::LibSql(e.to_string()))?;
                match cursor.next().await.map_err(|e| Error::LibSql(e.to_string()))? {
                    Some(row) => Some(
                        row.get(0)
                            .map_err(|e| Error::LibSql(e.to_string()))?,
                    ),
                    None => None,
                }
            };

            let new = crate::utils::increment(old.as_deref())
                .expect("utils::increment always returns Some");

            conn.execute(
                &format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)", name),
                libsql::params![key, new.clone()],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;

            conn.execute("COMMIT", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;

            Ok(new)
        })
    }

    fn increment_batch<'a>(&self, iter: &mut dyn Iterator<Item = Vec<u8>>) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();
        let keys: Vec<Vec<u8>> = iter.collect();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;

            conn.execute("BEGIN", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;

            for key in keys {
                let old: Option<Vec<u8>> = {
                    let mut cursor = conn
                        .query(
                            &format!("SELECT value FROM {} WHERE key = ?1", name),
                            libsql::params![key.clone()],
                        )
                        .await
                        .map_err(|e| Error::LibSql(e.to_string()))?;
                    match cursor.next().await.map_err(|e| Error::LibSql(e.to_string()))? {
                        Some(row) => Some(
                            row.get(0)
                                .map_err(|e| Error::LibSql(e.to_string()))?,
                        ),
                        None => None,
                    }
                };
                let new = crate::utils::increment(old.as_deref())
                    .expect("utils::increment always returns Some");

                conn.execute(
                    &format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)", name),
                    libsql::params![key, new],
                )
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            }

            conn.execute("COMMIT", libsql::params![])
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;

            Ok(())
        })
    }

    fn scan_prefix<'a>(
        &'a self,
        prefix: Vec<u8>,
    ) -> Box<dyn Iterator<Item = TupleOfBytes> + 'a> {
        Box::new(
            self.iter_from(&prefix, false)
                .take_while(move |(key, _)| key.starts_with(&prefix)),
        )
    }

    fn watch_prefix<'a>(&'a self, _prefix: &[u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        // Turso over HTTP does not support change feeds. Return a future that
        // resolves immediately — cache invalidation via watch is a no-op.
        Box::pin(async {})
    }

    fn clear(&self) -> Result<()> {
        let url = self.url.clone();
        let token = token_str(&self.auth_token);
        let name = self.name.clone();

        block_on(async move {
            let db = Builder::new_remote(url, token.clone())
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))?;
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute(
                &format!("DELETE FROM {}", name),
                libsql::params![],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;
            Ok(())
        })
    }
}
