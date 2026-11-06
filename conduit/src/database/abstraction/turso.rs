use super::{KeyValueDatabaseEngine, KvTree};
use crate::{Result, utils::error::Error};
use conduit_config::Config;
use libsql::Builder;
use std::{pin::Pin, sync::Arc};
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

        // Validate connectivity at startup
        let token = turso.auth_token.as_deref().unwrap_or("");
        let db = Builder::new_remote(turso.url.clone(), token.to_string());
        let rt = tokio::runtime::Runtime::new()
            .map_err(|_| Error::Initialization("failed to create tokio runtime"))?;
        rt.block_on(db.build())
            .map_err(|e| Error::LibSql(e.to_string()))?;
        drop(rt);

        Ok(Arc::new(Engine {
            url: turso.url.clone(),
            auth_token: turso.auth_token.clone(),
        }))
    }

    fn open_tree(&self, name: &'static str) -> Result<Arc<dyn KvTree>> {
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

/// Run an async libsql operation on a temporary Tokio runtime.
///
/// Conduit's database traits are synchronous, but libsql is async (HTTP-based).
/// We bridge the gap by creating a short-lived runtime per operation.
fn block_on<F, T>(f: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let rt = tokio::runtime::Runtime::new()
        .map_err(|_| Error::Initialization("failed to create tokio runtime"))?;
    let result = rt.block_on(f);
    drop(rt);
    result
}

fn token_str(auth_token: &Option<String>) -> String {
    auth_token.as_deref().unwrap_or("").to_string()
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
