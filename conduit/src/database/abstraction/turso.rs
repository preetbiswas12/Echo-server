use super::{KeyValueDatabaseEngine, KvTree, watchers::Watchers};
use crate::{Result, utils::error::Error};
use conduit_config::Config;
use libsql::Builder;
use std::{fmt::Write as _, future::Future, pin::Pin, sync::Arc, time::Duration};
use tracing::debug;

type TupleOfBytes = (Vec<u8>, Vec<u8>);

pub struct Engine {
    pub url: String,
    pub auth_token: Option<String>,
    /// The shared remote database: one pooled HTTP client, so statements reuse
    /// keep-alive connections instead of paying a fresh DNS lookup and TLS
    /// handshake per statement.
    pub db: Arc<libsql::Database>,
}

impl KeyValueDatabaseEngine for Arc<Engine> {
    fn open(config: &Config) -> Result<Self> {
        let turso = config.turso.as_ref().ok_or_else(|| {
            Error::Initialization(
                "Turso backend selected but no [global.turso] config section found",
            )
        })?;

        let url = turso.url.clone();
        let auth_token = turso.auth_token.clone();
        let db = block_on(async {
            Builder::new_remote(url.clone(), token_str(&auth_token))
                .build()
                .await
                .map_err(|e| Error::LibSql(e.to_string()))
        })?;

        let engine = Engine {
            url,
            auth_token,
            db: Arc::new(db),
        };

        // Validate connectivity and ensure auxiliary tables exist (e.g. media chunk storage).
        engine.ensure_aux_tables()?;

        Ok(Arc::new(engine))
    }

    fn open_tree(&self, name: &'static str) -> Result<Arc<dyn KvTree>> {
        ensure_kv_table(&self.db, name)?;
        Ok(Arc::new(TursoTable {
            name: name.to_owned(),
            db: self.db.clone(),
            watchers: Watchers::default(),
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
pub(crate) fn block_on<F, T>(f: F) -> Result<T>
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

fn is_busy_error(e: &Error) -> bool {
    let s = e.to_string();
    s.contains("SQLITE_BUSY") || s.contains("database is locked")
}

/// How many times a database statement is retried while the remote database
/// reports its write lock as busy.
const BUSY_RETRIES: usize = 6;

/// How many times a compare-and-swap increment retries after losing the race
/// to a concurrent writer.
const CAS_ATTEMPTS: usize = 12;

/// Runs an operation, retrying with exponential backoff while the remote
/// database reports a busy/locked write lock.
///
/// Turso holds the write lock for the entire transaction of each concurrent
/// writer, and this backend issues several statements per logical operation,
/// so two overlapping write-heavy requests used to make one of them fail
/// immediately with `SQLITE_BUSY`. Waiting a little and retrying instead turns
/// that contention into latency rather than errors.
pub(crate) async fn with_busy_retry<F, Fut, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut delay = Duration::from_millis(250);
    let mut last_err = None;

    for attempt in 0..BUSY_RETRIES {
        match f().await {
            Ok(value) => return Ok(value),
            Err(e) if is_busy_error(&e) => {
                last_err = Some(e);
                if attempt + 1 < BUSY_RETRIES {
                    tokio::time::sleep(delay).await;
                    delay = delay.mul_f32(2.0).min(Duration::from_secs(2));
                }
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.expect("busy loop always records the last error"))
}

/// Executes a single statement, waiting out remote write-lock contention.
async fn exec_with_retry(
    conn: &libsql::Connection,
    sql: &str,
    params: Vec<libsql::Value>,
) -> Result<()> {
    exec_changed_with_retry(conn, sql, params).await.map(|_| ())
}

/// Executes a single statement and returns its affected row count, waiting out
/// remote write-lock contention.
async fn exec_changed_with_retry(
    conn: &libsql::Connection,
    sql: &str,
    params: Vec<libsql::Value>,
) -> Result<u64> {
    with_busy_retry(|| {
        let conn = conn.clone();
        let sql = sql.to_owned();
        let params = params.clone();
        async move {
            conn.execute(&sql, params)
                .await
                .map_err(|e| Error::LibSql(e.to_string()))
        }
    })
    .await
}

/// Builds a SQLite blob literal (`X'ABCD..'`) for inlining raw bytes into
/// batched SQL statements.
pub(crate) fn blob_literal(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 3);
    out.push_str("X'");
    for byte in bytes {
        let _ = write!(out, "{byte:02X}");
    }
    out.push('\'');
    out
}

/// Builds a quoted SQL text literal.
pub(crate) fn text_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Splits generated SQL statements into groups whose combined size stays under
/// `max_bytes`, so each pipeline request stays within remote size limits.
pub(crate) fn inline_sql_groups(
    statements: impl Iterator<Item = String>,
    max_bytes: usize,
) -> Vec<String> {
    let mut groups = Vec::new();
    let mut current = String::new();
    for statement in statements {
        if !current.is_empty() && current.len() + statement.len() > max_bytes {
            groups.push(std::mem::take(&mut current));
        }
        current.push_str(&statement);
        current.push('\n');
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

/// Runs a transactional batch (BEGIN and COMMIT are injected by the client and
/// the whole batch travels as one pipeline request), waiting out remote
/// write-lock contention. Each attempt opens a fresh connection so a stream
/// that was left holding an open, but lock-free, transaction after a failed
/// attempt is never reused.
pub(crate) async fn transactional_batch_with_retry(
    db: Arc<libsql::Database>,
    sql: String,
) -> Result<()> {
    with_busy_retry(|| {
        let db = db.clone();
        let sql = sql.clone();
        async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            conn.execute_transactional_batch(&sql)
                .await
                .map(|_| ())
                .map_err(|e| Error::LibSql(e.to_string()))
        }
    })
    .await
}

fn ensure_kv_table(db: &Arc<libsql::Database>, name: &str) -> Result<()> {
    if !is_safe_identifier(name) {
        return Err(Error::Initialization("invalid Turso table name"));
    }
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{name}\" (\"key\" BLOB PRIMARY KEY, \"value\" BLOB NOT NULL)"
    );
    block_on(async {
        let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
        exec_with_retry(&conn, &sql, Vec::new()).await
    })
}

impl Engine {
    fn ensure_aux_tables(&self) -> Result<()> {
        block_on(async {
            let conn = self.db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            exec_with_retry(
                &conn,
                "CREATE TABLE IF NOT EXISTS media_chunks (file_id TEXT NOT NULL, chunk_index INTEGER NOT NULL, chunk_data BLOB NOT NULL, chunk_hash BLOB, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), PRIMARY KEY (file_id, chunk_index))",
                Vec::new(),
            )
            .await?;
            exec_with_retry(
                &conn,
                "CREATE INDEX IF NOT EXISTS idx_media_chunks_created_at ON media_chunks(created_at)",
                Vec::new(),
            )
            .await?;

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
                exec_with_retry(
                    &conn,
                    "ALTER TABLE media_chunks ADD COLUMN chunk_hash BLOB",
                    Vec::new(),
                )
                .await?;
            }

            Ok(())
        })
    }
}

pub struct TursoTable {
    pub name: String,
    pub db: Arc<libsql::Database>,
    watchers: Watchers,
}

impl KvTree for TursoTable {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let name = self.name.clone();
        let db = self.db.clone();
        let key = key.to_vec();

        block_on(async move {
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
        let name = self.name.clone();
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        block_on(async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let sql = format!("INSERT OR REPLACE INTO {} (key, value) VALUES (?1, ?2)", name);
            let result = exec_with_retry(&conn, &sql, vec![key.clone().into(), value.into()]).await;
            if result.is_ok() {
                self.watchers.wake(&key);
            }
            result
        })
    }

    fn insert_batch<'a>(
        &self,
        iter: &mut dyn Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let name = self.name.clone();
        let db = self.db.clone();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = iter.collect();

        if pairs.is_empty() {
            return Ok(());
        }

        // The rows are written as one or more transactional batches, each sent
        // as a single pipeline request, so the remote write lock is held for
        // fast server-side execution instead of one round-trip per row.
        block_on(async move {
            let groups = inline_sql_groups(
                pairs.iter().map(|(key, value)| {
                    format!(
                        "INSERT OR REPLACE INTO {name} (key, value) VALUES ({}, {});",
                        blob_literal(key),
                        blob_literal(value)
                    )
                }),
                2 * 1024 * 1024,
            );

            for sql in groups {
                transactional_batch_with_retry(db.clone(), sql).await?;
            }

            // Wake sync watchers for every key that changed.
            for (key, _) in &pairs {
                self.watchers.wake(key);
            }

            Ok(())
        })
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        let name = self.name.clone();
        let db = self.db.clone();
        let key = key.to_vec();

        block_on(async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let sql = format!("DELETE FROM {} WHERE key = ?1", name);
            exec_with_retry(&conn, &sql, vec![key.into()]).await
        })
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = TupleOfBytes> + 'a> {
        // Fetch all rows upfront into a Vec, then return a standard iterator.
        let name = self.name.clone();
        let db = self.db.clone();

        let rows = block_on(async move {
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
        let name = self.name.clone();
        let db = self.db.clone();
        let from = from.to_vec();

        let rows = block_on(async move {
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
        let name = self.name.clone();
        let db = self.db.clone();
        let key = key.to_vec();

        block_on(async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let sql_select = format!("SELECT value FROM {} WHERE key = ?1", name);
            let sql_cas = format!(
                "INSERT INTO {} (key, value) VALUES (?1, ?2) \
                 ON CONFLICT(key) DO UPDATE SET value = ?2 WHERE value = ?3",
                name
            );

            // Compare-and-swap increment: read the old value, then write the
            // incremented value only if the stored value still equals the old
            // one. No transaction is held open across the two round-trips, so
            // the remote write lock is never kept while waiting on the
            // network, and a lost race simply retries with a fresh read.
            for attempt in 0..CAS_ATTEMPTS {
                let old: Option<Vec<u8>> = with_busy_retry(|| {
                    let conn = conn.clone();
                    let sql = sql_select.clone();
                    let key = key.clone();
                    async move {
                        let mut cursor = conn
                            .query(&sql, libsql::params![key])
                            .await
                            .map_err(|e| Error::LibSql(e.to_string()))?;
                        match cursor
                            .next()
                            .await
                            .map_err(|e| Error::LibSql(e.to_string()))?
                        {
                            Some(row) => {
                                let value: Vec<u8> =
                                    row.get(0).map_err(|e| Error::LibSql(e.to_string()))?;
                                Ok(Some(value))
                            }
                            None => Ok(None),
                        }
                    }
                })
                .await?;

                let new = crate::utils::increment(old.as_deref())
                    .expect("utils::increment always returns Some");

                // An absent old value is guarded with an empty blob, which no
                // increment ever stores, so a row that appears concurrently is
                // detected as a lost race instead of being clobbered.
                let changed = exec_changed_with_retry(
                    &conn,
                    &sql_cas,
                    vec![
                        key.clone().into(),
                        new.clone().into(),
                        old.clone().unwrap_or_default().into(),
                    ],
                )
                .await?;

                if changed > 0 {
                    self.watchers.wake(&key);
                    return Ok(new);
                }

                // A concurrent writer incremented this counter between our
                // read and our write. Back off briefly, then re-read and try
                // again.
                tokio::time::sleep(Duration::from_millis(50 + 50 * attempt as u64)).await;
            }

            Err(Error::LibSql(
                "increment lost the compare-and-swap race too many times".to_owned(),
            ))
        })
    }

    fn increment_batch<'a>(&self, iter: &mut dyn Iterator<Item = Vec<u8>>) -> Result<()> {
        let name = self.name.clone();
        let db = self.db.clone();

        // Deduplicate while collecting: a repeated key would increment twice.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for key in iter {
            if seen.insert(key.clone()) {
                keys.push(key);
            }
        }

        if keys.is_empty() {
            return Ok(());
        }

        block_on(async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;

            // Read every counter's old value in a single request.
            let placeholders = (1..=keys.len())
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql_select = format!(
                "SELECT key, value FROM {} WHERE key IN ({placeholders})",
                name
            );
            let select_params: Vec<libsql::Value> =
                keys.iter().map(|key| key.clone().into()).collect();

            let mut old_by_key: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
                keys.iter().map(|key| (key.clone(), None)).collect();
            {
                let mut cursor = with_busy_retry(|| {
                    let conn = conn.clone();
                    let sql = sql_select.clone();
                    let params = select_params.clone();
                    async move {
                        conn.query(&sql, params)
                            .await
                            .map_err(|e| Error::LibSql(e.to_string()))
                    }
                })
                .await?;
                while let Some(row) = cursor
                    .next()
                    .await
                    .map_err(|e| Error::LibSql(e.to_string()))?
                {
                    let key: Vec<u8> = row.get(0).map_err(|e| Error::LibSql(e.to_string()))?;
                    let value: Option<Vec<u8>> =
                        row.get(1).map_err(|e| Error::LibSql(e.to_string()))?;
                    if old_by_key.contains_key(&key) {
                        old_by_key.insert(key, value);
                    }
                }
            }

            // One transactional batch of compare-and-swap increments, sent as
            // a single pipeline request: the remote write lock is held for one
            // fast server-side execution instead of one round-trip per
            // counter. A counter changed between the read and the batch keeps
            // its stored value (the compare fails), which is benign for the
            // notification counts this powers.
            let sql = keys
                .iter()
                .map(|key| {
                    let old = old_by_key.get(key).cloned().flatten();
                    let new = crate::utils::increment(old.as_deref())
                        .expect("utils::increment always returns Some");
                    format!(
                        "INSERT INTO {name} (key, value) VALUES ({}, {}) \
                         ON CONFLICT(key) DO UPDATE SET value = {} WHERE value = {};",
                        blob_literal(key),
                        blob_literal(&new),
                        blob_literal(&new),
                        blob_literal(old.as_deref().unwrap_or(&[])),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            let result = transactional_batch_with_retry(db, sql).await;
            if result.is_ok() {
                for key in &keys {
                    self.watchers.wake(key);
                }
            }
            result
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

    fn watch_prefix<'a>(&'a self, prefix: &[u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        // Long-poll sync waits on this: it resolves when a write touches a key
        // under the prefix (see the wake calls in the mutation paths).
        self.watchers.watch(prefix)
    }

    fn clear(&self) -> Result<()> {
        let name = self.name.clone();
        let db = self.db.clone();

        block_on(async move {
            let conn = db.connect().map_err(|e| Error::LibSql(e.to_string()))?;
            let sql = format!("DELETE FROM {}", name);
            exec_with_retry(&conn, &sql, Vec::new()).await
        })
    }
}
