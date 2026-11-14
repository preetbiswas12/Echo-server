//! Chunked, encrypted media storage for the Turso media backend.
//!
//! Media files are split into chunks of at most the configured [`chunk size`],
//! every chunk is encrypted with AES-256-GCM using a per-file key derived
//! (HKDF-SHA256, bound to the file's content hash) from the configured master
//! key, and the ciphertext chunks are stored in the `media_chunks` table of the
//! main Turso database, alongside per-chunk SHA-256 hashes.
//!
//! Each stored chunk is laid out as `nonce (12 bytes) || ciphertext || tag`,
//! and its hash covers exactly those stored bytes, so chunk integrity can be
//! checked (and the hashes listed) without transferring the chunk contents.
//!
//! On read, every chunk's stored hash is re-verified, every chunk is
//! authenticated by its AES-GCM tag, and the reassembled plaintext is verified
//! against the file's SHA-256 digest before it is served.
//!
//! Retention is driven by the existing metadata layer: files that exceed the
//! configured retention policy are purged through `purge_files`, which removes
//! their chunks from the database.

use std::sync::Arc;

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use hkdf::Hkdf;
use rand::{RngCore, rng};
use ruma::api::error::ErrorKind;
use sha2::{Digest, Sha256};

use conduit_config::{TursoConfig, TursoMediaBackend};

use crate::{Error, Result};

use crate::database::abstraction::turso::{
    block_on, transactional_batch_with_retry, with_busy_retry,
};

/// HKDF info string binding derived keys to the Echo media chunk format.
const KEY_INFO: &[u8] = b"echo-media-chunk";
/// AES-256-GCM nonce length in bytes.
const NONCE_LEN: usize = 12;
/// AES-256-GCM authentication tag length in bytes.
const TAG_LEN: usize = 16;
/// AES-256 key length in bytes.
const KEY_LEN: usize = 32;

/// Chunked encrypted media storage in the main Turso database.
///
/// All operations share the database's pooled HTTP connection, mirroring the
/// key-value engine's usage pattern for the remote Turso backend.
pub struct TursoChunkStore {
    /// Shared remote database handle so operations reuse pooled connections.
    db: Arc<libsql::Database>,
    master_key: [u8; KEY_LEN],
    chunk_size: usize,
}

impl TursoChunkStore {
    /// Builds a store from the media backend settings and the main database's
    /// Turso connection settings (media chunks live in the same database).
    pub fn new(media: &TursoMediaBackend, database: &TursoConfig) -> Result<Self> {
        let master_key = hex::decode(&media.master_key)
            .ok()
            .and_then(|bytes| <[u8; KEY_LEN]>::try_from(bytes).ok())
            .ok_or_else(|| {
                Error::TursoMedia(
                    "the configured media master key is not a 64-character hex string".to_owned(),
                )
            })?;

        let chunk_size = usize::try_from(media.chunk_size.0)
            .map_err(|_| Error::TursoMedia("the configured media chunk size is too large".to_owned()))?;

        if chunk_size == 0 {
            return Err(Error::TursoMedia(
                "the configured media chunk size must be greater than zero".to_owned(),
            ));
        }

        let db = block_on(async {
            libsql::Builder::new_remote(
                database.url.clone(),
                database.auth_token.clone().unwrap_or_default(),
            )
            .build()
            .await
            .map_err(|e| Error::LibSql(e.to_string()))
        })?;

        Ok(Self {
            db: Arc::new(db),
            master_key,
            chunk_size,
        })
    }

    /// Stores `file` as encrypted chunks under `file_id` (the file's sha256
    /// hex), atomically replacing any chunks previously stored for it.
    pub async fn create_file(&self, file_id: &str, file: &[u8]) -> Result<()> {
        let file_sha256 = sha256_from_id(file_id)?;
        let file_key = derive_file_key(&self.master_key, &file_sha256);

        let chunks: Vec<(i64, Vec<u8>, Vec<u8>)> = chunk_plaintexts(file, self.chunk_size)
            .into_iter()
            .enumerate()
            .map(|(index, plaintext)| {
                let chunk = encrypt_chunk(&file_key, plaintext)?;
                let hash = chunk_hash(&chunk);

                Ok((index as i64, chunk, hash.to_vec()))
            })
            .collect::<Result<_>>()?;

        // The DELETE plus every chunk INSERT is sent as transactional batches
        // — single pipeline requests — so the remote write lock is held for
        // fast server-side execution instead of one round-trip per chunk.
        let file_id_sql = crate::database::abstraction::turso::text_literal(file_id);
        let mut statements = Vec::with_capacity(chunks.len() + 1);
        statements.push(format!(
            "DELETE FROM media_chunks WHERE file_id = {file_id_sql};"
        ));
        for (index, chunk, hash) in &chunks {
            statements.push(format!(
                "INSERT INTO media_chunks (file_id, chunk_index, chunk_data, chunk_hash) VALUES ({file_id_sql}, {index}, {}, {});",
                crate::database::abstraction::turso::blob_literal(chunk),
                crate::database::abstraction::turso::blob_literal(hash),
            ));
        }

        for group in crate::database::abstraction::turso::inline_sql_groups(
            statements.into_iter(),
            2 * 1024 * 1024,
        ) {
            transactional_batch_with_retry(self.db.clone(), group).await?;
        }

        Ok(())
    }

    /// Reads, verifies and reassembles the file stored under `file_id`.
    ///
    /// Every chunk's stored hash is re-verified, every chunk is authenticated
    /// by its AES-GCM tag, and the reassembled plaintext is verified against
    /// `file_id` before being returned.
    pub async fn get_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let file_sha256 = sha256_from_id(file_id)?;
        let file_key = derive_file_key(&self.master_key, &file_sha256);

        let conn = self.connect().await?;

        let mut cursor = conn
            .query(
                "SELECT chunk_index, chunk_data, chunk_hash FROM media_chunks WHERE file_id = ?1 ORDER BY chunk_index ASC",
                libsql::params![file_id],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;

        let mut file = Vec::new();
        let mut expected_index = 0i64;

        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?
        {
            let index: i64 = row.get(0).map_err(|e| Error::LibSql(e.to_string()))?;

            if index != expected_index {
                return Err(Error::TursoMedia(format!(
                    "media file \"{file_id}\" is missing chunk {expected_index}"
                )));
            }

            let chunk: Vec<u8> = row.get(1).map_err(|e| Error::LibSql(e.to_string()))?;
            let stored_hash: Option<Vec<u8>> =
                row.get(2).map_err(|e| Error::LibSql(e.to_string()))?;

            let Some(stored_hash) = stored_hash else {
                return Err(Error::TursoMedia(format!(
                    "media file \"{file_id}\" chunk {index} has no stored hash"
                )));
            };

            if chunk_hash(&chunk).as_slice() != stored_hash.as_slice() {
                return Err(Error::TursoMedia(format!(
                    "media file \"{file_id}\" chunk {index} hash mismatch"
                )));
            }

            file.extend_from_slice(&decrypt_chunk(&file_key, &chunk)?);
            expected_index += 1;
        }

        if expected_index == 0 {
            return Err(Error::BadRequest(ErrorKind::NotFound, "File does not exist"));
        }

        if Sha256::digest(&file).as_slice() != file_sha256.as_slice() {
            return Err(Error::TursoMedia(format!(
                "media file \"{file_id}\" failed full-file hash verification"
            )));
        }

        Ok(file)
    }

    /// Returns the stored `(chunk_index, chunk_hash)` pairs for `file_id`,
    /// without transferring any chunk contents.
    pub async fn chunk_hashes(&self, file_id: &str) -> Result<Vec<(u64, Vec<u8>)>> {
        let conn = self.connect().await?;

        let mut cursor = conn
            .query(
                "SELECT chunk_index, chunk_hash FROM media_chunks WHERE file_id = ?1 ORDER BY chunk_index ASC",
                libsql::params![file_id],
            )
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?;

        let mut hashes = Vec::new();

        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| Error::LibSql(e.to_string()))?
        {
            let index: i64 = row.get(0).map_err(|e| Error::LibSql(e.to_string()))?;

            if index < 0 {
                return Err(Error::TursoMedia(format!(
                    "media file \"{file_id}\" has a negative chunk index"
                )));
            }

            let hash: Option<Vec<u8>> = row.get(1).map_err(|e| Error::LibSql(e.to_string()))?;

            let Some(hash) = hash else {
                return Err(Error::TursoMedia(format!(
                    "media file \"{file_id}\" chunk {index} has no stored hash"
                )));
            };

            hashes.push((index as u64, hash));
        }

        Ok(hashes)
    }

    /// Deletes the chunks of the given files (sha256 hexes) from the database.
    ///
    /// Missing files are ignored (idempotent, matching the filesystem
    /// backend's behavior); per-file failures are collected and returned.
    pub async fn delete_files(&self, file_ids: &[String]) -> Result<Vec<Error>> {
        if file_ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.connect().await?;
        let mut errors = Vec::new();

        for file_id in file_ids {
            if let Err(e) = with_busy_retry(|| {
                let conn = conn.clone();
                let sql = "DELETE FROM media_chunks WHERE file_id = ?1".to_owned();
                let file_id = file_id.clone();
                async move {
                    conn.execute(&sql, libsql::params![file_id])
                        .await
                        .map(|_| ())
                        .map_err(|e| Error::LibSql(e.to_string()))
                }
            })
            .await
            {
                errors.push(Error::LibSql(format!(
                    "failed to delete the media chunks of \"{file_id}\": {e}"
                )));
            }
        }

        Ok(errors)
    }

    /// Opens a connection on the shared database. Connections are cheap
    /// handles over the pooled HTTP client, so this costs no round-trips.
    async fn connect(&self) -> Result<libsql::Connection> {
        self.db.connect().map_err(|e| Error::LibSql(e.to_string()))
    }
}

/// Decodes a sha256 hex file id into its 32-byte digest.
fn sha256_from_id(file_id: &str) -> Result<[u8; 32]> {
    hex::decode(file_id)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| Error::TursoMedia(format!("\"{file_id}\" is not a valid sha256 hex string")))
}

/// Derives the per-file AES-256 key from the master key, binding it to the
/// file's content hash so the same file always maps to the same key.
fn derive_file_key(master_key: &[u8; KEY_LEN], file_sha256: &[u8; 32]) -> [u8; KEY_LEN] {
    let hkdf = Hkdf::<Sha256>::new(Some(file_sha256), master_key);
    let mut file_key = [0u8; KEY_LEN];

    hkdf.expand(KEY_INFO, &mut file_key)
        .expect("valid HKDF output length");

    file_key
}

/// Encrypts one chunk, returning `nonce || ciphertext || tag`.
fn encrypt_chunk(file_key: &[u8; KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(file_key)
        .map_err(|e| Error::TursoMedia(format!("failed to initialize the media cipher: {e}")))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    rng().fill_bytes(&mut nonce_bytes);

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext)
        .map_err(|e| Error::TursoMedia(format!("failed to encrypt a media chunk: {e}")))?;

    let mut chunk = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    chunk.extend_from_slice(&nonce_bytes);
    chunk.extend_from_slice(&ciphertext);

    Ok(chunk)
}

/// Decrypts one chunk produced by `encrypt_chunk` (`nonce || ciphertext || tag`).
fn decrypt_chunk(file_key: &[u8; KEY_LEN], chunk: &[u8]) -> Result<Vec<u8>> {
    if chunk.len() < NONCE_LEN + TAG_LEN {
        return Err(Error::TursoMedia(format!(
            "media chunk is too short to be valid ({} bytes)",
            chunk.len()
        )));
    }

    let cipher = Aes256Gcm::new_from_slice(file_key)
        .map_err(|e| Error::TursoMedia(format!("failed to initialize the media cipher: {e}")))?;

    let (nonce, ciphertext) = chunk.split_at(NONCE_LEN);

    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            Error::TursoMedia(
                "media chunk failed authentication (corrupted or wrong key)".to_owned(),
            )
        })
}

/// SHA-256 over the stored chunk bytes (`nonce || ciphertext || tag`).
fn chunk_hash(chunk: &[u8]) -> [u8; 32] {
    Sha256::digest(chunk).into()
}

/// Splits a file's plaintext into at most `chunk_size`-sized chunks.
///
/// A zero-byte file is stored as a single empty chunk so that reading it back
/// yields an empty (but authenticated) plaintext.
fn chunk_plaintexts(file: &[u8], chunk_size: usize) -> Vec<&[u8]> {
    if file.is_empty() {
        vec![&[]]
    } else {
        file.chunks(chunk_size).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn master_key() -> [u8; KEY_LEN] {
        [0x42; KEY_LEN]
    }

    fn file_hash() -> [u8; 32] {
        Sha256::digest(b"echo test file").into()
    }

    #[test]
    fn derive_file_key_is_deterministic_and_file_bound() {
        let first = derive_file_key(&master_key(), &file_hash());
        let second = derive_file_key(&master_key(), &file_hash());
        assert_eq!(first, second);

        let other_hash: [u8; 32] = Sha256::digest(b"a different file").into();
        assert_ne!(derive_file_key(&master_key(), &other_hash), first);
    }

    #[test]
    fn chunk_roundtrip() {
        let key = derive_file_key(&master_key(), &file_hash());
        let plaintext: &[u8] = b"hello echo media";

        let chunk = encrypt_chunk(&key, plaintext).expect("encryption works");
        assert_eq!(chunk.len(), NONCE_LEN + plaintext.len() + TAG_LEN);
        assert_ne!(&chunk[NONCE_LEN..], plaintext);

        let decrypted = decrypt_chunk(&key, &chunk).expect("decryption works");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let key = derive_file_key(&master_key(), &file_hash());

        let chunk = encrypt_chunk(&key, &[]).expect("encryption works");
        assert_eq!(chunk.len(), NONCE_LEN + TAG_LEN);

        let decrypted = decrypt_chunk(&key, &chunk).expect("decryption works");
        assert!(decrypted.is_empty());
    }

    #[test]
    fn chunks_are_nondeterministic_and_hashed() {
        let key = derive_file_key(&master_key(), &file_hash());

        let first = encrypt_chunk(&key, b"same plaintext").expect("encryption works");
        let second = encrypt_chunk(&key, b"same plaintext").expect("encryption works");

        // Fresh nonces: identical plaintexts produce distinct stored chunks.
        assert_ne!(first, second);
        assert_ne!(chunk_hash(&first), chunk_hash(&second));
        assert_eq!(chunk_hash(&first).len(), 32);
    }

    #[test]
    fn tampered_chunk_fails_authentication() {
        let key = derive_file_key(&master_key(), &file_hash());
        let mut chunk = encrypt_chunk(&key, b"tamper me").expect("encryption works");

        let last = chunk.len() - 1;
        chunk[last] ^= 0x01;

        assert!(decrypt_chunk(&key, &chunk).is_err());
    }

    #[test]
    fn truncated_chunk_fails_authentication() {
        let key = derive_file_key(&master_key(), &file_hash());
        let chunk = encrypt_chunk(&key, b"cut me").expect("encryption works");

        assert!(decrypt_chunk(&key, &chunk[..chunk.len() - 4]).is_err());
        assert!(decrypt_chunk(&key, &chunk[..NONCE_LEN + 1]).is_err());
    }

    #[test]
    fn wrong_key_fails_authentication() {
        let key = derive_file_key(&master_key(), &file_hash());
        let chunk = encrypt_chunk(&key, b"secret").expect("encryption works");

        let other_master = [0x07u8; KEY_LEN];
        let other_key = derive_file_key(&other_master, &file_hash());

        assert!(decrypt_chunk(&other_key, &chunk).is_err());
    }

    #[test]
    fn multi_chunk_reassembly() {
        let key = derive_file_key(&master_key(), &file_hash());
        let file: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

        let mut reassembled = Vec::new();

        for plaintext in chunk_plaintexts(&file, 1000) {
            let chunk = encrypt_chunk(&key, plaintext).expect("encryption works");
            assert!(chunk.len() <= NONCE_LEN + 1000 + TAG_LEN);
            reassembled.extend_from_slice(&decrypt_chunk(&key, &chunk).expect("decryption works"));
        }

        assert_eq!(reassembled, file);
    }

    #[test]
    fn chunk_plaintexts_splits_and_handles_empty() {
        let empty = chunk_plaintexts(&[], 100);
        assert_eq!(empty.len(), 1);
        assert!(empty[0].is_empty());

        let small = chunk_plaintexts(b"abc", 100);
        assert_eq!(small.len(), 1);
        assert_eq!(small[0], b"abc");

        let file: Vec<u8> = (0..250u8).collect();
        assert_eq!(
            chunk_plaintexts(&file, 100),
            vec![&file[0..100], &file[100..200], &file[200..250]]
        );
    }

    #[test]
    fn sha256_from_id_roundtrip() {
        let hash = file_hash();
        assert_eq!(sha256_from_id(&hex::encode(hash)).expect("valid"), hash);
        assert!(sha256_from_id("not-hex").is_err());
        assert!(sha256_from_id(&hex::encode([0u8; 16])).is_err());
    }
}
