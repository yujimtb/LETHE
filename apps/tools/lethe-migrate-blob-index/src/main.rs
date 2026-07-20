use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use rusqlite::Connection;
use serde::Serialize;
use sha2::{Digest, Sha256};

const BLOB_REF_PREFIX: &str = "blob:sha256:";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    DryRun,
    Execute,
    Verify,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "dry-run" => Ok(Self::DryRun),
            "execute" => Ok(Self::Execute),
            "verify" => Ok(Self::Verify),
            _ => Err("--mode must be dry-run, execute, or verify".to_owned()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry-run",
            Self::Execute => "execute",
            Self::Verify => "verify",
        }
    }
}

#[derive(Debug)]
struct Arguments {
    mode: Mode,
    database: PathBuf,
    blob_dir: PathBuf,
    receipt: PathBuf,
}

#[derive(Debug)]
struct IndexRow {
    blob_ref: String,
    file_name: Option<String>,
}

#[derive(Serialize)]
struct Receipt {
    schema: &'static str,
    mode: &'static str,
    database: String,
    blob_dir: String,
    row_count: u64,
    index_digest_sha256: String,
    invalid_rows: u64,
    index_mismatches: u64,
    missing_files: u64,
    content_digest_mismatches: u64,
    executed_at: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = parse_arguments(env::args().skip(1))?;
    validate_paths(&arguments)?;

    let connection = Connection::open(&arguments.database)?;
    let columns = table_columns(&connection)?;
    let rows = match (
        columns.iter().any(|column| column == "file_path"),
        columns.iter().any(|column| column == "file_name"),
        arguments.mode,
    ) {
        (true, false, Mode::DryRun | Mode::Execute) => read_legacy_rows(&connection)?,
        (false, true, Mode::Verify) => read_current_rows(&connection)?,
        (true, false, Mode::Verify) => {
            return Err("verify requires the migrated file_name blob index".into());
        }
        (false, true, Mode::DryRun | Mode::Execute) => {
            return Err("blob index is already migrated; use --mode=verify".into());
        }
        _ => {
            return Err(
                "blob index schema must contain exactly one of file_path or file_name".into(),
            );
        }
    };

    let receipt = verify_rows(&arguments, &rows)?;
    if receipt.invalid_rows != 0
        || receipt.index_mismatches != 0
        || receipt.missing_files != 0
        || receipt.content_digest_mismatches != 0
    {
        return Err("blob index or CAS content verification failed".into());
    }

    if arguments.mode == Mode::Execute {
        connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE blobs_relocated (
                 blob_ref TEXT PRIMARY KEY,
                 file_name TEXT NOT NULL CHECK (
                     length(file_name) = 64
                     AND file_name NOT GLOB '*[^0-9a-f]*'
                 )
             );
             INSERT INTO blobs_relocated(blob_ref, file_name)
                 SELECT blob_ref, substr(blob_ref, 13) FROM blobs;
             DROP TABLE blobs;
             ALTER TABLE blobs_relocated RENAME TO blobs;
             COMMIT;",
        )?;
    }

    let encoded = serde_json::to_vec_pretty(&receipt)?;
    fs::write(&arguments.receipt, encoded)?;
    println!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}

fn parse_arguments(
    arguments: impl Iterator<Item = String>,
) -> Result<Arguments, Box<dyn std::error::Error>> {
    let mut values = BTreeMap::new();
    for argument in arguments {
        let (name, value) = argument
            .split_once('=')
            .ok_or_else(|| format!("argument must use --name=value form: {argument}"))?;
        if !matches!(name, "--mode" | "--database" | "--blob-dir" | "--receipt") {
            return Err(format!("unknown argument: {name}").into());
        }
        if value.is_empty() {
            return Err(format!("argument value must not be empty: {name}").into());
        }
        if values.insert(name.to_owned(), value.to_owned()).is_some() {
            return Err(format!("duplicate argument: {name}").into());
        }
    }

    let required = |name: &str| {
        values
            .get(name)
            .cloned()
            .ok_or_else(|| format!("missing {name}"))
    };
    Ok(Arguments {
        mode: Mode::parse(&required("--mode")?)?,
        database: PathBuf::from(required("--database")?),
        blob_dir: PathBuf::from(required("--blob-dir")?),
        receipt: PathBuf::from(required("--receipt")?),
    })
}

fn validate_paths(arguments: &Arguments) -> Result<(), Box<dyn std::error::Error>> {
    if !arguments.database.is_file() {
        return Err("database must be an existing file".into());
    }
    if !arguments.blob_dir.is_dir() {
        return Err("blob-dir must be an existing directory".into());
    }
    if arguments.receipt.exists() {
        return Err("receipt path already exists".into());
    }
    let receipt_parent = arguments
        .receipt
        .parent()
        .ok_or("receipt must have an explicit parent directory")?;
    if !receipt_parent.is_dir() {
        return Err("receipt parent directory must already exist".into());
    }
    Ok(())
}

fn table_columns(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("PRAGMA table_info(blobs)")?;
    statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect()
}

fn read_legacy_rows(connection: &Connection) -> rusqlite::Result<Vec<IndexRow>> {
    let mut statement = connection.prepare("SELECT blob_ref FROM blobs ORDER BY blob_ref")?;
    statement
        .query_map([], |row| {
            Ok(IndexRow {
                blob_ref: row.get(0)?,
                file_name: None,
            })
        })?
        .collect()
}

fn read_current_rows(connection: &Connection) -> rusqlite::Result<Vec<IndexRow>> {
    let mut statement =
        connection.prepare("SELECT blob_ref, file_name FROM blobs ORDER BY blob_ref")?;
    statement
        .query_map([], |row| {
            Ok(IndexRow {
                blob_ref: row.get(0)?,
                file_name: Some(row.get(1)?),
            })
        })?
        .collect()
}

fn verify_rows(
    arguments: &Arguments,
    rows: &[IndexRow],
) -> Result<Receipt, Box<dyn std::error::Error>> {
    let mut index_digest = Sha256::new();
    let mut invalid_rows = 0;
    let mut index_mismatches = 0;
    let mut missing_files = 0;
    let mut content_digest_mismatches = 0;

    for row in rows {
        let Some(expected_file_name) = digest_from_blob_ref(&row.blob_ref) else {
            invalid_rows += 1;
            continue;
        };
        let stored_file_name = row.file_name.as_deref().unwrap_or(expected_file_name);
        index_digest.update(row.blob_ref.as_bytes());
        index_digest.update([0]);
        index_digest.update(stored_file_name.as_bytes());
        index_digest.update([0]);

        if stored_file_name != expected_file_name {
            index_mismatches += 1;
            continue;
        }
        let file_path = arguments.blob_dir.join(stored_file_name);
        if !file_path.is_file() {
            missing_files += 1;
            continue;
        }
        if sha256_file(&file_path)? != expected_file_name {
            content_digest_mismatches += 1;
        }
    }

    Ok(Receipt {
        schema: "schema:lethe-offline-blob-index-verification-receipt",
        mode: arguments.mode.as_str(),
        database: arguments.database.display().to_string(),
        blob_dir: arguments.blob_dir.display().to_string(),
        row_count: rows.len() as u64,
        index_digest_sha256: hex::encode(index_digest.finalize()),
        invalid_rows,
        index_mismatches,
        missing_files,
        content_digest_mismatches,
        executed_at: Utc::now().to_rfc3339(),
    })
}

fn digest_from_blob_ref(blob_ref: &str) -> Option<&str> {
    let digest = blob_ref.strip_prefix(BLOB_REF_PREFIX)?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(digest)
}

fn sha256_file(path: &Path) -> Result<String, std::io::Error> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    std::io::copy(&mut file, &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::{Mode, digest_from_blob_ref};

    #[test]
    fn blob_reference_requires_exact_lowercase_sha256() {
        let digest = "a".repeat(64);
        assert_eq!(
            digest_from_blob_ref(&format!("blob:sha256:{digest}")),
            Some(digest.as_str())
        );
        assert_eq!(
            digest_from_blob_ref(&format!("blob:sha256:{}", "A".repeat(64))),
            None
        );
        assert_eq!(digest_from_blob_ref("blob:sha256:abc"), None);
        assert_eq!(digest_from_blob_ref(&format!("sha256:{digest}")), None);
    }

    #[test]
    fn mode_is_explicit_and_closed() {
        assert_eq!(Mode::parse("dry-run"), Ok(Mode::DryRun));
        assert_eq!(Mode::parse("execute"), Ok(Mode::Execute));
        assert_eq!(Mode::parse("verify"), Ok(Mode::Verify));
        assert!(Mode::parse("fallback").is_err());
    }
}
