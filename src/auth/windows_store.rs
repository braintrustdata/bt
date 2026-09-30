//! One user-scoped Windows auth snapshot, outside MSIX's AppData overlay.
//! Metadata and secrets migrate together; neither legacy file is a fallback
//! once the snapshot exists. Only an unpackaged process may read legacy data.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::{AuthStore, SecretStore};

#[cfg(windows)]
mod native;

const VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    auth: AuthStore,
    secrets: SecretStore,
    // Kept in the encrypted commit until both old files have been removed.
    // Hashes prevent interrupted cleanup from deleting subsequently changed data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    legacy_cleanup: Option<LegacyCleanup>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            version: VERSION,
            auth: AuthStore::default(),
            secrets: SecretStore::default(),
            legacy_cleanup: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct LegacyCleanup {
    auth: Option<[u8; 32]>,
    secrets: Option<[u8; 32]>,
}

type Transform = fn(&[u8]) -> Result<Vec<u8>>;

#[cfg(windows)]
/// Returns the private auth directory, the ancestor at or above which reparse
/// points are admin-controlled (e.g. a relocated profile), and the legacy dir.
fn locations() -> Result<(PathBuf, PathBuf, PathBuf)> {
    // Test-only injection keeps existing auth fixtures isolated. Production
    // never trusts HOME, USERPROFILE or APPDATA inherited from a desktop app.
    #[cfg(test)]
    {
        let root = PathBuf::from(
            std::env::var_os("XDG_CONFIG_HOME")
                .context("Windows auth tests require an isolated XDG_CONFIG_HOME")?,
        );
        let legacy = root.join("bt");
        Ok((legacy.join("windows-auth"), root, legacy))
    }
    #[cfg(not(test))]
    {
        let profile = native::profile_dir()?;
        Ok((
            profile.join(".braintrust").join("auth"),
            profile,
            native::legacy_auth_dir()?,
        ))
    }
}

#[cfg(windows)]
pub(super) fn path() -> Result<PathBuf> {
    // Only the initialized location is cached, never credentials. A long-lived
    // daemon still decrypts the latest snapshot for every credential resolution.
    #[cfg(not(test))]
    {
        // Fallible initialization must remain retryable; do not cache an error.
        static INITIALIZED: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
        let mut initialized = INITIALIZED
            .lock()
            .map_err(|_| anyhow::anyhow!("Windows auth initialization lock is poisoned"))?;
        if initialized.is_none() {
            *initialized = Some(initialize()?);
        }
        Ok(initialized.as_ref().expect("initialized above").clone())
    }
    #[cfg(test)]
    initialize()
}

#[cfg(windows)]
fn initialize() -> Result<PathBuf> {
    let (directory, trusted, legacy) = locations()?;
    native::ensure_private_dir(&directory, &trusted)?;
    let path = directory.join("credentials.dpapi");
    let existing = read_snapshot(&path, native::unprotect)?;
    if existing.is_some_and(|snapshot| snapshot.legacy_cleanup.is_none()) {
        return Ok(path);
    }
    if native::is_packaged()? {
        // The desktop-app policy controls the relay's CHILDREN, not necessarily
        // the relay itself. The hidden command takes the second hop and verifies
        // the worker is unpackaged before it opens AppData.
        let helper = native::run_migration_helper(false);
        let snapshot = read_snapshot(&path, native::unprotect)?;
        match (helper, snapshot) {
            (Ok(()), None) => {
                bail!("Windows auth migration did not create a credential store")
            }
            (Ok(()), Some(_)) => {}
            // Only legacy cleanup was pending; the committed snapshot is usable.
            (Err(error), Some(_)) => crate::ui::print_command_status(
                crate::ui::CommandStatus::Warning,
                &format!("Could not finish removing legacy Windows credentials ({error:#}); run `bt profiles` from a normal Windows terminal to retry."),
            ),
            (Err(error), None) => return Err(error),
        }
    } else {
        migrate(&path, &legacy, native::protect, native::unprotect)?;
    }
    Ok(path)
}

#[cfg(windows)]
pub(super) fn migrate_helper(worker: bool) -> Result<()> {
    if !worker {
        return native::run_migration_helper(true);
    }
    if native::is_packaged()? {
        bail!("Windows kept the credential migration worker inside the desktop app; run `bt profiles` from a normal Windows terminal");
    }
    // No credentials, profile names or source/destination paths are accepted
    // from the parent process. Resolve both locations for this Windows user.
    let (directory, trusted, legacy) = locations()?;
    native::ensure_private_dir(&directory, &trusted)?;
    migrate(
        &directory.join("credentials.dpapi"),
        &legacy,
        native::protect,
        native::unprotect,
    )
}

#[cfg(windows)]
pub(super) fn load_auth(path: &Path) -> Result<AuthStore> {
    Ok(read_snapshot(path, native::unprotect)?
        .unwrap_or_default()
        .auth)
}

/// The caller holds the snapshot's auth-store lock, including ID backfills.
#[cfg(windows)]
pub(super) fn save_auth(path: &Path, auth: &AuthStore) -> Result<()> {
    let mut snapshot = read_snapshot(path, native::unprotect)?.unwrap_or_default();
    snapshot.auth = auth.clone();
    write_snapshot(path, &snapshot, native::protect)
}

#[cfg(windows)]
pub(super) fn load_secrets(path: &Path) -> Result<SecretStore> {
    Ok(read_snapshot(path, native::unprotect)?
        .unwrap_or_default()
        .secrets)
}

#[cfg(windows)]
pub(super) fn save_secrets(path: &Path, secrets: &SecretStore) -> Result<()> {
    super::with_auth_store_lock(path, || {
        let mut snapshot = read_snapshot(path, native::unprotect)?.unwrap_or_default();
        snapshot.secrets = secrets.clone();
        write_snapshot(path, &snapshot, native::protect)
    })
}

#[cfg(windows)]
pub(super) fn set_secret(key: &str, value: Option<&str>) -> Result<()> {
    let path = path()?;
    super::with_auth_store_lock(&path, || {
        let mut snapshot = read_snapshot(&path, native::unprotect)?
            .context("Windows credential store disappeared while updating it")?;
        match value {
            Some(value) => {
                snapshot.secrets.secrets.insert(key.into(), value.into());
            }
            None => {
                snapshot.secrets.secrets.remove(key);
            }
        }
        write_snapshot(&path, &snapshot, native::protect)
    })
}

fn read_snapshot(path: &Path, unprotect: Transform) -> Result<Option<Snapshot>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("could not read Windows credential store"),
    };
    let plaintext =
        Zeroizing::new(unprotect(&bytes).context("could not decrypt Windows credential store")?);
    // Serde errors may quote invalid values. Never include decrypted content in
    // an error or fall back to a legacy store after corruption/decryption errors.
    let snapshot: Snapshot = serde_json::from_slice(&plaintext)
        .map_err(|_| anyhow::anyhow!("invalid Windows credential store"))?;
    if snapshot.version != VERSION {
        bail!(
            "unsupported Windows credential store version {}; upgrade bt",
            snapshot.version
        );
    }
    Ok(Some(snapshot))
}

fn write_snapshot(path: &Path, snapshot: &Snapshot, protect: Transform) -> Result<()> {
    let plaintext = Zeroizing::new(serde_json::to_vec(snapshot)?);
    let encrypted = protect(&plaintext).context("could not encrypt Windows credential store")?;
    let parent = path
        .parent()
        .context("credential store has no parent directory")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .context("could not stage Windows credential store")?;
    temp.write_all(&encrypted)?;
    temp.as_file().sync_all()?;
    temp.persist(path)
        .map_err(|error| error.error)
        .context("could not commit Windows credential store")?;
    Ok(())
}

struct LegacyInput {
    // On Windows the open handle denies concurrent writes/replacements until
    // the canonical snapshot has been committed and verified.
    _file: File,
    bytes: Zeroizing<Vec<u8>>,
}

impl LegacyInput {
    fn open(path: &Path) -> Result<Option<Self>> {
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Foundation::GENERIC_READ;
            use windows_sys::Win32::Storage::FileSystem::{DELETE, FILE_SHARE_READ};
            options
                .access_mode(GENERIC_READ | DELETE)
                .share_mode(FILE_SHARE_READ);
        }
        let mut file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not open legacy auth file {}", path.display()))
            }
        };
        let mut bytes = Zeroizing::new(Vec::new());
        file.read_to_end(&mut bytes)?;
        Ok(Some(Self { _file: file, bytes }))
    }

    fn digest(&self) -> [u8; 32] {
        Sha256::digest(&*self.bytes).into()
    }

    fn remove(self, _path: &Path) -> Result<()> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::{
                FileDispositionInfo, SetFileInformationByHandle, FILE_DISPOSITION_INFO,
            };
            let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
            // Delete the verified file by its still-open handle. Closing it and
            // then unlinking the pathname would permit a legacy writer to swap
            // in new credentials between digest validation and deletion.
            if unsafe {
                SetFileInformationByHandle(
                    self._file.as_raw_handle(),
                    FileDispositionInfo,
                    (&disposition as *const FILE_DISPOSITION_INFO).cast(),
                    std::mem::size_of::<FILE_DISPOSITION_INFO>() as u32,
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error())
                    .context("could not remove legacy auth file");
            }
            Ok(())
        }
        #[cfg(not(windows))]
        {
            // Portable migration tests; production uses handle-based deletion.
            fs::remove_file(_path).context("could not remove legacy auth file")
        }
    }
}

fn migrate(path: &Path, legacy: &Path, protect: Transform, unprotect: Transform) -> Result<()> {
    super::with_auth_store_lock(path, || {
        // Never select a newer-looking legacy copy over a committed snapshot.
        let operation = || migrate_locked(path, legacy, protect, unprotect);
        if legacy.try_exists()? {
            super::with_auth_store_lock(&legacy.join("auth.json"), operation)
        } else {
            operation()
        }
    })
}

fn migrate_locked(
    path: &Path,
    legacy: &Path,
    protect: Transform,
    unprotect: Transform,
) -> Result<()> {
    let mut snapshot = match read_snapshot(path, unprotect)? {
        Some(snapshot) => snapshot,
        None => {
            let auth = LegacyInput::open(&legacy.join("auth.json"))?;
            let secrets = LegacyInput::open(&legacy.join("secrets.json"))?;
            let mut snapshot = Snapshot::default();
            if let Some(auth) = &auth {
                snapshot.auth = serde_json::from_slice(&auth.bytes).map_err(|_| {
                    anyhow::anyhow!("invalid legacy auth.json; credentials were not migrated")
                })?;
            }
            if let Some(secrets) = &secrets {
                snapshot.secrets = serde_json::from_slice(&secrets.bytes).map_err(|_| {
                    anyhow::anyhow!("invalid legacy secrets.json; credentials were not migrated")
                })?;
            }
            if auth.is_some() || secrets.is_some() {
                snapshot.legacy_cleanup = Some(LegacyCleanup {
                    auth: auth.as_ref().map(LegacyInput::digest),
                    secrets: secrets.as_ref().map(LegacyInput::digest),
                });
            }
            write_snapshot(path, &snapshot, protect)?;
            // Check the actual persisted, decrypted snapshot before deleting any
            // source data. A crash before this point leaves the originals intact.
            let verified = read_snapshot(path, unprotect)?
                .context("Windows credential store disappeared during migration")?;
            let expected = Zeroizing::new(serde_json::to_vec(&snapshot)?);
            let actual = Zeroizing::new(serde_json::to_vec(&verified)?);
            if *expected != *actual {
                bail!("Windows credential migration verification failed; original files were retained");
            }
            snapshot
        }
    };
    let Some(cleanup) = &snapshot.legacy_cleanup else {
        return Ok(());
    };
    let files = [
        ("secrets.json", &cleanup.secrets),
        ("auth.json", &cleanup.auth),
    ];
    // Cleanup is best effort and attempted once: the committed snapshot is
    // already authoritative, so a legacy file that changed (never delete new
    // data) or cannot be removed is left in place rather than blocking auth.
    // Missing means a previous attempt already removed it.
    for (name, expected) in files {
        let Some(expected) = expected else { continue };
        let file = legacy.join(name);
        let result = LegacyInput::open(&file).and_then(|input| match input {
            Some(input) if input.digest() != *expected => {
                bail!("it changed after credentials were migrated")
            }
            Some(input) => input.remove(&file),
            None => Ok(()),
        });
        if let Err(error) = result {
            crate::ui::print_command_status(
                crate::ui::CommandStatus::Warning,
                &format!(
                    "Credentials were migrated, but legacy {} was not removed ({error:#}). bt no longer reads it; delete it once you no longer need it.",
                    file.display()
                ),
            );
        }
    }
    snapshot.legacy_cleanup = None;
    write_snapshot(path, &snapshot, protect)
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests exercise migration transactions on every OS. Native DPAPI
    // confidentiality/integrity and Windows permissions are tested in native.
    fn identity(bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("legacy");
        let shared = dir.path().join("shared");
        fs::create_dir(&legacy).unwrap();
        fs::create_dir(&shared).unwrap();
        fs::write(legacy.join("auth.json"), br#"{"profiles":{"test-profile":{"auth_kind":"oauth","oauth_access_expires_at":4102444800,"oauth_client_id":"test-client"}},"profile_ids":{"test-profile":"00000000-0000-4000-8000-000000000001"}}"#).unwrap();
        fs::write(legacy.join("secrets.json"), br#"{"secrets":{"oauth_access::test-profile":"test-access","oauth_refresh::test-profile":"test-refresh"}}"#).unwrap();
        (dir, shared.join("credentials.dpapi"), legacy)
    }

    #[test]
    fn upgrade_preserves_profile_identity_and_oauth_credentials_without_legacy_fallback() {
        let (_dir, path, legacy) = fixture();
        migrate(&path, &legacy, identity, identity).unwrap();
        let snapshot = read_snapshot(&path, identity).unwrap().unwrap();
        assert_eq!(
            snapshot.auth.profile_ids["test-profile"],
            "00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(
            snapshot.auth.profiles["test-profile"].oauth_access_expires_at,
            Some(4102444800)
        );
        assert_eq!(
            snapshot.secrets.secrets["oauth_access::test-profile"],
            "test-access"
        );
        assert_eq!(
            snapshot.secrets.secrets["oauth_refresh::test-profile"],
            "test-refresh"
        );
        assert!(!legacy.join("auth.json").exists());
        assert!(!legacy.join("secrets.json").exists());
        fs::write(legacy.join("auth.json"), b"invalid stale metadata").unwrap();
        fs::write(legacy.join("secrets.json"), b"invalid stale secrets").unwrap();
        migrate(&path, &legacy, identity, identity).unwrap();
        let current = read_snapshot(&path, identity).unwrap().unwrap();
        assert_eq!(
            current.secrets.secrets["oauth_refresh::test-profile"],
            "test-refresh"
        );
    }

    #[test]
    fn failed_protection_leaves_both_legacy_files_and_no_canonical_store() {
        let (_dir, path, legacy) = fixture();
        let failure = |_: &[u8]| -> Result<Vec<u8>> { bail!("protection unavailable") };
        assert!(migrate(&path, &legacy, failure, identity).is_err());
        assert!(!path.exists());
        assert!(legacy.join("auth.json").exists());
        assert!(legacy.join("secrets.json").exists());
        migrate(&path, &legacy, identity, identity).unwrap();
        assert_eq!(
            read_snapshot(&path, identity)
                .unwrap()
                .unwrap()
                .secrets
                .secrets["oauth_refresh::test-profile"],
            "test-refresh"
        );
    }

    #[test]
    fn corrupt_committed_store_never_recovers_from_stale_legacy_credentials() {
        let (_dir, path, legacy) = fixture();
        fs::write(&path, b"corrupt snapshot").unwrap();
        assert!(migrate(&path, &legacy, identity, identity).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"corrupt snapshot");
        assert!(legacy.join("secrets.json").exists());
    }

    #[test]
    fn interrupted_cleanup_resumes_without_reimporting_or_losing_credentials() {
        let (_dir, path, legacy) = fixture();
        let auth = LegacyInput::open(&legacy.join("auth.json"))
            .unwrap()
            .unwrap();
        let secrets = LegacyInput::open(&legacy.join("secrets.json"))
            .unwrap()
            .unwrap();
        let snapshot = Snapshot {
            auth: serde_json::from_slice(&auth.bytes).unwrap(),
            secrets: serde_json::from_slice(&secrets.bytes).unwrap(),
            legacy_cleanup: Some(LegacyCleanup {
                auth: Some(auth.digest()),
                secrets: Some(secrets.digest()),
            }),
            ..Snapshot::default()
        };
        drop((auth, secrets));
        write_snapshot(&path, &snapshot, identity).unwrap();
        fs::remove_file(legacy.join("secrets.json")).unwrap();
        migrate(&path, &legacy, identity, identity).unwrap();
        let current = read_snapshot(&path, identity).unwrap().unwrap();
        assert!(current.legacy_cleanup.is_none());
        assert_eq!(
            current.secrets.secrets["oauth_access::test-profile"],
            "test-access"
        );
        assert!(!legacy.join("auth.json").exists());
    }

    #[test]
    fn changed_legacy_file_is_not_deleted_after_interrupted_migration() {
        let (_dir, path, legacy) = fixture();
        let snapshot = Snapshot {
            legacy_cleanup: Some(LegacyCleanup {
                auth: Some(
                    LegacyInput::open(&legacy.join("auth.json"))
                        .unwrap()
                        .unwrap()
                        .digest(),
                ),
                secrets: Some([0; 32]),
            }),
            ..Snapshot::default()
        };
        write_snapshot(&path, &snapshot, identity).unwrap();
        // The snapshot stays authoritative: cleanup finishes without deleting
        // the changed file, so later runs are not blocked by it.
        migrate(&path, &legacy, identity, identity).unwrap();
        assert!(!legacy.join("auth.json").exists());
        assert!(legacy.join("secrets.json").exists());
        let current = read_snapshot(&path, identity).unwrap().unwrap();
        assert!(current.legacy_cleanup.is_none());
        migrate(&path, &legacy, identity, identity).unwrap();
        assert!(legacy.join("secrets.json").exists());
    }

    #[test]
    fn invalid_legacy_secret_does_not_publish_partial_metadata_or_echo_secret() {
        let (_dir, path, legacy) = fixture();
        fs::write(
            legacy.join("secrets.json"),
            br#"{"secrets":"test-sensitive-value"}"#,
        )
        .unwrap();
        let error = migrate(&path, &legacy, identity, identity).unwrap_err();
        assert!(!format!("{error:#}").contains("test-sensitive-value"));
        assert!(!path.exists());
        assert!(legacy.join("auth.json").exists());
        assert!(legacy.join("secrets.json").exists());
    }

    #[cfg(windows)]
    #[test]
    fn locked_legacy_file_does_not_block_cleanup_or_later_runs() {
        use std::os::windows::fs::OpenOptionsExt;
        let (_dir, path, legacy) = fixture();
        migrate(&path, &legacy, identity, identity).unwrap();
        // Simulate an interrupted cleanup whose legacy file is held open
        // without sharing (e.g. by an editor or antivirus scanner).
        let bytes = br#"{"secrets":{}}"#;
        fs::write(legacy.join("secrets.json"), bytes).unwrap();
        let mut snapshot = read_snapshot(&path, identity).unwrap().unwrap();
        snapshot.legacy_cleanup = Some(LegacyCleanup {
            auth: None,
            secrets: Some(Sha256::digest(bytes).into()),
        });
        write_snapshot(&path, &snapshot, identity).unwrap();
        let lock = OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(legacy.join("secrets.json"))
            .unwrap();

        migrate(&path, &legacy, identity, identity).unwrap();
        drop(lock);
        let current = read_snapshot(&path, identity).unwrap().unwrap();
        assert!(current.legacy_cleanup.is_none());
        assert_eq!(
            current.secrets.secrets["oauth_refresh::test-profile"],
            "test-refresh"
        );
        assert!(legacy.join("secrets.json").exists());
        migrate(&path, &legacy, identity, identity).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn legacy_file_cannot_be_replaced_between_verification_and_cleanup() {
        let (_dir, _, legacy) = fixture();
        let source = legacy.join("secrets.json");
        let replacement = legacy.join("replacement.json");
        fs::write(&replacement, b"new credentials").unwrap();
        let input = LegacyInput::open(&source).unwrap().unwrap();
        assert!(fs::rename(&replacement, &source).is_err());
        assert!(fs::write(&source, b"new credentials").is_err());
        input.remove(&source).unwrap();
        assert!(!source.exists());
        assert_eq!(fs::read(&replacement).unwrap(), b"new credentials");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn production_auth_preserves_login_on_upgrade_and_uses_only_the_protected_store() {
        let _lock = super::super::env_test_lock().lock().await;
        struct RestoreConfig(Option<std::ffi::OsString>);
        impl Drop for RestoreConfig {
            fn drop(&mut self) {
                match &self.0 {
                    Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                    None => std::env::remove_var("XDG_CONFIG_HOME"),
                }
            }
        }
        let (dir, _, legacy) = fixture();
        fs::rename(&legacy, dir.path().join("bt")).unwrap();
        let _restore = RestoreConfig(std::env::var_os("XDG_CONFIG_HOME"));
        std::env::set_var("XDG_CONFIG_HOME", dir.path());
        let auth = super::super::load_auth_store().unwrap();
        let id = auth.profile_ids["test-profile"].clone();
        assert_eq!(id, "00000000-0000-4000-8000-000000000001");
        assert_eq!(
            super::super::load_profile_oauth_refresh_token("test-profile")
                .unwrap()
                .as_deref(),
            Some("test-refresh")
        );
        super::super::commit_api_key_profile(
            "test-profile",
            "test-new-api-key",
            Some("https://app.example.test".into()),
            None,
        )
        .unwrap();
        assert_eq!(
            super::super::load_auth_store().unwrap().profile_ids["test-profile"],
            id
        );
        assert_eq!(
            super::super::load_profile_secret("test-profile")
                .unwrap()
                .as_deref(),
            Some("test-new-api-key")
        );
        assert!(
            super::super::load_profile_oauth_refresh_token("test-profile")
                .unwrap()
                .is_none()
        );
        let persisted = fs::read(path().unwrap()).unwrap();
        assert!(!persisted
            .windows(b"test-new-api-key".len())
            .any(|w| w == b"test-new-api-key"));
        assert!(!dir.path().join("bt/secrets.json").exists());
        assert!(!dir.path().join("bt/auth.json").exists());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_locations_do_not_follow_inherited_appdata_or_userprofile() {
        let _lock = super::super::env_test_lock().lock().await;
        let profile = native::profile_dir().unwrap();
        let legacy = native::legacy_auth_dir().unwrap();
        let original_appdata = std::env::var_os("APPDATA");
        let original_profile = std::env::var_os("USERPROFILE");
        let isolated = tempfile::tempdir().unwrap();
        std::env::set_var("APPDATA", isolated.path().join("virtualized-appdata"));
        std::env::set_var("USERPROFILE", isolated.path().join("virtualized-profile"));
        let actual_profile = native::profile_dir();
        let actual_legacy = native::legacy_auth_dir();
        for (name, value) in [
            ("APPDATA", original_appdata),
            ("USERPROFILE", original_profile),
        ] {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        assert_eq!(actual_profile.unwrap(), profile);
        assert_eq!(actual_legacy.unwrap(), legacy);
    }
}
