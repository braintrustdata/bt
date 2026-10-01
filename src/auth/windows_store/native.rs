use std::ffi::{c_void, OsStr, OsString};
use std::io;
use std::mem::size_of;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};
use std::ptr::{null, null_mut};

use anyhow::{bail, ensure, Context, Result};
use windows_sys::Win32::Foundation::{
    CloseHandle, LocalFree, APPMODEL_ERROR_NO_PACKAGE, ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND,
    ERROR_INSUFFICIENT_BUFFER, ERROR_PATH_NOT_FOUND, ERROR_SUCCESS, HANDLE, INVALID_HANDLE_VALUE,
    WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Globalization::{CompareStringOrdinal, CSTR_GREATER_THAN, CSTR_LESS_THAN};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SetSecurityInfo,
    SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::Cryptography::{
    CryptProtectData, CryptUnprotectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
};
use windows_sys::Win32::Security::{
    GetSecurityDescriptorDacl, GetTokenInformation, TokenUser, DACL_SECURITY_INFORMATION,
    PROTECTED_DACL_SECURITY_INFORMATION, SECURITY_ATTRIBUTES, TOKEN_IMPERSONATE, TOKEN_QUERY,
    TOKEN_USER,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateDirectoryW, CreateFileW, FileAttributeTagInfo, GetFileInformationByHandleEx,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
    FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::Storage::Packaging::Appx::GetCurrentPackageFullName;
use windows_sys::Win32::System::Com::CoTaskMemFree;
use windows_sys::Win32::System::Threading::{
    CreateProcessW, DeleteProcThreadAttributeList, GetCurrentProcess, GetExitCodeProcess,
    InitializeProcThreadAttributeList, OpenProcessToken, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject, CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT,
    EXTENDED_STARTUPINFO_PRESENT, LPPROC_THREAD_ATTRIBUTE_LIST, PROCESS_INFORMATION,
    PROC_THREAD_ATTRIBUTE_DESKTOP_APP_POLICY, STARTUPINFOEXW,
};
use windows_sys::Win32::System::WindowsProgramming::PROCESS_CREATION_DESKTOP_APP_BREAKAWAY_ENABLE_PROCESS_TREE;
use windows_sys::Win32::UI::Shell::{
    FOLDERID_Profile, FOLDERID_RoamingAppData, SHGetKnownFolderPath, KF_FLAG_DONT_VERIFY,
    KF_FLAG_NO_PACKAGE_REDIRECTION,
};
use zeroize::Zeroize;

struct OwnedHandle(HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

struct LocalAllocation(*mut c_void);

impl Drop for LocalAllocation {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

struct KnownFolderPath(*mut u16);

impl Drop for KnownFolderPath {
    fn drop(&mut self) {
        unsafe { CoTaskMemFree(self.0.cast()) };
    }
}

fn wide(value: &OsStr) -> Result<Vec<u16>> {
    let mut value: Vec<u16> = value.encode_wide().collect();
    ensure!(!value.contains(&0), "Windows path contains a NUL character");
    value.push(0);
    Ok(value)
}

// The caller owns a NUL-terminated string allocated by the Windows API.
unsafe fn wide_slice<'a>(value: *const u16) -> &'a [u16] {
    let mut len = 0;
    while *value.add(len) != 0 {
        len += 1;
    }
    std::slice::from_raw_parts(value, len)
}

fn known_folder(id: &windows_sys::core::GUID) -> Result<PathBuf> {
    // A null token lets Shell expand registry values such as %USERPROFILE%
    // using the inherited process environment. An explicit token resolves them
    // for the Windows account instead, including redirected roaming folders.
    let mut token = null_mut();
    if unsafe {
        OpenProcessToken(
            GetCurrentProcess(),
            TOKEN_QUERY | TOKEN_IMPERSONATE,
            &mut token,
        )
    } == 0
    {
        return Err(io::Error::last_os_error()).context("Cannot open Windows known-folder token");
    }
    let token = OwnedHandle(token);
    let mut path = KnownFolderPath(null_mut());
    // These flags select the native path; they do not disable filesystem
    // virtualization. The migration worker must independently verify that it
    // has no package identity before opening the legacy AppData files.
    let result = unsafe {
        SHGetKnownFolderPath(
            id,
            (KF_FLAG_NO_PACKAGE_REDIRECTION | KF_FLAG_DONT_VERIFY) as u32,
            token.0,
            &mut path.0,
        )
    };
    ensure!(
        result >= 0,
        "Cannot resolve Windows known folder (HRESULT {result:#010x})"
    );
    ensure!(!path.0.is_null(), "Windows returned no known-folder path");
    let path = PathBuf::from(unsafe { OsString::from_wide(wide_slice(path.0)) });
    ensure!(
        path.is_absolute(),
        "Windows returned a non-absolute known-folder path"
    );
    Ok(path)
}

pub(super) fn profile_dir() -> Result<PathBuf> {
    known_folder(&FOLDERID_Profile)
}

pub(super) fn legacy_auth_dir() -> Result<PathBuf> {
    Ok(known_folder(&FOLDERID_RoamingAppData)?.join("bt"))
}

pub(super) fn is_packaged() -> Result<bool> {
    let mut len = 0;
    match unsafe { GetCurrentPackageFullName(&mut len, null_mut()) } {
        APPMODEL_ERROR_NO_PACKAGE => Ok(false),
        ERROR_INSUFFICIENT_BUFFER | ERROR_SUCCESS => Ok(true),
        error => Err(io::Error::from_raw_os_error(error as i32))
            .context("Cannot determine Windows package identity"),
    }
}

fn current_user_sid() -> Result<String> {
    let mut token = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error()).context("Cannot open Windows user token");
    }
    let token = OwnedHandle(token);
    let mut bytes = 0;
    let result = unsafe { GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut bytes) };
    let error = io::Error::last_os_error();
    ensure!(
        result == 0 && error.raw_os_error() == Some(ERROR_INSUFFICIENT_BUFFER as i32),
        "Cannot size Windows user token: {error}"
    );
    // TOKEN_USER contains a pointer; a byte Vec would not guarantee alignment.
    let mut storage = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            storage.as_mut_ptr().cast(),
            bytes,
            &mut bytes,
        )
    } == 0
    {
        return Err(io::Error::last_os_error()).context("Cannot read Windows user token");
    }
    let user = unsafe { &*storage.as_ptr().cast::<TOKEN_USER>() };
    sid_string(user.User.Sid)
}

fn sid_string(sid: *mut c_void) -> Result<String> {
    let mut text = null_mut();
    if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
        return Err(io::Error::last_os_error()).context("Cannot encode Windows user SID");
    }
    let allocation = LocalAllocation(text.cast());
    String::from_utf16(unsafe { wide_slice(allocation.0.cast()) })
        .context("Windows returned an invalid SID string")
}

fn security_descriptor(sddl: &str) -> Result<LocalAllocation> {
    let sddl = wide(OsStr::new(sddl))?;
    let mut descriptor = null_mut();
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error())
            .context("Cannot construct auth directory permissions");
    }
    Ok(LocalAllocation(descriptor))
}

fn apply_private_dacl(handle: HANDLE, descriptor: &LocalAllocation) -> Result<()> {
    let mut present = 0;
    let mut defaulted = 0;
    let mut dacl = null_mut();
    if unsafe { GetSecurityDescriptorDacl(descriptor.0, &mut present, &mut dacl, &mut defaulted) }
        == 0
    {
        return Err(io::Error::last_os_error()).context("Cannot read auth directory permissions");
    }
    ensure!(
        present != 0 && !dacl.is_null(),
        "Auth directory permissions have no DACL"
    );
    let error = unsafe {
        SetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            dacl,
            null(),
        )
    };
    if error != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(error as i32))
            .context("Cannot restrict auth directory permissions");
    }
    Ok(())
}

fn open_directory(path: &[u16], writable_dacl: bool) -> io::Result<OwnedHandle> {
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            // SetSecurityInfo reads the existing descriptor when applying a
            // protected DACL; WRITE_DAC alone is not sufficient.
            FILE_READ_ATTRIBUTES
                | if writable_dacl {
                    READ_CONTROL | WRITE_DAC
                } else {
                    0
                },
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    Ok(OwnedHandle(handle))
}

/// `trusted` and its ancestors may be reparse points: admins commonly relocate
/// `C:\Users` or a profile with a junction. Anything bt creates or uses below
/// it must be a real directory.
pub(super) fn ensure_private_dir(path: &Path, trusted: &Path) -> Result<()> {
    ensure!(
        path.starts_with(trusted) && path != trusted,
        "Auth directory must be inside its trusted parent"
    );
    ensure!(
        path.is_absolute(),
        "Auth directory must be an absolute Windows path"
    );
    ensure!(
        path.components()
            .all(|part| !matches!(part, Component::ParentDir | Component::CurDir)),
        "Auth directory must not contain relative path components"
    );
    ensure!(
        path.file_name().is_some(),
        "Auth directory must not be a filesystem root"
    );
    let descriptor = security_descriptor(&format!(
        "D:P(A;OICI;FA;;;{})(A;OICI;FA;;;SY)",
        current_user_sid()?
    ))?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    let mut current = PathBuf::new();
    let mut handles = Vec::new();
    // Keep every ancestor open without FILE_SHARE_DELETE while descending. This
    // prevents an ancestor being renamed/replaced between inspection and use.
    // Existing ancestors are never re-ACL'd; only newly created directories and
    // the requested auth directory get the private, inheritable DACL.
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let encoded = wide(current.as_os_str())?;
        let is_target = current == path;
        let handle = match open_directory(&encoded, is_target) {
            Ok(handle) => handle,
            Err(error)
                if matches!(error.raw_os_error(), Some(code)
                if code == ERROR_FILE_NOT_FOUND as i32 || code == ERROR_PATH_NOT_FOUND as i32) =>
            {
                if unsafe { CreateDirectoryW(encoded.as_ptr(), &attributes) } == 0 {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
                        return Err(error).context("Cannot create private auth directory");
                    }
                }
                open_directory(&encoded, is_target).context("Cannot open auth directory")?
            }
            Err(error) => return Err(error).context("Cannot inspect auth directory ancestry"),
        };
        let mut info = FILE_ATTRIBUTE_TAG_INFO::default();
        if unsafe {
            GetFileInformationByHandleEx(
                handle.0,
                FileAttributeTagInfo,
                (&mut info as *mut FILE_ATTRIBUTE_TAG_INFO).cast(),
                size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error())
                .context("Cannot inspect auth directory attributes");
        }
        ensure!(
            info.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
                || trusted.starts_with(&current),
            "Auth directory and its ancestors below {} must not be Windows reparse points",
            trusted.display()
        );
        ensure!(
            info.FileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0,
            "Auth directory path contains a non-directory"
        );
        if is_target {
            apply_private_dacl(handle.0, &descriptor)?;
        }
        handles.push(handle);
    }
    Ok(())
}

struct DpapiOutput {
    blob: CRYPT_INTEGER_BLOB,
    plaintext: bool,
}

impl Drop for DpapiOutput {
    fn drop(&mut self) {
        if !self.blob.pbData.is_null() {
            unsafe {
                if self.plaintext {
                    std::slice::from_raw_parts_mut(self.blob.pbData, self.blob.cbData as usize)
                        .zeroize();
                }
                LocalFree(self.blob.pbData.cast());
            }
        }
    }
}

fn dpapi(data: &[u8], decrypt: bool) -> Result<Vec<u8>> {
    let input = CRYPT_INTEGER_BLOB {
        cbData: data
            .len()
            .try_into()
            .context("Auth snapshot exceeds Windows DPAPI size limit")?,
        // The Windows API declares a mutable pointer but never modifies input.
        pbData: data.as_ptr().cast_mut(),
    };
    let mut output = DpapiOutput {
        blob: CRYPT_INTEGER_BLOB {
            cbData: 0,
            pbData: null_mut(),
        },
        plaintext: decrypt,
    };
    let success = unsafe {
        if decrypt {
            CryptUnprotectData(
                &input,
                null_mut(),
                null(),
                null(),
                null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output.blob,
            )
        } else {
            // Deliberately no CRYPTPROTECT_LOCAL_MACHINE: only this Windows user
            // can decrypt the snapshot, including from another bt installation.
            CryptProtectData(
                &input,
                null(),
                null(),
                null(),
                null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output.blob,
            )
        }
    };
    if success == 0 {
        return Err(io::Error::last_os_error()).context(if decrypt {
            "Cannot decrypt Windows auth snapshot"
        } else {
            "Cannot encrypt Windows auth snapshot"
        });
    }
    if output.blob.cbData == 0 {
        return Ok(Vec::new());
    }
    ensure!(
        !output.blob.pbData.is_null(),
        "Windows DPAPI returned an invalid buffer"
    );
    Ok(
        unsafe { std::slice::from_raw_parts(output.blob.pbData, output.blob.cbData as usize) }
            .to_vec(),
    )
}

pub(super) fn protect(data: &[u8]) -> Result<Vec<u8>> {
    dpapi(data, false)
}

pub(super) fn unprotect(data: &[u8]) -> Result<Vec<u8>> {
    dpapi(data, true)
}

struct AttributeList {
    // Pointer-aligned storage for the opaque native structure, never resized.
    storage: Vec<usize>,
}

impl AttributeList {
    fn new() -> Result<Self> {
        let mut bytes = 0;
        let result = unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut bytes) };
        let error = io::Error::last_os_error();
        ensure!(
            result == 0 && error.raw_os_error() == Some(ERROR_INSUFFICIENT_BUFFER as i32),
            "Cannot size Windows process attributes: {error}"
        );
        let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
        if unsafe {
            InitializeProcThreadAttributeList(storage.as_mut_ptr().cast(), 1, 0, &mut bytes)
        } == 0
        {
            return Err(io::Error::last_os_error())
                .context("Cannot initialize Windows process attributes");
        }
        Ok(Self { storage })
    }

    fn as_ptr(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_mut_ptr().cast()
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.as_ptr()) };
    }
}

pub(super) fn run_migration_helper(worker: bool) -> Result<()> {
    launch_migration_helper(worker).context(
        "Windows auth migration could not complete. Open a normal Windows terminal outside the packaged app and run `bt profiles`, then retry",
    )
}

fn migration_environment(
    variables: impl IntoIterator<Item = (OsString, OsString)>,
) -> Result<Vec<u16>> {
    let mut entries = Vec::new();
    for (name, value) in variables {
        // This helper accesses native user storage, not command configuration.
        // In particular, do not reload BRAINTRUST_ENV_FILE: the parent may have
        // overridden it with --env-file, which is intentionally not forwarded.
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("BRAINTRUST_")
        {
            continue;
        }
        let name = wide(&name)?;
        let value = wide(&value)?;
        ensure!(
            name.len() <= i32::MAX as usize,
            "Windows environment variable name is too long"
        );
        entries.push((name, value));
    }
    entries.sort_by(|(a, _), (b, _)| {
        match unsafe {
            CompareStringOrdinal(a.as_ptr(), a.len() as i32, b.as_ptr(), b.len() as i32, 1)
        } {
            CSTR_LESS_THAN => std::cmp::Ordering::Less,
            CSTR_GREATER_THAN => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    });
    let mut block = Vec::new();
    for (name, value) in entries {
        block.extend_from_slice(&name[..name.len() - 1]);
        block.push(b'=' as u16);
        block.extend(value);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn launch_migration_helper(worker: bool) -> Result<()> {
    let exe = std::env::current_exe().context("Cannot locate bt migration executable")?;
    ensure!(
        exe.is_absolute(),
        "Migration executable path must be absolute"
    );
    let application = wide(exe.as_os_str())?;
    ensure!(
        !application.contains(&(b'"' as u16)),
        "Migration executable path contains a quote"
    );
    // argv[0] is quoted independently of lpApplicationName. No shell, PATH
    // search, user-supplied arguments, credential data, or destination paths.
    let mut command = vec![b'"' as u16];
    command.extend_from_slice(&application[..application.len() - 1]);
    command.extend("\" util migrate-windows-auth".encode_utf16());
    if worker {
        command.extend(" --worker".encode_utf16());
    }
    command.push(0);
    // Declare the value before the list: it must remain alive until the native
    // list is destroyed (including error paths). ENABLE_PROCESS_TREE affects
    // descendants, so the caller deliberately uses a relay followed by worker.
    let policy = PROCESS_CREATION_DESKTOP_APP_BREAKAWAY_ENABLE_PROCESS_TREE;
    let mut attributes = AttributeList::new()?;
    if unsafe {
        UpdateProcThreadAttribute(
            attributes.as_ptr(),
            0,
            PROC_THREAD_ATTRIBUTE_DESKTOP_APP_POLICY as usize,
            (&policy as *const u32).cast(),
            size_of::<u32>(),
            null_mut(),
            null(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error())
            .context("Cannot enable Windows migration breakaway policy");
    }
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.lpAttributeList = attributes.as_ptr();
    let mut process = PROCESS_INFORMATION::default();
    let environment = migration_environment(std::env::vars_os())?;
    if unsafe {
        CreateProcessW(
            application.as_ptr(),
            command.as_mut_ptr(),
            null(),
            null(),
            0,
            CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_ptr().cast(),
            null(),
            &startup.StartupInfo,
            &mut process,
        )
    } == 0
    {
        return Err(io::Error::last_os_error())
            .context("Cannot launch Windows auth migration helper");
    }
    let process_handle = OwnedHandle(process.hProcess);
    let _thread_handle = OwnedHandle(process.hThread);
    // The relay gets longer than its worker so it can report worker failure.
    let wait =
        unsafe { WaitForSingleObject(process_handle.0, if worker { 60_000 } else { 120_000 }) };
    if wait == WAIT_TIMEOUT {
        unsafe {
            TerminateProcess(process_handle.0, 1);
            WaitForSingleObject(process_handle.0, 5_000);
        }
        bail!("Windows auth migration helper timed out");
    }
    if wait != WAIT_OBJECT_0 {
        return Err(io::Error::last_os_error())
            .context("Cannot wait for Windows auth migration helper");
    }
    let mut exit_code = 0;
    if unsafe { GetExitCodeProcess(process_handle.0, &mut exit_code) } == 0 {
        return Err(io::Error::last_os_error())
            .context("Cannot read Windows auth migration helper result");
    }
    ensure!(
        exit_code == 0,
        "Windows auth migration helper failed (exit code {exit_code})"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::Security::Authorization::GetNamedSecurityInfoW;
    use windows_sys::Win32::Security::{
        GetAce, GetSecurityDescriptorControl, ACCESS_ALLOWED_ACE, CONTAINER_INHERIT_ACE,
        INHERITED_ACE, OBJECT_INHERIT_ACE, SE_DACL_PROTECTED,
    };
    use zeroize::Zeroizing;

    #[test]
    fn migration_does_not_inherit_cli_configuration_or_credentials() -> Result<()> {
        let block = migration_environment([
            ("SystemRoot".into(), "C:\\Windows".into()),
            ("braintrust_env_file".into(), "missing.env".into()),
            ("BRAINTRUST_API_KEY".into(), "test-sensitive-value".into()),
        ])?;
        let entries = String::from_utf16(&block)?;
        assert_eq!(entries, "SystemRoot=C:\\Windows\0\0");
        Ok(())
    }

    #[test]
    fn dpapi_roundtrips_large_snapshot_and_rejects_tampering() -> Result<()> {
        // Larger than Credential Manager's per-credential limit. Synthetic only.
        let plaintext = Zeroizing::new(vec![b'x'; 256 * 1024]);
        let mut encrypted = protect(&plaintext)?;
        let decrypted = Zeroizing::new(unprotect(&encrypted)?);
        assert_eq!(*decrypted, *plaintext);
        let last = encrypted.len() - 1;
        encrypted[last] ^= 1;
        assert!(unprotect(&encrypted).is_err());
        Ok(())
    }

    fn assert_private_acl(path: &Path, inherited: bool) -> Result<()> {
        let encoded = wide(path.as_os_str())?;
        let mut descriptor = null_mut();
        let mut dacl = null_mut();
        let result = unsafe {
            GetNamedSecurityInfoW(
                encoded.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        };
        ensure!(result == ERROR_SUCCESS, "Cannot read test ACL: {result}");
        let descriptor = LocalAllocation(descriptor);
        let mut control = 0;
        let mut revision = 0;
        assert_ne!(
            unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) },
            0
        );
        if !inherited {
            assert_ne!(control & SE_DACL_PROTECTED, 0);
        }
        assert!(!dacl.is_null());
        assert_eq!(unsafe { (*dacl).AceCount }, 2);
        let mut sids = Vec::new();
        for index in 0..2 {
            let mut ace = null_mut();
            assert_ne!(unsafe { GetAce(dacl, index, &mut ace) }, 0);
            let ace = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
            assert_eq!(ace.Header.AceType, 0); // ACCESS_ALLOWED_ACE_TYPE
            if inherited {
                assert_ne!(ace.Header.AceFlags as u32 & INHERITED_ACE, 0);
            } else {
                assert_eq!(
                    ace.Header.AceFlags as u32 & (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE),
                    OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
                );
            }
            sids.push(sid_string((&ace.SidStart as *const u32).cast_mut().cast())?);
        }
        sids.sort();
        let mut expected = vec![current_user_sid()?, "S-1-5-18".to_owned()];
        expected.sort();
        assert_eq!(sids, expected);
        Ok(())
    }

    #[test]
    fn private_directory_replaces_existing_acl_and_children_inherit() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let auth = temp.path().join("synthetic-auth");
        ensure_private_dir(&auth, temp.path())?;
        // Simulate an existing directory with an explicit world-access ACE.
        let encoded = wide(auth.as_os_str())?;
        let handle = open_directory(&encoded, true)?;
        apply_private_dacl(handle.0, &security_descriptor("D:P(A;OICI;FA;;;WD)")?)?;
        drop(handle);
        ensure_private_dir(&auth, temp.path())?;
        assert_private_acl(&auth, false)?;
        let child = auth.join("synthetic-snapshot");
        std::fs::write(&child, b"synthetic ciphertext")?;
        assert_private_acl(&child, true)?;
        Ok(())
    }

    #[test]
    fn private_directory_rejects_files_and_untrusted_reparse_ancestors() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let file = temp.path().join("not-a-directory");
        std::fs::write(&file, b"synthetic")?;
        assert!(ensure_private_dir(&file, temp.path()).is_err());
        let target = temp.path().join("target");
        std::fs::create_dir(&target)?;
        let link = temp.path().join("link");
        if let Err(error) = std::os::windows::fs::symlink_dir(&target, &link) {
            // Windows without Developer Mode requires the symlink privilege.
            if error.raw_os_error() == Some(1314) {
                return Ok(());
            }
            return Err(error.into());
        }
        assert!(ensure_private_dir(&link, temp.path()).is_err());
        assert!(ensure_private_dir(&link.join("auth"), temp.path()).is_err());
        assert!(!target.join("auth").exists());
        // A relocated trusted parent (e.g. a junctioned profile) is allowed.
        ensure_private_dir(&link.join("auth"), &link)?;
        assert!(target.join("auth").is_dir());
        Ok(())
    }

    fn junction(target: &Path, link: &Path) -> Result<()> {
        // Junctions, unlike symlinks, need no privilege, so this always runs.
        let status = std::process::Command::new("cmd")
            .arg("/C")
            .arg("mklink")
            .arg("/J")
            .arg(link)
            .arg(target)
            .stdout(std::process::Stdio::null())
            .status()?;
        ensure!(status.success(), "mklink /J failed: {status}");
        Ok(())
    }

    #[test]
    fn private_directory_allows_junctions_only_at_or_above_trusted_parent() -> Result<()> {
        let temp = tempfile::tempdir()?;
        // Mirrors `C:\Users` relocated to another volume with a junction.
        let real_users = temp.path().join("real-users");
        std::fs::create_dir_all(real_users.join("synthetic-user"))?;
        let users = temp.path().join("Users");
        junction(&real_users, &users)?;
        let profile = users.join("synthetic-user");
        let auth = profile.join(".braintrust").join("auth");
        ensure_private_dir(&auth, &profile)?;
        assert!(real_users
            .join("synthetic-user")
            .join(".braintrust")
            .join("auth")
            .is_dir());
        assert_private_acl(&auth, false)?;

        // A junction between the trusted parent and the auth dir is refused.
        let elsewhere = temp.path().join("elsewhere");
        std::fs::create_dir(&elsewhere)?;
        let other_profile = temp.path().join("other-profile");
        std::fs::create_dir(&other_profile)?;
        junction(&elsewhere, &other_profile.join(".braintrust"))?;
        assert!(ensure_private_dir(
            &other_profile.join(".braintrust").join("auth"),
            &other_profile
        )
        .is_err());
        assert!(!elsewhere.join("auth").exists());
        Ok(())
    }
}
