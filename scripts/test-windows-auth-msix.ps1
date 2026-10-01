#requires -Version 5.1
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$BtPath)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# This deliberately touches the production Known Folder paths, not test overrides.
# Never run it on a developer workstation or a persistent/self-hosted runner.
if ($env:OS -ne 'Windows_NT' -or $env:GITHUB_ACTIONS -ne 'true' -or
    $env:RUNNER_ENVIRONMENT -ne 'github-hosted' -or $env:RUNNER_OS -ne 'Windows') {
    throw 'This smoke test requires a disposable GitHub-hosted Windows Actions runner.'
}
if (-not [Environment]::Is64BitProcess -or $PSVersionTable.PSEdition -ne 'Desktop') {
    throw 'Run this script with 64-bit Windows PowerShell 5.1.'
}
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'The disposable runner must be elevated to trust the test signing certificate.'
}
$BtPath = (Resolve-Path -LiteralPath $BtPath).Path
$launcherSource = Join-Path $PSScriptRoot '..\tests\windows-auth-msix\Launcher.cs'
$null = Get-Command Invoke-CommandInDesktopPackage -ErrorAction Stop

Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class BtMsixKnownFolders {
    [DllImport("shell32.dll", PreserveSig = true)]
    private static extern int SHGetKnownFolderPath(ref Guid id, uint flags, IntPtr token, out IntPtr path);
    public static string Get(string id) {
        Guid guid = new Guid(id);
        IntPtr path;
        int result = SHGetKnownFolderPath(ref guid, 0, IntPtr.Zero, out path);
        Marshal.ThrowExceptionForHR(result);
        try { return Marshal.PtrToStringUni(path); }
        finally { Marshal.FreeCoTaskMem(path); }
    }
}
'@
Add-Type -AssemblyName System.Security
Add-Type -AssemblyName System.Drawing
$profileRoot = [BtMsixKnownFolders]::Get('5E6C858F-0E22-4760-9AFE-EA3317B67173')
$roamingRoot = [BtMsixKnownFolders]::Get('3EB685DB-65F9-4CF6-A03A-E3EF65729F3D')
$braintrustRoot = Join-Path $profileRoot '.braintrust'
$canonicalDir = Join-Path $braintrustRoot 'auth'
$canonicalPath = Join-Path $canonicalDir 'credentials.dpapi'
$legacyDir = Join-Path $roamingRoot 'bt'
$legacyAuth = Join-Path $legacyDir 'auth.json'
$legacySecrets = Join-Path $legacyDir 'secrets.json'

# Existence checks only: do not read an existing user's credentials, even to back them up.
foreach ($path in @($canonicalDir, $legacyAuth, $legacySecrets)) {
    if (Test-Path -LiteralPath $path) { throw 'Refusing an existing production auth store or legacy credential file.' }
}
foreach ($path in @($braintrustRoot, $legacyDir)) {
    if ((Test-Path -LiteralPath $path) -and
        ((Get-Item -LiteralPath $path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'Refusing a reparse point at a credential-store parent.'
    }
}
$hadBraintrustRoot = Test-Path -LiteralPath $braintrustRoot
$hadLegacyDir = Test-Path -LiteralPath $legacyDir
$runId = [Guid]::NewGuid().ToString('N')
# Keep controller IPC outside AppData, which is exactly the directory being virtualized.
$work = Join-Path $profileRoot ('bt-msix-smoke-' + $runId)
$packageName = 'Bt.AuthSmoke.' + $runId
$publisher = 'CN=Braintrust Auth Smoke ' + $runId
$utf8 = New-Object Text.UTF8Encoding($false)
$originalEnvironment = @{}
$originalXdg = [Environment]::GetEnvironmentVariable('XDG_CONFIG_HOME', 'Process')
$certificate = $null
$package = $null
$serverJob = $null
$ownsNormal = $false
$ownsCanonical = $false
$ownsWork = $false
$cleanupErrors = New-Object 'Collections.Generic.List[string]'

function Assert-Smoke([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

function Write-Json([string]$Path, $Value) {
    [IO.File]::WriteAllText($Path, ($Value | ConvertTo-Json -Depth 20 -Compress), $utf8)
}

function Wait-File([string]$Path, [int]$Seconds = 120) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    while (-not [IO.File]::Exists($Path)) {
        if ([DateTime]::UtcNow -ge $deadline) { throw 'Timed out waiting for an isolated smoke fixture.' }
        Start-Sleep -Milliseconds 100
    }
}

function Redact-Diagnostic([string]$Text) {
    foreach ($token in $allTokens) { $Text = $Text.Replace($token, '[redacted]') }
    return $Text
}

function Invoke-Fixture([bool]$Packaged, [hashtable]$Request) {
    $id = [Guid]::NewGuid().ToString('N')
    $requestPath = Join-Path $work ($id + '.request.json')
    $resultPath = Join-Path $work ($id + '.result.json')
    $Request.resultPath = $resultPath
    $Request.legacyDir = $legacyDir
    $Request.workingDirectory = $work
    if ($Request.action -eq 'run') {
        # Match a packaged desktop app spawning the separately installed CLI.
        $Request.btPath = $BtPath
        $childEnvironment = @{}
        foreach ($key in $originalEnvironment.Keys) { $childEnvironment[$key] = $null }
        $childEnvironment.XDG_CONFIG_HOME = Join-Path $work 'config'
        if ($Request.ContainsKey('environment')) {
            foreach ($key in $Request.environment.Keys) { $childEnvironment[$key] = $Request.environment[$key] }
        }
        $Request.environment = $childEnvironment
    }
    Write-Json $requestPath $Request
    if ($Packaged) {
        Invoke-CommandInDesktopPackage -PackageFamilyName $package.PackageFamilyName -AppId Fixture `
            -Command (Join-Path $package.InstallLocation 'Launcher.exe') -Args ('"' + $requestPath + '"') `
            -PreventBreakaway | Out-Null
    } else {
        $process = Start-Process -FilePath (Join-Path $work 'package\Launcher.exe') `
            -ArgumentList ('"' + $requestPath + '"') -PassThru -WindowStyle Hidden
        try {
            if (-not $process.WaitForExit(120000)) {
                $process.Kill()
                throw 'Unpackaged launcher timed out.'
            }
        } finally { $process.Dispose() }
    }
    Wait-File $resultPath
    $result = [IO.File]::ReadAllText($resultPath) | ConvertFrom-Json
    # Never dump fixture requests/results: they contain synthetic credentials.
    Assert-Smoke ($result.success -eq $true) ('Launcher failed during ' + $Request.action + ': ' + (Redact-Diagnostic $result.error))
    if ($Packaged) {
        Assert-Smoke ($result.packageFullName -ceq $package.PackageFullName) 'Fixture lost its installed package identity.'
    } else {
        Assert-Smoke ($null -eq $result.packageFullName) 'Ordinary fixture unexpectedly has package identity.'
    }
    if ($Request.action -eq 'run') {
        if ($Packaged) {
            Assert-Smoke ($result.childPackageFullName -ceq $package.PackageFullName) 'bt escaped package identity before exercising auth.'
        } else {
            Assert-Smoke ($null -eq $result.childPackageFullName) 'Ordinary bt unexpectedly has package identity.'
        }
        Assert-Smoke ($result.exitCode -eq 0) ('bt failed during ' + $Request.arguments[0] + ' (exit ' + $result.exitCode + '): ' + (Redact-Diagnostic $result.stderr))
    }
    return $result
}

function Invoke-Bt([bool]$Packaged, [string[]]$Arguments, [hashtable]$Environment = @{}) {
    return Invoke-Fixture $Packaged @{ action = 'run'; arguments = @($Arguments) + @('--json', '--no-input', '--no-color'); environment = $Environment }
}

function Assert-Snapshot([string]$Name, [string]$Token) {
    Assert-Smoke ([IO.File]::Exists($canonicalPath)) 'Production canonical DPAPI store was not created.'
    $encrypted = [IO.File]::ReadAllBytes($canonicalPath)
    $asUtf8 = [Text.Encoding]::UTF8.GetString($encrypted)
    $asUtf16 = [Text.Encoding]::Unicode.GetString($encrypted)
    foreach ($secret in $allTokens) {
        Assert-Smoke (-not $asUtf8.Contains($secret) -and -not $asUtf16.Contains($secret)) 'Canonical credential file contains a plaintext synthetic token.'
    }
    $plaintext = [Security.Cryptography.ProtectedData]::Unprotect($encrypted, $null, [Security.Cryptography.DataProtectionScope]::CurrentUser)
    try {
        $snapshot = [Text.Encoding]::UTF8.GetString($plaintext) | ConvertFrom-Json
        Assert-Smoke ($snapshot.version -eq 1) 'Unexpected canonical snapshot version.'
        Assert-Smoke (@($snapshot.auth.profiles.PSObject.Properties).Count -eq 1) 'Unexpected canonical profile set.'
        Assert-Smoke ($null -ne $snapshot.auth.profiles.PSObject.Properties[$Name]) 'Canonical profile mutation was not shared.'
        Assert-Smoke ($snapshot.auth.profile_ids.PSObject.Properties[$Name].Value -ceq $stableId) 'Migration or mutation changed the stable profile ID.'
        Assert-Smoke ($snapshot.secrets.secrets.PSObject.Properties[$Name].Value -ceq $Token) 'Canonical snapshot selected the wrong credential.'
        Assert-Smoke (@($snapshot.secrets.secrets.PSObject.Properties).Count -eq 1) 'Unexpected canonical secret set.'
        Assert-Smoke ($null -eq $snapshot.PSObject.Properties['legacy_cleanup']) 'Legacy migration cleanup did not finish.'
    } finally { [Array]::Clear($plaintext, 0, $plaintext.Length) }
}

function Set-ExpectedToken([string]$Token, [string]$Label) {
    Write-Json (Join-Path $work 'expected.json') @{ token = $Token; label = $Label }
}

function Assert-Consumer([bool]$Packaged, [string]$Name, [string]$Token, [string]$Label) {
    Set-ExpectedToken $Token $Label
    $result = Invoke-Bt $Packaged @('status', '--all', '--profile', $Name)
    $status = $result.stdout | ConvertFrom-Json
    Assert-Smoke (@($status.profiles).Count -eq 1 -and $status.profiles[0].name -ceq $Name -and $status.profiles[0].status -eq 'ok') 'Credential consumer could not authenticate the shared profile.'
    $requests = @([IO.File]::ReadAllLines((Join-Path $work 'requests.log')))
    Assert-Smoke ($requests -contains $Label) 'Consumer did not reach the isolated login endpoint with the expected current credential.'
    Assert-Smoke (-not ($requests -contains 'REJECTED')) 'The isolated endpoint observed an unexpected credential or request.'
}

try {
    foreach ($entry in @(Get-ChildItem Env:)) {
        if ($entry.Name -like 'BRAINTRUST_*') {
            $originalEnvironment[$entry.Name] = $entry.Value
            [Environment]::SetEnvironmentVariable($entry.Name, $null, 'Process')
        }
    }
    $null = New-Item -ItemType Directory -Path $work
    $ownsWork = $true
    $packageRoot = Join-Path $work 'package'
    $null = New-Item -ItemType Directory -Path $packageRoot
    [Environment]::SetEnvironmentVariable('XDG_CONFIG_HOME', (Join-Path $work 'config'), 'Process')
    # Prevent project-config discovery outside the disposable working directory.
    $null = New-Item -ItemType Directory -Path (Join-Path $work '.bt')
    [IO.File]::WriteAllText((Join-Path $work '.bt\config.json'), '{}', $utf8)

    $csc = Join-Path $env:WINDIR 'Microsoft.NET\Framework64\v4.0.30319\csc.exe'
    & $csc /nologo /target:exe /platform:x64 /reference:System.Web.Extensions.dll `
        ('/out:' + (Join-Path $packageRoot 'Launcher.exe')) $launcherSource
    if ($LASTEXITCODE -ne 0) { throw 'Compiling the MSIX launcher failed.' }
    foreach ($size in @(44, 50, 150)) {
        $bitmap = New-Object Drawing.Bitmap($size, $size)
        $graphics = [Drawing.Graphics]::FromImage($bitmap)
        try {
            $graphics.Clear([Drawing.Color]::SteelBlue)
            $bitmap.Save((Join-Path $packageRoot ('logo' + $size + '.png')), [Drawing.Imaging.ImageFormat]::Png)
        } finally { $graphics.Dispose(); $bitmap.Dispose() }
    }
    # Full-trust packagedClassicApp keeps default filesystem virtualization enabled.
    # https://learn.microsoft.com/windows/msix/desktop/desktop-to-uwp-manual-conversion
    # https://learn.microsoft.com/windows/msix/desktop/desktop-to-uwp-behind-the-scenes
    $manifest = @"
<?xml version="1.0" encoding="utf-8"?>
<Package xmlns="http://schemas.microsoft.com/appx/manifest/foundation/windows10"
 xmlns:uap="http://schemas.microsoft.com/appx/manifest/uap/windows10"
 xmlns:uap10="http://schemas.microsoft.com/appx/manifest/uap/windows10/10"
 xmlns:rescap="http://schemas.microsoft.com/appx/manifest/foundation/windows10/restrictedcapabilities"
 IgnorableNamespaces="uap uap10 rescap">
 <Identity Name="$packageName" Publisher="$publisher" Version="1.0.0.0" ProcessorArchitecture="x64" />
 <Properties><DisplayName>BT Auth Smoke</DisplayName><PublisherDisplayName>Braintrust Test</PublisherDisplayName><Logo>logo50.png</Logo></Properties>
 <Resources><Resource Language="en-us" /></Resources>
 <Dependencies><TargetDeviceFamily Name="Windows.Desktop" MinVersion="10.0.19041.0" MaxVersionTested="10.0.26100.0" /></Dependencies>
 <Applications><Application Id="Fixture" Executable="Launcher.exe" uap10:RuntimeBehavior="packagedClassicApp" uap10:TrustLevel="mediumIL">
  <uap:VisualElements DisplayName="BT Auth Smoke" Description="Synthetic credential smoke fixture" Square150x150Logo="logo150.png" Square44x44Logo="logo44.png" BackgroundColor="transparent" />
 </Application></Applications>
 <Capabilities><rescap:Capability Name="runFullTrust" /></Capabilities>
</Package>
"@
    [IO.File]::WriteAllText((Join-Path $packageRoot 'AppxManifest.xml'), $manifest, $utf8)
    $sdkRoot = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\bin'
    $makeappx = Get-ChildItem -LiteralPath $sdkRoot -Recurse -Filter makeappx.exe |
        Where-Object { $_.FullName -match '\\x64\\makeappx\.exe$' } |
        Sort-Object FullName -Descending | Select-Object -First 1
    Assert-Smoke ($null -ne $makeappx) 'Windows SDK makeappx.exe was not found.'
    $signtool = Join-Path $makeappx.DirectoryName 'signtool.exe'
    Assert-Smoke ([IO.File]::Exists($signtool)) 'Matching Windows SDK signtool.exe was not found.'
    $msix = Join-Path $work 'auth-smoke.msix'
    & $makeappx.FullName pack /d $packageRoot /p $msix /o
    if ($LASTEXITCODE -ne 0) { throw 'MSIX packing failed.' }
    # Same code-signing certificate pattern as test-signing-self-signed.yml; no timestamp network dependency.
    $certificate = New-SelfSignedCertificate -Subject $publisher -Type CodeSigningCert `
        -CertStoreLocation Cert:\CurrentUser\My -KeyUsage DigitalSignature -HashAlgorithm SHA256 -NotAfter (Get-Date).AddDays(1)
    $cer = Join-Path $work 'signing.cer'
    Export-Certificate -Cert $certificate -FilePath $cer | Out-Null
    # TrustedPeople is sufficient for sideloading; never import a root or invoke CryptUI.
    Import-Certificate -FilePath $cer -CertStoreLocation Cert:\LocalMachine\TrustedPeople | Out-Null
    & $signtool sign /fd SHA256 /sha1 $certificate.Thumbprint /s My $msix
    if ($LASTEXITCODE -ne 0) { throw 'MSIX signing failed.' }
    Add-AppxPackage -Path $msix
    $package = Get-AppxPackage -Name $packageName
    Assert-Smoke ($null -ne $package) 'Signed MSIX was not installed.'

    # A loopback-only mock checks actual Authorization headers without recording credentials.
    # No HttpListener URL ACL or external services are required.
    $serverJob = Start-Job -ArgumentList $work -ScriptBlock {
        param($Root)
        $ErrorActionPreference = 'Stop'
        $encoding = New-Object Text.UTF8Encoding($false)
        $listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback, 0)
        $listener.Start()
        try {
            $port = $listener.LocalEndpoint.Port
            $portPath = Join-Path $Root 'port'
            [IO.File]::WriteAllText(($portPath + '.tmp'), [string]$port, $encoding)
            [IO.File]::Move(($portPath + '.tmp'), $portPath)
            while ($true) {
                # Keep Stop-Job responsive instead of blocking indefinitely in a native accept.
                if (-not $listener.Pending()) {
                    Start-Sleep -Milliseconds 25
                    continue
                }
                $client = $listener.AcceptTcpClient()
                try {
                    $client.ReceiveTimeout = 10000
                    $client.SendTimeout = 10000
                    $stream = $client.GetStream()
                    $reader = New-Object IO.StreamReader($stream, [Text.Encoding]::ASCII, $false, 1024, $true)
                    $line = $reader.ReadLine()
                    $validPath = $line -eq 'POST /api/apikey/login HTTP/1.1'
                    $authorization = $null
                    while ($null -ne ($line = $reader.ReadLine()) -and $line -ne '') {
                        if ($line.StartsWith('Authorization:', [StringComparison]::OrdinalIgnoreCase)) {
                            $authorization = $line.Substring(14).Trim()
                        }
                    }
                    $expected = [IO.File]::ReadAllText((Join-Path $Root 'expected.json')) | ConvertFrom-Json
                    $accepted = $validPath -and $authorization -ceq ('Bearer ' + $expected.token)
                    $label = if ($accepted) { $expected.label } else { 'REJECTED' }
                    [IO.File]::AppendAllText((Join-Path $Root 'requests.log'), ($label + "`n"), $encoding)
                    $body = if ($accepted) {
                        @{ org_info = @(@{ id = '00000000-0000-4000-8000-000000000002'; name = 'synthetic-msix-org'; api_url = "http://127.0.0.1:$port" }) } | ConvertTo-Json -Depth 5 -Compress
                    } else { '{"error":"unexpected synthetic credential or request"}' }
                    $bytes = $encoding.GetBytes($body)
                    $status = if ($accepted) { '200 OK' } else { '401 Unauthorized' }
                    $headers = [Text.Encoding]::ASCII.GetBytes("HTTP/1.1 $status`r`nContent-Type: application/json`r`nContent-Length: $($bytes.Length)`r`nConnection: close`r`n`r`n")
                    $stream.Write($headers, 0, $headers.Length)
                    $stream.Write($bytes, 0, $bytes.Length)
                    $reader.Dispose()
                } finally { $client.Dispose() }
            }
        } finally { $listener.Stop() }
    }
    Wait-File (Join-Path $work 'port') 30
    $appUrl = 'http://127.0.0.1:' + [IO.File]::ReadAllText((Join-Path $work 'port'))
    $profileName = 'synthetic-msix-profile'
    $renamedProfile = 'synthetic-msix-renamed'
    $stableId = '00000000-0000-4000-8000-000000000001'
    $staleToken = 'sk-synthetic-msix-stale-' + $runId
    $normalToken = 'sk-synthetic-msix-normal-' + $runId
    $ordinaryRotation = 'sk-synthetic-msix-ordinary-rotation-' + $runId
    $packagedRotation = 'sk-synthetic-msix-packaged-rotation-' + $runId
    $allTokens = @($staleToken, $normalToken, $ordinaryRotation, $packagedRotation)
    $auth = @{ profiles = @{ $profileName = @{ auth_kind = 'api_key'; app_url = $appUrl; org_name = 'synthetic-msix-org'; org_bound = $true } }; profile_ids = @{ $profileName = $stableId } }
    $authJson = $auth | ConvertTo-Json -Depth 10 -Compress
    $staleSecrets = @{ secrets = @{ $profileName = $staleToken } } | ConvertTo-Json -Compress
    $normalSecrets = @{ secrets = @{ $profileName = $normalToken } } | ConvertTo-Json -Compress

    # Modern MSIX redirects NEW AppData files. Seed packaged FIRST, then create real files.
    # Also own seed-created normal files if virtualization is unexpectedly inactive.
    $ownsNormal = $true
    $null = Invoke-Fixture $true @{ action = 'seed'; authJson = $authJson; secretsJson = $staleSecrets }
    Assert-Smoke (-not (Test-Path -LiteralPath $legacyAuth) -and -not (Test-Path -LiteralPath $legacySecrets)) 'MSIX virtualization is inactive: packaged seed modified the ordinary view.'
    $null = New-Item -ItemType Directory -Path $legacyDir -Force
    [IO.File]::WriteAllText($legacyAuth, $authJson, $utf8)
    [IO.File]::WriteAllText($legacySecrets, $normalSecrets, $utf8)
    $inside = Invoke-Fixture $true @{ action = 'probe' }
    $outside = Invoke-Fixture $false @{ action = 'probe' }
    Assert-Smoke ($inside.authJson -ceq $authJson -and $inside.secretsJson -ceq $staleSecrets) 'Packaged probe did not observe the stale private legacy overlay.'
    Assert-Smoke ($outside.authJson -ceq $authJson -and $outside.secretsJson -ceq $normalSecrets) 'Ordinary probe did not observe the distinct current legacy store.'
    Write-Host 'Proved divergent packaged and ordinary legacy credential views.'

    # No bt process has run yet. Only production packaged migration may create the snapshot.
    Assert-Smoke (-not (Test-Path -LiteralPath $canonicalDir)) 'Canonical store exists before first packaged bt invocation.'
    $ownsCanonical = $true
    $first = Invoke-Bt $true @('profiles', 'list')
    $profiles = @($first.stdout | ConvertFrom-Json)
    Assert-Smoke ($profiles.Count -eq 1 -and $profiles[0].name -ceq $profileName) 'Packaged-first migration did not expose the expected profile.'
    Assert-Snapshot $profileName $normalToken
    Assert-Smoke (-not (Test-Path -LiteralPath $legacyAuth) -and -not (Test-Path -LiteralPath $legacySecrets)) 'Normal legacy credential files were not removed after migration.'
    $overlay = Invoke-Fixture $true @{ action = 'probe' }
    Assert-Smoke ($overlay.secretsJson -ceq $staleSecrets) 'The stale overlay disappeared instead of testing canonical-store precedence.'
    Assert-Consumer $true $profileName $normalToken 'packaged-migrated-consumer'
    Assert-Consumer $false $profileName $normalToken 'ordinary-migrated-consumer'
    Write-Host 'Packaged-first migration selected normal credentials, preserved identity, and removed normal legacy files.'

    Set-ExpectedToken $ordinaryRotation 'ordinary-login'
    $null = Invoke-Bt $false @('login', '--save-env-api-key', '--profile', $profileName, '--app-url', $appUrl) @{ BRAINTRUST_API_KEY = $ordinaryRotation }
    Assert-Snapshot $profileName $ordinaryRotation
    Assert-Consumer $true $profileName $ordinaryRotation 'packaged-after-ordinary-login'
    Set-ExpectedToken $packagedRotation 'packaged-login'
    $null = Invoke-Bt $true @('login', '--save-env-api-key', '--profile', $profileName, '--app-url', $appUrl) @{ BRAINTRUST_API_KEY = $packagedRotation }
    Assert-Snapshot $profileName $packagedRotation
    Assert-Consumer $false $profileName $packagedRotation 'ordinary-after-packaged-login'

    $null = Invoke-Bt $false @('profiles', 'rename', $profileName, $renamedProfile)
    Assert-Snapshot $renamedProfile $packagedRotation
    Assert-Consumer $true $renamedProfile $packagedRotation 'packaged-after-ordinary-rename'
    $null = Invoke-Bt $true @('profiles', 'rename', $renamedProfile, $profileName)
    Assert-Snapshot $profileName $packagedRotation
    Assert-Consumer $false $profileName $packagedRotation 'ordinary-after-packaged-rename'
    $overlay = Invoke-Fixture $true @{ action = 'probe' }
    Assert-Smoke ($overlay.secretsJson -ceq $staleSecrets) 'Expected stale overlay no longer exists at final precedence check.'
    Assert-Consumer $true $profileName $packagedRotation 'packaged-final-current-token'
    Assert-Snapshot $profileName $packagedRotation
    Assert-Smoke (-not (Test-Path -LiteralPath $legacyAuth) -and -not (Test-Path -LiteralPath $legacySecrets)) 'A later operation recreated normal legacy credentials.'
    Write-Host 'MSIX auth smoke passed: real login rotations and profile renames share encrypted production storage in both directions.'
} finally {
    # Finish every cleanup even if an earlier cleanup operation fails.
    if ($null -ne $serverJob) {
        try { Stop-Job -Job $serverJob; Remove-Job -Job $serverJob -Force } catch { $cleanupErrors.Add('mock server') }
    }
    try {
        # Query the unique name even if installation completed just before an exception.
        Get-AppxPackage -Name $packageName | Remove-AppxPackage -ErrorAction Stop
    } catch { $cleanupErrors.Add('installed package') }
    if ($null -ne $certificate) {
        foreach ($store in @('Cert:\LocalMachine\TrustedPeople', 'Cert:\CurrentUser\My')) {
            try {
                $certPath = Join-Path $store $certificate.Thumbprint
                if (Test-Path -LiteralPath $certPath) { Remove-Item -LiteralPath $certPath -Force }
            } catch { $cleanupErrors.Add('test certificate') }
        }
    }
    if ($ownsNormal) {
        foreach ($path in @($legacyAuth, $legacySecrets)) {
            try { if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force } } catch { $cleanupErrors.Add('owned legacy fixture') }
        }
    }
    if ($ownsCanonical) {
        try { if (Test-Path -LiteralPath $canonicalDir) { Remove-Item -LiteralPath $canonicalDir -Recurse -Force } } catch { $cleanupErrors.Add('owned canonical fixture') }
    }
    foreach ($parent in @(@{ path = $legacyDir; existed = $hadLegacyDir }, @{ path = $braintrustRoot; existed = $hadBraintrustRoot })) {
        try {
            if (-not $parent.existed -and (Test-Path -LiteralPath $parent.path) -and
                @(Get-ChildItem -LiteralPath $parent.path -Force).Count -eq 0) {
                Remove-Item -LiteralPath $parent.path -Force
            }
        } catch { $cleanupErrors.Add('empty fixture parent') }
    }
    try { if ($ownsWork -and (Test-Path -LiteralPath $work)) { Remove-Item -LiteralPath $work -Recurse -Force } } catch { $cleanupErrors.Add('disposable working directory') }
    foreach ($key in $originalEnvironment.Keys) { [Environment]::SetEnvironmentVariable($key, $originalEnvironment[$key], 'Process') }
    [Environment]::SetEnvironmentVariable('XDG_CONFIG_HOME', $originalXdg, 'Process')
    if ($cleanupErrors.Count -gt 0) { throw ('MSIX smoke cleanup failed for: ' + ($cleanupErrors -join ', ')) }
}
