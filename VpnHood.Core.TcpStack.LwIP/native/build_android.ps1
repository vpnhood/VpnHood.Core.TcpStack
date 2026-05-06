<#
.SYNOPSIS
    Cross-compiles liblwip_shim.so for all Android ABIs using the Android NDK.
.PARAMETER NdkPath
    Path to the Android NDK root. Defaults to $env:ANDROID_NDK_HOME or auto-detected.
.PARAMETER LwipDir
    Path to the lwIP source tree. Defaults to the standard location relative to this repo.
.PARAMETER ApiLevel
    Minimum Android API level. Defaults to 21 (Android 5.0).
.EXAMPLE
    .\build_android.ps1
    .\build_android.ps1 -NdkPath "C:\Android\ndk\27.0.12077973"
#>
param(
    [string]$NdkPath = "",
    [string]$LwipDir = "",
    [int]$ApiLevel = 21
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path $PSCommandPath -Parent

# --- Locate NDK ---
if (-not $NdkPath) {
    $NdkPath = $env:ANDROID_NDK_HOME
}
if (-not $NdkPath) {
    $NdkPath = $env:ANDROID_NDK
}
if (-not $NdkPath) {
    # Auto-detect from common SDK paths
    $sdkCandidates = @(
        "$env:LOCALAPPDATA\Android\Sdk\ndk",
        "C:\Android\Sdk\ndk",
        "C:\Program Files\Android\Sdk\ndk"
    )
    foreach ($sdk in $sdkCandidates) {
        if (Test-Path $sdk) {
            $versions = Get-ChildItem $sdk -Directory | Sort-Object Name -Descending
            if ($versions) {
                $NdkPath = $versions[0].FullName
                break
            }
        }
    }
}

if (-not $NdkPath -or -not (Test-Path $NdkPath)) {
    Write-Error @"
Android NDK not found. Install the NDK and either:
  - Set the ANDROID_NDK_HOME environment variable, or
  - Pass -NdkPath "C:\path\to\ndk"

Install via Android Studio: SDK Manager -> SDK Tools -> NDK (Side by side)
Or via command line: sdkmanager "ndk;27.0.12077973"
"@
    exit 1
}

# --- Locate lwIP ---
if (-not $LwipDir) {
    $LwipDir = Join-Path $ScriptDir "..\..\..\..\..\_Test\lwip"
    $LwipDir = [System.IO.Path]::GetFullPath($LwipDir)
}

if (-not (Test-Path (Join-Path $LwipDir "src\core\tcp.c"))) {
    Write-Error "lwIP source not found at: $LwipDir`nSet -LwipDir to the lwIP root directory."
    exit 1
}

# --- Verify toolchain file ---
$ToolchainFile = Join-Path $NdkPath "build\cmake\android.toolchain.cmake"
if (-not (Test-Path $ToolchainFile)) {
    Write-Error "NDK toolchain file not found at: $ToolchainFile"
    exit 1
}

# --- Find cmake and ninja ---
$cmake = Get-Command cmake -ErrorAction SilentlyContinue
if (-not $cmake) {
    Write-Error "cmake not found in PATH. Install CMake and add it to PATH."
    exit 1
}

$ninja = Get-Command ninja -ErrorAction SilentlyContinue
if (-not $ninja) {
    # Try ninja bundled with NDK
    $ndkNinja = Join-Path $NdkPath "prebuilt\windows-x86_64\bin\ninja.exe"
    if (Test-Path $ndkNinja) {
        $env:PATH = "$([System.IO.Path]::GetDirectoryName($ndkNinja));$env:PATH"
    } else {
        Write-Error "ninja not found. Install Ninja or ensure the NDK prebuilt ninja is present."
        exit 1
    }
}

# --- ABI → RID mapping ---
$targets = @(
    @{ Abi = "arm64-v8a";   Rid = "android-arm64" }
    @{ Abi = "armeabi-v7a"; Rid = "android-arm" }
    @{ Abi = "x86_64";      Rid = "android-x64" }
    @{ Abi = "x86";         Rid = "android-x86" }
)

Write-Host ""
Write-Host "Android NDK : $NdkPath" -ForegroundColor DarkGray
Write-Host "lwIP source : $LwipDir" -ForegroundColor DarkGray
Write-Host "API level   : $ApiLevel" -ForegroundColor DarkGray
Write-Host ""

$failed = @()

foreach ($t in $targets) {
    $abi = $t.Abi
    $rid = $t.Rid
    $buildDir = Join-Path $ScriptDir "build_$abi"
    $outputFile = Join-Path $ScriptDir "..\runtimes\$rid\native\liblwip_shim.so"

    Write-Host "=== Building $abi ($rid) ===" -ForegroundColor Cyan

    New-Item -ItemType Directory -Force -Path $buildDir | Out-Null

    $cmakeArgs = @(
        "-S", $ScriptDir,
        "-B", $buildDir,
        "-G", "Ninja",
        "-DCMAKE_TOOLCHAIN_FILE=$ToolchainFile",
        "-DANDROID_ABI=$abi",
        "-DANDROID_PLATFORM=android-$ApiLevel",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DLWIP_DIR=$LwipDir"
    )

    & cmake @cmakeArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "CMake configure failed for $abi"
        $failed += $abi
        continue
    }

    & cmake --build $buildDir --config Release
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Build failed for $abi"
        $failed += $abi
        continue
    }

    if (Test-Path $outputFile) {
        $size = (Get-Item $outputFile).Length
        Write-Host "  Output: $outputFile ($size bytes)" -ForegroundColor Green
    } else {
        Write-Warning "  Expected output not found: $outputFile"
        $failed += $abi
    }

    Write-Host ""
}

if ($failed.Count -gt 0) {
    Write-Error "Failed ABIs: $($failed -join ', ')"
    exit 1
}

Write-Host "All Android builds succeeded!" -ForegroundColor Green
