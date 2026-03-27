#requires -Version 5.1
# S3-Manager version 2.5 - build 20260323-1225

<#
.SYNOPSIS
    Interactive S3 bucket management tool using AWS.Tools modules.

.DESCRIPTION
    Provides a menu-driven interface for common S3 operations:
    listing, uploading, downloading, deleting bucket content,
    and managing versioned objects (list versions, purge all versions/markers).
    Supports any S3-compatible endpoint.
    Optionally writes a structured audit log of all activity to a text file.

.NOTES
    Requires AWS.Tools.Common and AWS.Tools.S3 (installed automatically if missing).
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Region: Helpers
# ---------------------------------------------------------------------------

function Write-Section {
    [CmdletBinding()]
    param([string]$Text)

    Write-Host ""
    Write-Host "==== $Text ====" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# Region: Audit logging
# ---------------------------------------------------------------------------

# Script-scoped audit log state
$script:AuditLogPath    = $null
$script:AuditLogEnabled = $false
$script:ParallelWorkers  = 2   # upload workers
$script:DownloadWorkers  = 2   # download workers
$script:DeleteWorkers    = 2   # delete batch workers
$script:DeleteBatchSize  = 100  # keys per batch (lower = more parallelism)
$script:ForcePathStyle  = $true

# Debug / performance log - always on by default
$script:DebugLogPath    = Join-Path $env:TEMP 'S3Manager-debug.log'
$script:DebugLogEnabled = $true

function Write-AuditLog {
    <#
    .SYNOPSIS
        Writes a structured entry to the audit log if audit logging is enabled.
        Always writes to host as well (pass -HostColor to colour the host output).
    .PARAMETER Message
        The message to log.
    .PARAMETER Level
        Severity label: INFO, ACTION, PREVIEW, WARN, ERROR. Defaults to INFO.
    .PARAMETER NoHost
        Suppress Write-Host output (use when the caller already printed the line).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Message,

        [ValidateSet('INFO','ACTION','PREVIEW','WARN','ERROR','DEBUG')]
        [string]$Level = 'INFO',

        [string]$HostColor = 'White',

        [switch]$NoHost
    )

    if (-not $NoHost) {
        Write-Host $Message -ForegroundColor $HostColor
    }

    if (-not $script:AuditLogEnabled -or [string]::IsNullOrWhiteSpace($script:AuditLogPath)) {
        return
    }

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $user      = [Environment]::UserName
    $entry     = "$timestamp  [$Level]  [$user]  $Message"

    try {
        $stream = [System.IO.File]::Open(
            $script:AuditLogPath,
            [System.IO.FileMode]::Append,
            [System.IO.FileAccess]::Write,
            [System.IO.FileShare]::ReadWrite)
        $writer = [System.IO.StreamWriter]::new($stream, [System.Text.Encoding]::UTF8)
        $writer.WriteLine($entry)
        $writer.Close()
        $stream.Close()
    }
    catch {
        Write-Host "  [Audit log write failed: $($_.Exception.Message)]" -ForegroundColor DarkRed
    }
}

function Write-AuditHeader {
    <#
    .SYNOPSIS
        Writes a session-start banner to the audit log.
    #>
    [CmdletBinding()]
    param(
        [string]$Bucket,
        [string]$Endpoint
    )

    if (-not $script:AuditLogEnabled) { return }

    $line = '=' * 72
    Write-AuditLog -Message $line           -Level INFO -NoHost
    Write-AuditLog -Message "S3 Manager - Audit Log Session Start" -Level INFO -NoHost
    Write-AuditLog -Message "Bucket   : $Bucket"    -Level INFO -NoHost
    Write-AuditLog -Message "Endpoint : $Endpoint"  -Level INFO -NoHost
    Write-AuditLog -Message "Host     : $([Environment]::MachineName)" -Level INFO -NoHost
    Write-AuditLog -Message "User     : $([Environment]::UserName)"    -Level INFO -NoHost
    Write-AuditLog -Message $line           -Level INFO -NoHost
}

function Import-AwsModules {
    <#
    .SYNOPSIS
        Ensures AWS.Tools.Common and AWS.Tools.S3 are installed and imported.
    #>
    [CmdletBinding()]
    param()

    $requiredModules = @('AWS.Tools.Common', 'AWS.Tools.S3')

    foreach ($module in $requiredModules) {
        $available = Get-Module -ListAvailable -Name $module |
            Sort-Object Version -Descending |
            Select-Object -First 1

        if (-not $available) {
            Write-Host ""
            Write-Host "Module '$module' is not installed." -ForegroundColor Yellow
            Write-Host "It will be installed from the PowerShell Gallery for the current user only." -ForegroundColor Yellow
            $answer = Read-Host "Install '$module' now? [Y/N]"
            if ($answer -notin @('Y', 'y')) {
                throw "Module '$module' is required. Installation declined - cannot continue."
            }
            Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
            $available = Get-Module -ListAvailable -Name $module |
                Sort-Object Version -Descending |
                Select-Object -First 1
        }

        Write-Host "Using $module v$($available.Version)" -ForegroundColor Green
        Import-Module $module -ErrorAction Stop
    }
}

function Read-SecretAsPlainText {
    <#
    .SYNOPSIS
        Prompts for a secret value using SecureString and returns it as plain text.
        Memory is zeroed immediately after conversion.
    #>
    [CmdletBinding()]
    param(
        [string]$Prompt = 'Secret key'
    )

    $secure = Read-Host $Prompt -AsSecureString
    $ptr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($ptr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($ptr)
    }
}

function Format-S3Prefix {
    <#
    .SYNOPSIS
        Normalises a user-supplied S3 prefix to the form "folder/subfolder/"
        Returns an empty string for a root prefix.
    #>
    [CmdletBinding()]
    param([string]$Prefix)

    if ([string]::IsNullOrWhiteSpace($Prefix)) { return '' }

    $p = $Prefix -replace '\\', '/'
    $p = $p.Trim('/')

    if ([string]::IsNullOrWhiteSpace($p)) { return '' }

    return "$p/"
}

function Get-MimeType {
    <#
    .SYNOPSIS
        Returns a MIME type string for a given file extension.
        Falls back to application/octet-stream for unknown types.
    #>
    [CmdletBinding()]
    param(
        [string]$Extension
    )

    $map = @{
        '.html' = 'text/html'
        '.htm'  = 'text/html'
        '.css'  = 'text/css'
        '.js'   = 'application/javascript'
        '.mjs'  = 'application/javascript'
        '.json' = 'application/json'
        '.xml'  = 'application/xml'
        '.txt'  = 'text/plain'
        '.csv'  = 'text/csv'
        '.md'   = 'text/markdown'
        '.jpg'  = 'image/jpeg'
        '.jpeg' = 'image/jpeg'
        '.png'  = 'image/png'
        '.gif'  = 'image/gif'
        '.svg'  = 'image/svg+xml'
        '.webp' = 'image/webp'
        '.ico'  = 'image/x-icon'
        '.pdf'  = 'application/pdf'
        '.zip'  = 'application/zip'
        '.gz'   = 'application/gzip'
        '.tar'  = 'application/x-tar'
        '.mp4'  = 'video/mp4'
        '.mp3'  = 'audio/mpeg'
        '.woff' = 'font/woff'
        '.woff2'= 'font/woff2'
        '.ttf'  = 'font/ttf'
    }

    if ([string]::IsNullOrWhiteSpace($Extension)) { return 'application/octet-stream' }
    $ext = $Extension.ToLower()
    if ($map.ContainsKey($ext)) { return $map[$ext] }
    return 'application/octet-stream'
}

function New-S3Client {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [pscustomobject]$Session
    )

    $s3Config = New-Object Amazon.S3.AmazonS3Config
    $s3Config.ServiceURL = $Session.Endpoint
    if ($Session.PSObject.Properties['ForcePathStyle']) { $s3Config.ForcePathStyle = [bool]$Session.ForcePathStyle }
    return [Amazon.S3.AmazonS3Client]::new($Session.Credential, $s3Config)
}

function Get-S3ObjectPage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [pscustomobject]$Session,
        [string]$Prefix = '',
        [string]$ContinuationToken
    )

    $client = $null
    try {
        $client = New-S3Client -Session $Session
        $request = [Amazon.S3.Model.ListObjectsV2Request]::new()
        $request.BucketName = $Session.Bucket
        $request.Prefix = $Prefix
        $request.MaxKeys = 1000
        if (-not [string]::IsNullOrWhiteSpace($ContinuationToken)) {
            $request.ContinuationToken = $ContinuationToken
        }

        $response = $client.ListObjectsV2Async($request).GetAwaiter().GetResult()
        $items = @(
            $response.S3Objects | Where-Object { -not $_.Key.EndsWith('/') } | ForEach-Object {
                [pscustomobject]@{
                    Key          = $_.Key
                    Size         = [long]$_.Size
                    LastModified = $_.LastModified
                    StorageClass = $_.StorageClass
                }
            }
        )

        return [pscustomobject]@{
            Items                 = $items
            IsTruncated           = [bool]$response.IsTruncated
            NextContinuationToken = $response.NextContinuationToken
        }
    }
    finally {
        if ($null -ne $client) { $client.Dispose() }
    }
}

function Get-S3VersionPage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [pscustomobject]$Session,
        [string]$Prefix = '',
        [string]$KeyMarker,
        [string]$VersionIdMarker
    )

    $client = $null
    try {
        $client = New-S3Client -Session $Session
        $request = [Amazon.S3.Model.ListVersionsRequest]::new()
        $request.BucketName = $Session.Bucket
        $request.Prefix = $Prefix
        $request.MaxKeys = 1000
        if (-not [string]::IsNullOrWhiteSpace($KeyMarker)) {
            $request.KeyMarker = $KeyMarker
        }
        if (-not [string]::IsNullOrWhiteSpace($VersionIdMarker)) {
            $request.VersionIdMarker = $VersionIdMarker
        }

        $response = $client.ListVersionsAsync($request).GetAwaiter().GetResult()
        $items = New-Object System.Collections.Generic.List[object]

        foreach ($v in $response.Versions) {
            if (-not $v.Key.EndsWith('/')) {
                $items.Add([pscustomobject]@{
                    Key            = $v.Key
                    VersionId      = $v.VersionId
                    IsDeleteMarker = $false
                    LastModified   = $v.LastModified
                    Size           = [long]$v.Size
                })
            }
        }

        foreach ($m in $response.DeleteMarkers) {
            if (-not $m.Key.EndsWith('/')) {
                $items.Add([pscustomobject]@{
                    Key            = $m.Key
                    VersionId      = $m.VersionId
                    IsDeleteMarker = $true
                    LastModified   = $m.LastModified
                    Size           = 0L
                })
            }
        }

        return [pscustomobject]@{
            Items               = @($items)
            IsTruncated         = [bool]$response.IsTruncated
            NextKeyMarker       = $response.NextKeyMarker
            NextVersionIdMarker = $response.NextVersionIdMarker
        }
    }
    finally {
        if ($null -ne $client) { $client.Dispose() }
    }
}


function Write-DebugLog {
    <#
    .SYNOPSIS
        Writes a DEBUG or PERF entry to the debug log file.
        Always writes regardless of audit log state.
        Does NOT write to host - debug output stays in the file only.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [string]$Message,

        [ValidateSet('DEBUG','PERF','ERROR')]
        [string]$Level = 'DEBUG'
    )

    if (-not $script:DebugLogEnabled -or [string]::IsNullOrWhiteSpace($script:DebugLogPath)) {
        return
    }

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'
    $user      = [Environment]::UserName
    $entry     = "$timestamp  [$Level]  [$user]  $Message"

    try {
        $stream = [System.IO.File]::Open(
            $script:DebugLogPath,
            [System.IO.FileMode]::Append,
            [System.IO.FileAccess]::Write,
            [System.IO.FileShare]::ReadWrite)
        $writer = [System.IO.StreamWriter]::new($stream, [System.Text.Encoding]::UTF8)
        $writer.WriteLine($entry)
        $writer.Close()
        $stream.Close()
    }
    catch {
        # Silently ignore debug log write failures
    }
}

# ---------------------------------------------------------------------------
# Region: Versioning helpers
# ---------------------------------------------------------------------------

function Get-S3VersioningStatus {
    <#
    .SYNOPSIS
        Returns the versioning configuration for the bucket.
        Possible Status values: '' (never enabled), 'Enabled', 'Suspended'.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    try {
        $config = Get-S3BucketVersioning `
            -BucketName  $Session.Bucket `
            -EndpointUrl $Session.Endpoint `
            -Credential  $Session.Credential

        # Status is an enum; cast to string so callers can use -eq 'Enabled' etc.
        return [string]$config.Status
    }
    catch {
        # Not all S3-compatible endpoints support versioning; treat as disabled.
        Write-Verbose "Could not retrieve versioning status: $($_.Exception.Message)"
        return ''
    }
}

function Show-VersioningStatus {
    <#
    .SYNOPSIS
        Displays the current versioning status of the bucket.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "Bucket versioning status"

    $status = Get-S3VersioningStatus -Session $Session

    $label = switch ($status) {
        'Enabled'   { 'Enabled'            }
        'Suspended' { 'Suspended'          }
        default     { 'Never enabled / off' }
    }

    $color = if ($status -eq 'Enabled') { 'Green' } else { 'Yellow' }
    Write-Host "Versioning: $label" -ForegroundColor $color
}

function Show-BucketVersions {
    <#
    .SYNOPSIS
        Lists all versions and delete markers in the bucket for a given prefix.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "List object versions"

    $status = Get-S3VersioningStatus -Session $Session
    if ($status -ne 'Enabled' -and $status -ne 'Suspended') {
        Write-Host "Versioning is not enabled on this bucket." -ForegroundColor Yellow
        return
    }

    $prefixInput = Read-Host "Enter prefix/folder to list versions for (leave empty for root)"
    $prefix = Format-S3Prefix -Prefix $prefixInput

    $response = Get-S3Version `
        -BucketName  $Session.Bucket `
        -KeyPrefix   $prefix `
        -EndpointUrl $Session.Endpoint `
        -Credential  $Session.Credential

    $versions = $response | Where-Object { $_ -is [Amazon.S3.Model.S3ObjectVersion] -and -not $_.Key.EndsWith('/') }
    $markers  = $response | Where-Object { $_ -is [Amazon.S3.Model.DeleteMarkerEntry] }

    if (-not $versions -and -not $markers) {
        Write-Host "No versions or delete markers found." -ForegroundColor Yellow
        return
    }

    if ($versions) {
        Write-Host ""
        Write-Host "--- Object versions ---" -ForegroundColor Cyan
        $versions |
            Select-Object Key,
                VersionId,
                @{ N='Size (KB)';   E={ [math]::Round($_.Size / 1KB, 2) } },
                LastModified,
                IsLatest |
            Sort-Object Key, LastModified |
            Format-Table -AutoSize
    }

    if ($markers) {
        Write-Host ""
        Write-Host "--- Delete markers ---" -ForegroundColor Cyan
        $markers |
            Select-Object Key, VersionId, LastModified, IsLatest |
            Sort-Object Key, LastModified |
            Format-Table -AutoSize
    }

    Write-Host "Total: $($versions.Count) version(s), $($markers.Count) delete marker(s)." -ForegroundColor Green
}

function Remove-AllObjectVersions {
    <#
    .SYNOPSIS
        Permanently removes ALL versions and delete markers for objects under a
        given prefix. This cannot be undone.
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'None')]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "Permanently delete all object versions"

    $status = Get-S3VersioningStatus -Session $Session
    if ($status -ne 'Enabled' -and $status -ne 'Suspended') {
        Write-Host "Versioning is not enabled on this bucket. Use option 6 to delete current objects." -ForegroundColor Yellow
        return
    }

    $prefixInput = Read-Host "Enter prefix/folder (leave empty for the ENTIRE bucket)"
    $prefix = Format-S3Prefix -Prefix $prefixInput

    Write-Host ""
    $scope = if ([string]::IsNullOrWhiteSpace($prefix)) {
        "ALL versions and delete markers in bucket '$($Session.Bucket)'"
    } else {
        "all versions and delete markers under prefix '$prefix' in bucket '$($Session.Bucket)'"
    }

    Write-Host "WARNING: This will PERMANENTLY and IRRECOVERABLY delete $scope." -ForegroundColor Red
    Write-Host "         There is no way to undo this operation." -ForegroundColor Red
    Write-Host ""

    $confirm1 = Read-Host "Type DELETE to proceed"
    if ($confirm1 -ne 'DELETE') { Write-Host "Cancelled." -ForegroundColor Yellow; return }
    $confirm2 = Read-Host "Are you absolutely sure? Type YES"
    if ($confirm2 -ne 'YES') { Write-Host "Cancelled." -ForegroundColor Yellow; return }

    $listCalls     = 0
    $pageNo        = 0
    $totalSeen     = 0
    $totalDeleted  = 0
    $totalFailed   = 0
    $deleteApi     = 0
    $stopwatch     = [System.Diagnostics.Stopwatch]::StartNew()
    $keyMarker     = $null
    $versionMarker = $null
    $foundAny      = $false

    if ($PSCmdlet.ShouldProcess("versions/delete-markers in s3://$($Session.Bucket)", 'Permanent delete')) {
        do {
            $page = Get-S3VersionPage -Session $Session -Prefix $prefix -KeyMarker $keyMarker -VersionIdMarker $versionMarker
            $listCalls++
            $pageNo++
            $items = @($page.Items)

            if ($items.Count -gt 0) {
                $foundAny = $true
                $totalSeen += $items.Count
                Write-Host "Processing version page $pageNo ($($items.Count) item(s), $totalSeen discovered so far)..." -ForegroundColor Cyan
                $entries = @($items | ForEach-Object { @{ Key = $_.Key; VersionId = $_.VersionId } })
                $result  = Invoke-S3BatchDelete -Session $Session -Entries $entries -Activity "Purging versions (page $pageNo)"

                $rAuditLines   = if ($result -is [hashtable] -and $result.ContainsKey('AuditLines')) { $result['AuditLines'] } elseif ($result.PSObject.Properties['AuditLines']) { $result.AuditLines } else { @() }
                $rErrorLines   = if ($result -is [hashtable] -and $result.ContainsKey('ErrorLines')) { $result['ErrorLines'] } elseif ($result.PSObject.Properties['ErrorLines']) { $result.ErrorLines } else { @() }
                $rDeletedCount = if ($result -is [hashtable] -and $result.ContainsKey('DeletedCount')) { [int]$result['DeletedCount'] } elseif ($result.PSObject.Properties['DeletedCount']) { [int]$result.DeletedCount } else { 0 }
                $rFailedCount  = if ($result -is [hashtable] -and $result.ContainsKey('FailedCount')) { [int]$result['FailedCount'] } elseif ($result.PSObject.Properties['FailedCount']) { [int]$result.FailedCount } else { 0 }
                $rDeleteApi    = if ($result -is [hashtable] -and $result.ContainsKey('DeleteApiCalls')) { [int]$result['DeleteApiCalls'] } elseif ($result.PSObject.Properties['DeleteApiCalls']) { [int]$result.DeleteApiCalls } else { 0 }

                $totalDeleted += $rDeletedCount
                $totalFailed  += $rFailedCount
                $deleteApi    += $rDeleteApi

                foreach ($entry in $rAuditLines) { Write-AuditLog -Message ($entry -replace '^DELETE:', 'PURGE:') -Level ACTION -HostColor Gray }
                foreach ($entry in $rErrorLines) { Write-AuditLog -Message $entry -Level ERROR -HostColor Red }
            }

            $keyMarker     = $page.NextKeyMarker
            $versionMarker = $page.NextVersionIdMarker
        } while ($page.IsTruncated)

        $stopwatch.Stop()

        if (-not $foundAny) {
            Write-Host "No versions or delete markers found." -ForegroundColor Yellow
            return
        }

        $elapsedSec = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
        $totalCalls = $listCalls + $deleteApi
        $summary = "Purge complete. $totalDeleted removed, $totalFailed failed. | ${elapsedSec}s | Workers: $script:DeleteWorkers | Pages: $pageNo | API calls: LIST x$listCalls, DeleteObjects x$deleteApi (total: $totalCalls) | Batch size: $script:DeleteBatchSize | Streaming: page-by-page | Logs include batch/thread IDs"
        Write-AuditLog -Message $summary -Level ACTION -HostColor Green
        Write-DebugLog -Message "PURGE COMPLETE: $summary" -Level PERF
    }
    else {
        do {
            $page = Get-S3VersionPage -Session $Session -Prefix $prefix -KeyMarker $keyMarker -VersionIdMarker $versionMarker
            $pageNo++
            foreach ($entry in $page.Items) {
                $type = if ($entry.IsDeleteMarker) { '[marker]' } else { '[version]' }
                Write-Host "  What if: Purging $type $($entry.Key) [$($entry.VersionId)]"
                Write-AuditLog -Message "PREVIEW-PURGE $type s3://$($Session.Bucket)/$($entry.Key) [VersionId: $($entry.VersionId)]" -Level PREVIEW -NoHost
                $foundAny = $true
            }
            $keyMarker     = $page.NextKeyMarker
            $versionMarker = $page.NextVersionIdMarker
        } while ($page.IsTruncated)

        if (-not $foundAny) { Write-Host "No versions or delete markers found." -ForegroundColor Yellow }
    }
}

# ---------------------------------------------------------------------------
# Region: Session
# ---------------------------------------------------------------------------

function Get-S3ConfigPath {
    $dir = Join-Path $env:APPDATA 'S3Manager'
    if (-not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    return Join-Path $dir 'config.json'
}

function Save-S3Config {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][pscustomobject]$Session,
        [string]$AccessKey = '',
        [string]$SecretKey = ''
    )

    $configPath = Get-S3ConfigPath
    @{
        '_WARNING'      = 'CREDENTIALS STORED IN PLAINTEXT - DEBUG MODE ONLY - REMOVE BEFORE PRODUCTION USE'
        Endpoint        = $Session.Endpoint
        Bucket          = $Session.Bucket
        AccessKey       = $AccessKey
        SecretKey       = $SecretKey
        ParallelWorkers  = $script:ParallelWorkers
        DownloadWorkers  = $script:DownloadWorkers
        DeleteWorkers    = $script:DeleteWorkers
        DeleteBatchSize  = $script:DeleteBatchSize
        ForcePathStyle  = $script:ForcePathStyle
        AuditLogPath    = $script:AuditLogPath
        AuditLogEnabled = $script:AuditLogEnabled
        DebugLogPath    = $script:DebugLogPath
        DebugLogEnabled = $script:DebugLogEnabled
        SavedAt         = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
    } | ConvertTo-Json | Set-Content -LiteralPath $configPath -Encoding UTF8
    Write-Host "  Configuration saved to: $configPath" -ForegroundColor DarkGray
    Write-Host "  WARNING: Access key and secret key stored in plaintext." -ForegroundColor Yellow
    Write-DebugLog -Message "Config saved to $configPath (credentials included - debug mode)" -Level DEBUG
}

function Load-S3Config {
    [CmdletBinding()]
    param()

    $configPath = Get-S3ConfigPath
    if (-not (Test-Path -LiteralPath $configPath)) { return $null }
    try {
        return (Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json)
    }
    catch {
        Write-Host "  Could not read saved config: $($_.Exception.Message)" -ForegroundColor Yellow
        return $null
    }
}

function Remove-S3Config {
    [CmdletBinding()]
    param()

    $configPath = Get-S3ConfigPath
    if (Test-Path -LiteralPath $configPath) {
        Remove-Item -LiteralPath $configPath -Force
        Write-Host "Saved configuration deleted." -ForegroundColor Yellow
    } else {
        Write-Host "No saved configuration found." -ForegroundColor Yellow
    }
}

function Get-S3Session {
    <#
    .SYNOPSIS
        Interactively collects S3 connection settings and returns a session object.
        Plain-text credentials are NOT stored; only the AWS credential object is kept.
    #>
    [CmdletBinding()]
    param()

    Write-Section "S3 connection settings"

    $saved = Load-S3Config
    if ($saved) {
        $savedAt = if ($saved.PSObject.Properties['SavedAt']) { $saved.SavedAt } else { 'unknown' }
        Write-Host "  Saved config found (from $savedAt):" -ForegroundColor DarkGray
        Write-Host "    Endpoint   : $($saved.Endpoint)" -ForegroundColor DarkGray
        Write-Host "    Bucket     : $($saved.Bucket)" -ForegroundColor DarkGray
        $savedAccessKey = if ($saved.PSObject.Properties['AccessKey']) { $saved.AccessKey } else { '' }
        $savedSecretKey = if ($saved.PSObject.Properties['SecretKey']) { $saved.SecretKey } else { '' }
        $savedEndpoint  = if ($saved.PSObject.Properties['Endpoint'])  { $saved.Endpoint  } else { '' }
        $savedBucket    = if ($saved.PSObject.Properties['Bucket'])    { $saved.Bucket    } else { '' }

        if (-not [string]::IsNullOrWhiteSpace($savedAccessKey)) {
            Write-Host "    Access Key : $savedAccessKey" -ForegroundColor DarkGray
            Write-Host "    Secret Key : ********" -ForegroundColor DarkGray
        }
        Write-Host ""
    } else {
        $savedAccessKey = ''
        $savedSecretKey = ''
        $savedEndpoint  = ''
        $savedBucket    = ''
    }

    # If all four values are saved, load silently without prompting
    $allSaved = (-not [string]::IsNullOrWhiteSpace($savedAccessKey)) -and
                (-not [string]::IsNullOrWhiteSpace($savedSecretKey)) -and
                (-not [string]::IsNullOrWhiteSpace($savedEndpoint))  -and
                (-not [string]::IsNullOrWhiteSpace($savedBucket))

    if ($allSaved) {
        Write-Host "  All values loaded from saved config. Press Enter to connect, or type a new value to override." -ForegroundColor DarkGray
        Write-Host ""
        $overrideKey    = Read-Host "  Access Key       [saved - Enter to keep, or type new]"
        $accessKey      = if ([string]::IsNullOrWhiteSpace($overrideKey))    { $savedAccessKey } else { $overrideKey }

        $overrideSecret = Read-Host "  Secret Key       [saved - Enter to keep, or type new]"
        $secretKey      = if ([string]::IsNullOrWhiteSpace($overrideSecret)) { $savedSecretKey } else { $overrideSecret }

        $overrideEp     = Read-Host "  Endpoint URL     [saved: $savedEndpoint - Enter to keep]"
        $endpoint       = if ([string]::IsNullOrWhiteSpace($overrideEp))     { $savedEndpoint  } else { $overrideEp }

        $overrideBucket = Read-Host "  Bucket name      [saved: $savedBucket - Enter to keep]"
        $bucket         = if ([string]::IsNullOrWhiteSpace($overrideBucket)) { $savedBucket    } else { $overrideBucket }
    } else {
        # Partial or no saved config - prompt for each with fallback defaults
        if (-not [string]::IsNullOrWhiteSpace($savedAccessKey)) {
            $overrideKey = Read-Host "Enter Access Key [$savedAccessKey]"
            $accessKey   = if ([string]::IsNullOrWhiteSpace($overrideKey)) { $savedAccessKey } else { $overrideKey }
        } else {
            $accessKey = Read-Host "Enter Access Key"
        }

        if (-not [string]::IsNullOrWhiteSpace($savedSecretKey)) {
            $overrideSecret = Read-Host "Enter Secret Key [saved - Enter to keep, or type new]"
            $secretKey      = if ([string]::IsNullOrWhiteSpace($overrideSecret)) { $savedSecretKey } else { $overrideSecret }
        } else {
            $secretKey = Read-SecretAsPlainText -Prompt "Enter Secret Key"
        }

        if (-not [string]::IsNullOrWhiteSpace($savedEndpoint)) {
            $overrideEp = Read-Host "Enter Endpoint URL [$savedEndpoint]"
            $endpoint   = if ([string]::IsNullOrWhiteSpace($overrideEp)) { $savedEndpoint } else { $overrideEp }
        } else {
            $endpoint = Read-Host "Enter Endpoint URL  (e.g. https://s3-nl03.cloud.dm-p.com)"
        }

        if (-not [string]::IsNullOrWhiteSpace($savedBucket)) {
            $overrideBucket = Read-Host "Enter Bucket name [$savedBucket]"
            $bucket         = if ([string]::IsNullOrWhiteSpace($overrideBucket)) { $savedBucket } else { $overrideBucket }
        } else {
            $bucket = Read-Host "Enter Bucket name"
        }
    }

    if ([string]::IsNullOrWhiteSpace($endpoint)) {
        throw "Endpoint URL cannot be empty."
    }

    if ($endpoint -notmatch '^https?://') {
        $endpoint = "https://$endpoint"
    }

    $credential = New-Object Amazon.Runtime.BasicAWSCredentials($accessKey, $secretKey)

    $newSession = [pscustomobject]@{
        Endpoint       = $endpoint
        Bucket         = $bucket
        Credential     = $credential
        ForcePathStyle = $script:ForcePathStyle
    }

    # Restore saved non-credential settings
    if ($saved) {
        $savedUpWorkers  = if ($saved.PSObject.Properties['ParallelWorkers'])  { $saved.ParallelWorkers }  else { $null }
        $savedDlWorkers  = if ($saved.PSObject.Properties['DownloadWorkers'])  { $saved.DownloadWorkers }  else { $null }
        $savedDelWorkers = if ($saved.PSObject.Properties['DeleteWorkers'])    { $saved.DeleteWorkers }    else { $null }
        $savedForcePath  = if ($saved.PSObject.Properties['ForcePathStyle'])   { [bool]$saved.ForcePathStyle } else { $true }
        $savedAuditPath  = if ($saved.PSObject.Properties['AuditLogPath'])     { $saved.AuditLogPath }     else { '' }
        $savedAuditOn    = if ($saved.PSObject.Properties['AuditLogEnabled'])  { $saved.AuditLogEnabled }  else { $false }
        $savedDebugPath  = if ($saved.PSObject.Properties['DebugLogPath'])     { $saved.DebugLogPath }     else { '' }
        $savedDebugOn    = if ($saved.PSObject.Properties['DebugLogEnabled'])  { $saved.DebugLogEnabled }  else { $true }

        if ($null -ne $savedUpWorkers  -and [int]$savedUpWorkers  -gt 0) { $script:ParallelWorkers = [int]$savedUpWorkers }
        if ($null -ne $savedDlWorkers  -and [int]$savedDlWorkers  -gt 0) { $script:DownloadWorkers = [int]$savedDlWorkers }
        if ($null -ne $savedDelWorkers -and [int]$savedDelWorkers -gt 0) { $script:DeleteWorkers   = [int]$savedDelWorkers }
        $savedBatchSize = if ($saved.PSObject.Properties['DeleteBatchSize']) { $saved.DeleteBatchSize } else { $null }
        if ($null -ne $savedBatchSize -and [int]$savedBatchSize -gt 0) { $script:DeleteBatchSize = [int]$savedBatchSize }
        $script:ForcePathStyle = [bool]$savedForcePath
        Write-Host "  Restored workers - Upload: $script:ParallelWorkers  Download: $script:DownloadWorkers  Delete: $script:DeleteWorkers" -ForegroundColor DarkGray
        Write-Host "  Force path style          : $script:ForcePathStyle" -ForegroundColor DarkGray

        if ($savedAuditOn -and -not [string]::IsNullOrWhiteSpace($savedAuditPath)) {
            $script:AuditLogPath    = $savedAuditPath
            $script:AuditLogEnabled = $true
            Write-Host "  Restored audit log        : $script:AuditLogPath" -ForegroundColor DarkGray
        }
        if (-not [string]::IsNullOrWhiteSpace($savedDebugPath)) {
            $script:DebugLogPath    = $savedDebugPath
            $script:DebugLogEnabled = [bool]$savedDebugOn
        }
        Write-Host "  Debug log                 : $script:DebugLogPath" -ForegroundColor DarkGray
    }

    $saveChoice = Read-Host "Save connection settings to config.json for reuse? [Y/N]"
    if ($saveChoice -in @('Y', 'y')) {
        Save-S3Config -Session $newSession -AccessKey $accessKey -SecretKey $secretKey
    }
    else {
        Write-Host "  Configuration not saved." -ForegroundColor DarkGray
        Write-DebugLog -Message "Config save skipped by user" -Level DEBUG
    }

    # Zero credentials from memory now that the credential object is built
    $secretKey = $null
    $accessKey = $null

    Write-AuditHeader -Bucket $newSession.Bucket -Endpoint $newSession.Endpoint
    Write-AuditLog -Message "Connection configured for bucket '$($newSession.Bucket)' at $($newSession.Endpoint)" -Level INFO -NoHost
    Write-DebugLog -Message "Session init complete. Bucket: $($newSession.Bucket) Endpoint: $($newSession.Endpoint) Workers: Upload=$script:ParallelWorkers Download=$script:DownloadWorkers Delete=$script:DeleteWorkers" -Level DEBUG

    return $newSession
}

# ---------------------------------------------------------------------------
# Region: S3 Operations
# ---------------------------------------------------------------------------


function Test-S3ObjectExists {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [pscustomobject]$Session,
        [Parameter(Mandatory)] [string]$Key
    )

    $client = $null
    try {
        $client = New-S3Client -Session $Session
        $request = [Amazon.S3.Model.GetObjectMetadataRequest]::new()
        $request.BucketName = $Session.Bucket
        $request.Key        = $Key
        $null = $client.GetObjectMetadataAsync($request).GetAwaiter().GetResult()
        return $true
    }
    catch [Amazon.S3.AmazonS3Exception] {
        if ($_.Exception.StatusCode -eq [System.Net.HttpStatusCode]::NotFound) { return $false }
        throw
    }
    finally {
        if ($null -ne $client) { $client.Dispose() }
    }
}

function Test-S3Connection {
    <#
    .SYNOPSIS
        Verifies that the configured credentials can reach the S3 endpoint.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "Testing S3 connection"

    $client = $null
    try {
        $client = New-S3Client -Session $Session

        $bucketLocationRequest = [Amazon.S3.Model.GetBucketLocationRequest]::new()
        $bucketLocationRequest.BucketName = $Session.Bucket
        $null = $client.GetBucketLocationAsync($bucketLocationRequest).GetAwaiter().GetResult()

        $listRequest = [Amazon.S3.Model.ListObjectsV2Request]::new()
        $listRequest.BucketName = $Session.Bucket
        $listRequest.MaxKeys    = 1
        $null = $client.ListObjectsV2Async($listRequest).GetAwaiter().GetResult()

        Write-AuditLog -Message "Connection test PASSED for endpoint $($Session.Endpoint), bucket '$($Session.Bucket)' | API calls: GetBucketLocation x1, ListObjectsV2 x1" -Level INFO -HostColor Green
        return $true
    }
    catch {
        Write-AuditLog -Message "Connection test FAILED for bucket '$($Session.Bucket)': $($_.Exception.Message)" -Level ERROR -HostColor Red
        return $false
    }
    finally {
        if ($null -ne $client) { $client.Dispose() }
    }
}

function Show-BucketContent {
    <#
    .SYNOPSIS
        Lists objects in the bucket, optionally scoped to a prefix.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "List bucket content"

    $prefixInput = Read-Host "Enter prefix/folder in bucket (leave empty for root)"
    $prefix = Format-S3Prefix -Prefix $prefixInput

    $objects = Get-S3Object `
        -BucketName $Session.Bucket `
        -KeyPrefix  $prefix `
        -EndpointUrl $Session.Endpoint `
        -Credential  $Session.Credential |
        Where-Object { -not $_.Key.EndsWith('/') }

    Write-AuditLog -Message "LIST: s3://$($Session.Bucket)/$prefix | API calls: LIST x1" -Level INFO -NoHost

    if (-not $objects) {
        Write-Host "No objects found." -ForegroundColor Yellow
        return
    }

    $objects |
        Select-Object Key,
            @{ N='Size (KB)'; E={ [math]::Round($_.Size / 1KB, 2) } },
            LastModified,
            StorageClass |
        Format-Table -AutoSize
}

function Copy-FolderToS3Bucket {
    <#
    .SYNOPSIS
        Uploads all files in a local folder (recursively) to an S3 bucket prefix.
        Uses 2 parallel runspaces with TransferUtility for multipart throughput.
    .PARAMETER Session
        S3 session object returned by Get-S3Session.
    .PARAMETER Overwrite
        When specified, existing objects are overwritten without prompting.
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session,
        [switch]$Overwrite
    )

    Write-Section "Upload folder to bucket"

    $localFolder = Read-Host "Enter local folder path to upload"
    if (-not (Test-Path -LiteralPath $localFolder)) {
        throw "Local folder does not exist: $localFolder"
    }

    $s3Prefix = Format-S3Prefix -Prefix (Read-Host "Enter target prefix/folder in bucket (optional)")
    $rootPath = (Resolve-Path -LiteralPath $localFolder).Path.TrimEnd('\')
    $files    = Get-ChildItem -LiteralPath $rootPath -File -Recurse
    $total    = $files.Count

    if ($total -eq 0) {
        Write-Host "No files found in local folder." -ForegroundColor Yellow
        return
    }

    # Resolve work items on the main thread (conflict prompts need stdin)
    $workItems = [System.Collections.Generic.List[hashtable]]::new()
    $skipped   = 0

    foreach ($file in $files) {
        $relativePath = $file.FullName.Substring($rootPath.Length).TrimStart('\')
        $key          = $s3Prefix + ($relativePath -replace '\\', '/')
        $contentType  = Get-MimeType -Extension $file.Extension

        $objectExists = $false
        if (-not $Overwrite) {
            $objectExists = Test-S3ObjectExists -Session $Session -Key $key
        }

        if ($objectExists -and -not $Overwrite) {
            $answer = Read-Host "  '$key' already exists. Overwrite? [Y/N]"
            if ($answer -notin @('Y','y')) {
                Write-Host "  Skipped: $key" -ForegroundColor Yellow
                $skipped++
                continue
            }
        }

        if ($PSCmdlet.ShouldProcess("s3://$($Session.Bucket)/$key", "Upload '$($file.FullName)'")) {
            $workItems.Add(@{
                FilePath     = $file.FullName
                RelativePath = $relativePath
                Key          = $key
                ContentType  = if ([string]::IsNullOrWhiteSpace($contentType)) { 'application/octet-stream' } else { $contentType }
                SizeBytes    = if ($null -eq $file.Length) { 0L } else { [long]$file.Length }
            })
        } else {
            Write-Host "  What if: Uploading $relativePath -> $key"
            Write-AuditLog -Message "PREVIEW-UPLOAD: $($file.FullName) -> s3://$($Session.Bucket)/$key [$contentType]" -Level PREVIEW -NoHost
        }
    }

    if ($workItems.Count -eq 0) {
        Write-Host "Nothing to upload." -ForegroundColor Yellow
        return
    }

    Write-Host ""
    Write-Host "Uploading $($workItems.Count) file(s) using $script:ParallelWorkers parallel worker(s)..." -ForegroundColor Cyan

    $errors     = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
    $auditItems = [System.Collections.Concurrent.ConcurrentBag[string]]::new()

    $uploadScript = {
        param($Item, $Bucket, $Endpoint, $Credential, $DebugLogPath, $TaskId)

        $debugLines = [System.Collections.Generic.List[string]]::new()
        $sw         = [System.Diagnostics.Stopwatch]::StartNew()
        $threadId   = [System.Threading.Thread]::CurrentThread.ManagedThreadId
        function Stamp { return (Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff') }

        try {
            Import-Module AWS.Tools.Common -ErrorAction Stop
            Import-Module AWS.Tools.S3 -ErrorAction Stop
            function FlushDebug { param([string]$Line); $debugLines.Add($Line); if (-not [string]::IsNullOrWhiteSpace($DebugLogPath)) { try {
                    $fs = [System.IO.File]::Open($DebugLogPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
                    $fw = [System.IO.StreamWriter]::new($fs, [System.Text.Encoding]::UTF8)
                    $fw.WriteLine($Line)
                    $fw.Close(); $fs.Close()
                } catch {} } }
        FlushDebug "$(Stamp)  [DEBUG]  [W$TaskId/T$threadId] PUT-INIT: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) size=$($Item.SizeBytes)B contentType=$($Item.ContentType)"

            [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
            $s3Config                             = New-Object Amazon.S3.AmazonS3Config
            $s3Config.ServiceURL                  = $Endpoint
            $s3Config.ForcePathStyle              = $true
            $s3Client                             = New-Object Amazon.S3.AmazonS3Client($Credential, $s3Config)
            $xferConfig                           = New-Object Amazon.S3.Transfer.TransferUtilityConfig
            $xferConfig.MinSizeBeforePartUpload   = 5MB
            $xferConfig.ConcurrentServiceRequests = 4
            $xfer                = New-Object Amazon.S3.Transfer.TransferUtility($s3Client, $xferConfig)
            $req                 = New-Object Amazon.S3.Transfer.TransferUtilityUploadRequest
            $req.BucketName      = $Bucket
            $req.Key             = $Item.Key
            $req.FilePath        = $Item.FilePath
            $req.ContentType     = if ([string]::IsNullOrWhiteSpace($Item.ContentType)) { 'application/octet-stream' } else { $Item.ContentType }

            $partSize      = 5MB
            $fileSize      = if ($null -eq $Item.SizeBytes) { 0 } else { [long]$Item.SizeBytes }
            $isMultipart   = $fileSize -gt $partSize
            $partCount     = if ($isMultipart) { [math]::Ceiling($fileSize / $partSize) } else { 1 }
            $putCalls      = $partCount
            $initCalls     = if ($isMultipart) { 1 } else { 0 }
            $completeCalls = if ($isMultipart) { 1 } else { 0 }

            if ($isMultipart) {
                FlushDebug "$(Stamp)  [DEBUG]  [W$TaskId/T$threadId] CreateMultipartUpload: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) parts=$partCount partSize=5MB"
            }

            $xfer.Upload($req)

            if ($isMultipart) {
                FlushDebug "$(Stamp)  [DEBUG]  [W$TaskId/T$threadId] CompleteMultipartUpload: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) parts=$partCount"
            }

            $sw.Stop()
            $sizeMB   = [math]::Round($fileSize / 1MB, 3)
            $totalMs  = [math]::Round($sw.Elapsed.TotalMilliseconds)
            $speedMBs = if ($sw.Elapsed.TotalSeconds -gt 0) { [math]::Round($sizeMB / $sw.Elapsed.TotalSeconds, 2) } else { 0 }
            FlushDebug "$(Stamp)  [PERF]   [W$TaskId/T$threadId] PUT-DONE: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) size=${sizeMB}MB puts=$putCalls duration=${totalMs}ms speed=${speedMBs}MB/s multipart=$isMultipart"

            return @{
                Success          = $true
                Key              = $Item.Key
                FilePath         = $Item.FilePath
                PutRequests      = $putCalls
                InitRequests     = $initCalls
                CompleteRequests = $completeCalls
                IsMultipart      = $isMultipart
                PartCount        = $partCount
                DebugLines       = $debugLines.ToArray()
                TaskId           = $TaskId
                ThreadId         = $threadId
            }
        }
        catch {
            $sw.Stop()
            $errDetail = $_.Exception.Message
            $errLine   = $_.InvocationInfo.ScriptLineNumber
            $errScript = $_.InvocationInfo.Line.Trim()
            $fullError = "Line ${errLine}: $errScript -> $errDetail"
            FlushDebug "$(Stamp)  [ERROR]  [W$TaskId/T$threadId] PUT-FAIL: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) duration=$([math]::Round($sw.Elapsed.TotalMilliseconds))ms error=$fullError"
            return @{
                Success          = $false
                Key              = if ($null -ne $Item -and $null -ne $Item.Key)      { $Item.Key }      else { '(unknown)' }
                FilePath         = if ($null -ne $Item -and $null -ne $Item.FilePath) { $Item.FilePath }  else { '(unknown)' }
                Error            = $fullError
                PutRequests      = 0
                InitRequests     = 0
                CompleteRequests = 0
                DebugLines       = $debugLines.ToArray()
                TaskId           = $TaskId
                ThreadId         = $threadId
            }
        }
        finally {
            if ($null -ne $xfer) { try { $xfer.Dispose() } catch {} }
            if ($null -ne $s3Client) { try { $s3Client.Dispose() } catch {} }
        }
    }
    $stopwatch  = [System.Diagnostics.Stopwatch]::StartNew()
    $totalBytes = ($workItems | ForEach-Object { if ($null -eq $_.SizeBytes) { 0L } else { [long]$_.SizeBytes } } | Measure-Object -Sum).Sum
    if ($null -eq $totalBytes) { $totalBytes = 0L }
    [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
    [System.Net.ServicePointManager]::MaxServicePointIdleTime = 1
    Write-DebugLog -Message "UPLOAD START: $($workItems.Count) files, $([math]::Round($totalBytes/1MB,2)) MB total, $script:ParallelWorkers workers, multipart threshold 5MB" -Level PERF

    $pool = [RunspaceFactory]::CreateRunspacePool(1, $script:ParallelWorkers)
    $pool.Open()
    Write-DebugLog -Message "UPLOAD runspace pool opened with $script:ParallelWorkers max threads" -Level DEBUG

    $taskId = 0
    $handles = foreach ($item in $workItems) {
        $taskId++
        $ps = [PowerShell]::Create()
        $ps.RunspacePool = $pool
        $null = $ps.AddScript($uploadScript).AddArgument($item).AddArgument($Session.Bucket).AddArgument($Session.Endpoint).AddArgument($Session.Credential).AddArgument($script:DebugLogPath).AddArgument($taskId)
        @{ PS = $ps; Handle = $ps.BeginInvoke(); Item = $item; TaskId = $taskId }
    }

    $done            = 0
    $bytesDone       = 0
    $totalPuts       = 0
    $totalInits      = 0
    $totalCompletes  = 0
    $multipartCount  = 0

    foreach ($h in $handles) {
        $rawResult = $h.PS.EndInvoke($h.Handle)
        $result    = if ($rawResult -is [System.Collections.IList] -and $rawResult.Count -gt 0) { $rawResult[0] } else { $rawResult }
        $h.PS.Dispose()
        $done++

        $elapsedNow = [math]::Max($stopwatch.Elapsed.TotalSeconds, 0.1)
        $doneMB     = [math]::Round($bytesDone / 1MB, 2)
        $totalMBNow = [math]::Round($totalBytes / 1MB, 2)
        $speedNow   = [math]::Round($doneMB / $elapsedNow, 2)
        $pctDone    = if ($totalBytes -gt 0) { ($bytesDone / $totalBytes) * 100 } else { ($done / $workItems.Count) * 100 }
        Write-Progress -Activity "Uploading to bucket" `
            -Status "Files $done / $($workItems.Count) | Data ${doneMB}MB / ${totalMBNow}MB | ${speedNow} MB/s | $($h.Item.RelativePath)" `
            -PercentComplete $pctDone

        $rSuccess  = if ($result -is [hashtable] -and $result.ContainsKey('Success')) { $result['Success'] } elseif ($result.PSObject.Properties['Success']) { $result.Success } elseif ($result.PSObject.Properties['Success']) { $result.Success } else { $false }
        $rError    = if ($result -is [hashtable] -and $result.ContainsKey('Error'))            { $result['Error'] } elseif ($result.PSObject.Properties['Error']) { $result.Error } else { 'unknown error' }
        $rPuts     = if ($result -is [hashtable] -and $result.ContainsKey('PutRequests'))      { [int]$result['PutRequests'] } elseif ($result.PSObject.Properties['PutRequests']) { [int]$result.PutRequests } else { 0 }
        $rInits    = if ($result -is [hashtable] -and $result.ContainsKey('InitRequests'))     { [int]$result['InitRequests'] } elseif ($result.PSObject.Properties['InitRequests']) { [int]$result.InitRequests } else { 0 }
        $rComplete = if ($result -is [hashtable] -and $result.ContainsKey('CompleteRequests')) { [int]$result['CompleteRequests'] } elseif ($result.PSObject.Properties['CompleteRequests']) { [int]$result.CompleteRequests } elseif ($result.PSObject.Properties['CompleteRequests']) { [int]$result.CompleteRequests } else { 0 }
        $rMulti    = if ($result -is [hashtable] -and $result.ContainsKey('IsMultipart'))      { [bool]$result['IsMultipart'] } elseif ($result.PSObject.Properties['IsMultipart']) { [bool]$result.IsMultipart } else { $false }
        $rParts    = if ($result -is [hashtable] -and $result.ContainsKey('PartCount'))        { $result['PartCount'] } elseif ($result.PSObject.Properties['PartCount']) { $result.PartCount } else { 1 }
        $rKey      = if ($result -is [hashtable] -and $result.ContainsKey('Key'))              { $result['Key'] } elseif ($result.PSObject.Properties['Key']) { $result.Key } else { $h.Item.Key }
        $rFilePath = if ($result -is [hashtable] -and $result.ContainsKey('FilePath'))         { $result['FilePath'] } elseif ($result.PSObject.Properties['FilePath']) { $result.FilePath } else { $h.Item.FilePath }
        $rTaskId   = if ($result -is [hashtable] -and $result.ContainsKey('TaskId'))           { [int]$result['TaskId'] } elseif ($result.PSObject.Properties['TaskId']) { [int]$result.TaskId } else { [int]$h.TaskId }
        $rThreadId = if ($result -is [hashtable] -and $result.ContainsKey('ThreadId'))         { [int]$result['ThreadId'] } elseif ($result.PSObject.Properties['ThreadId']) { [int]$result.ThreadId } else { -1 }

        if ($rSuccess) {
            $itemBytes       = if ($null -eq $h.Item.SizeBytes) { 0L } else { [long]$h.Item.SizeBytes }
            $sizeMB          = [math]::Round($itemBytes / 1MB, 3)
            $sizeDisplay     = if ($itemBytes -lt 1KB) { "${itemBytes}B" } elseif ($itemBytes -lt 1MB) { "$([math]::Round($itemBytes/1KB,1))KB" } else { "${sizeMB}MB" }
            $bytesDone      += $itemBytes
            $totalPuts      += $rPuts
            $totalInits     += $rInits
            $totalCompletes += $rComplete
            if ($rMulti) { $multipartCount++ }
            $mpLabel = if ($rMulti) { " [multipart: $rParts parts]" } else { "" }
            Write-Host "  Uploaded: [W$rTaskId/T$rThreadId] $($h.Item.RelativePath) ($sizeDisplay)$mpLabel"
            $auditItems.Add("PUT: [W$rTaskId/T$rThreadId] $rFilePath -> s3://$($Session.Bucket)/$rKey [$($h.Item.ContentType)] $sizeDisplay | PUTs: $rPuts$mpLabel")
        } else {
            Write-Host "  FAILED: [W$rTaskId/T$rThreadId] $($h.Item.RelativePath) - $rError" -ForegroundColor Red
            $errors.Add("UPLOAD FAILED: [W$rTaskId/T$rThreadId] $rFilePath - $rError")
        }
    }

    $pool.Close(); $pool.Dispose()
    $stopwatch.Stop()
    Write-Progress -Activity "Uploading to bucket" -Completed

    $elapsedSec  = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
    $totalMB     = [math]::Round($totalBytes / 1MB, 2)
    $speedMBs    = if ($elapsedSec -gt 0) { [math]::Round($totalMB / $elapsedSec, 2) } else { 0 }
    $headCalls = if ($Overwrite) { 0 } else { $total }
    $totalApiCalls = $headCalls + $totalPuts + $totalInits + $totalCompletes

    foreach ($entry in $auditItems) { Write-AuditLog -Message $entry -Level ACTION -NoHost }
    foreach ($entry in $errors)     { Write-AuditLog -Message $entry -Level ERROR  -NoHost }

    $uploaded = $workItems.Count - $errors.Count
    $summary  = "Upload complete. $uploaded uploaded, $skipped skipped, $($errors.Count) failed. | $totalMB MB in ${elapsedSec}s @ ${speedMBs} MB/s | Workers: $script:ParallelWorkers | API calls: HEAD/Metadata x$headCalls, PUT x$totalPuts, CreateMultipartUpload x$totalInits, CompleteMultipartUpload x$totalCompletes (total: $totalApiCalls) | Multipart files: $multipartCount"
    Write-AuditLog -Message $summary -Level INFO -HostColor Green
    Write-DebugLog -Message "UPLOAD COMPLETE: $summary" -Level PERF
}


function Copy-S3BucketToFolder {
    <#
    .SYNOPSIS
        Downloads all objects under a bucket prefix to a local folder.
        Uses 2 parallel runspaces with TransferUtility for multipart throughput.
    .PARAMETER Session
        S3 session object returned by Get-S3Session.
    .PARAMETER Overwrite
        When specified, existing local files are overwritten without prompting.
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'Medium')]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session,
        [switch]$Overwrite
    )

    Write-Section "Download folder from bucket"

    $prefixInput = Read-Host "Enter source prefix/folder in bucket"
    $prefix = Format-S3Prefix -Prefix $prefixInput

    if ([string]::IsNullOrWhiteSpace($prefix)) {
        throw "A source prefix/folder is required for download."
    }

    $downloadFolder = Read-Host "Enter local target folder"
    if (-not (Test-Path -LiteralPath $downloadFolder)) {
        New-Item -ItemType Directory -Path $downloadFolder -Force | Out-Null
    }

    $targetRoot = (Resolve-Path -LiteralPath $downloadFolder).Path.TrimEnd('\')

    # Resolve work items on main thread (conflict prompts need stdin)
    $workItems = [System.Collections.Generic.List[hashtable]]::new()
    $skipped   = 0
    $foundAny   = $false
    $continuationToken = $null
    $listCalls  = 0

    do {
        $page = Get-S3ObjectPage -Session $Session -Prefix $prefix -ContinuationToken $continuationToken
        $listCalls++
        $objects = @($page.Items)
        if ($objects.Count -gt 0) { $foundAny = $true }

        foreach ($object in $objects) {
            $relativeKey = $object.Key.Substring($prefix.Length).TrimStart('/')
            $targetFile  = Join-Path $targetRoot ($relativeKey -replace '/', '\')
            $targetDir   = Split-Path -Path $targetFile -Parent

            if (-not (Test-Path -LiteralPath $targetDir)) {
                New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
            }

            if ((Test-Path -LiteralPath $targetFile) -and -not $Overwrite) {
                $answer = Read-Host "  '$targetFile' already exists. Overwrite? [Y/N]"
                if ($answer -notin @('Y','y')) {
                    Write-Host "  Skipped: $targetFile" -ForegroundColor Yellow
                    $skipped++
                    continue
                }
            }

            if ($PSCmdlet.ShouldProcess($targetFile, "Download 's3://$($Session.Bucket)/$($object.Key)'")) {
                $workItems.Add(@{
                    Key         = $object.Key
                    TargetFile  = $targetFile
                    RelativeKey = $relativeKey
                    SizeBytes   = if ($null -eq $object.Size) { 0L } else { [long]$object.Size }
                })
            } else {
                Write-Host "  What if: Downloading $($object.Key) -> $targetFile"
                Write-AuditLog -Message "PREVIEW-DOWNLOAD: s3://$($Session.Bucket)/$($object.Key) -> $targetFile" -Level PREVIEW -NoHost
            }
        }

        $continuationToken = $page.NextContinuationToken
    } while ($page.IsTruncated)

    if (-not $foundAny) {
        Write-Host "No objects found under prefix '$prefix'." -ForegroundColor Yellow
        return
    }

    if ($workItems.Count -eq 0) {
        Write-Host "Nothing to download." -ForegroundColor Yellow
        return
    }

    Write-Host ""
    Write-Host "Downloading $($workItems.Count) file(s) using $script:DownloadWorkers parallel worker(s)..." -ForegroundColor Cyan

    $errors     = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
    $auditItems = [System.Collections.Concurrent.ConcurrentBag[string]]::new()

    $downloadScript = {
        param($Item, $Bucket, $Endpoint, $Credential, $DebugLogPath, $TaskId)

        $debugLines = [System.Collections.Generic.List[string]]::new()
        $sw         = [System.Diagnostics.Stopwatch]::StartNew()
        $threadId   = [System.Threading.Thread]::CurrentThread.ManagedThreadId
        function Stamp { return (Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff') }

        try {
            Import-Module AWS.Tools.Common -ErrorAction Stop
            Import-Module AWS.Tools.S3 -ErrorAction Stop
            function FlushDebug { param([string]$Line); $debugLines.Add($Line); if (-not [string]::IsNullOrWhiteSpace($DebugLogPath)) { try {
                    $fs = [System.IO.File]::Open($DebugLogPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
                    $fw = [System.IO.StreamWriter]::new($fs, [System.Text.Encoding]::UTF8)
                    $fw.WriteLine($Line)
                    $fw.Close(); $fs.Close()
                } catch {} } }
        FlushDebug "$(Stamp)  [DEBUG]  [W$TaskId/T$threadId] GET-INIT: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) size=$($Item.SizeBytes)B target=$($Item.TargetFile)"

            [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
            [System.Net.ServicePointManager]::MaxServicePointIdleTime = 1
            $s3Config                             = New-Object Amazon.S3.AmazonS3Config
            $s3Config.ServiceURL                  = $Endpoint
            $s3Config.ForcePathStyle              = $true
            $s3Config.MaxErrorRetry               = 3
            $s3Client                             = New-Object Amazon.S3.AmazonS3Client($Credential, $s3Config)
            $sp = [System.Net.ServicePointManager]::FindServicePoint($Endpoint)
            if ($null -ne $sp) { $sp.ConnectionLimit = 128 }
            $xferConfig                           = New-Object Amazon.S3.Transfer.TransferUtilityConfig
            $xferConfig.ConcurrentServiceRequests = 4
            $xfer                = New-Object Amazon.S3.Transfer.TransferUtility($s3Client, $xferConfig)
            $req                 = New-Object Amazon.S3.Transfer.TransferUtilityDownloadRequest
            $req.BucketName      = $Bucket
            $req.Key             = $Item.Key
            $req.FilePath        = $Item.TargetFile

            $partSize = 5MB
            $fileSize = if ($null -eq $Item.SizeBytes) { 0 } else { [long]$Item.SizeBytes }
            $getCount = if ($fileSize -gt $partSize) { [math]::Ceiling($fileSize / $partSize) } else { 1 }

            if ($getCount -gt 1) {
                FlushDebug "$(Stamp)  [DEBUG]  [W$TaskId/T$threadId] GET-MULTIPART: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) rangedGets=$getCount partSize=5MB"
            }

            $xfer.Download($req)
            $sw.Stop()

            $sizeMB   = [math]::Round($fileSize / 1MB, 3)
            $totalMs  = [math]::Round($sw.Elapsed.TotalMilliseconds)
            $speedMBs = if ($sw.Elapsed.TotalSeconds -gt 0) { [math]::Round($sizeMB / $sw.Elapsed.TotalSeconds, 2) } else { 0 }
            FlushDebug "$(Stamp)  [PERF]   [W$TaskId/T$threadId] GET-DONE: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) size=${sizeMB}MB gets=$getCount duration=${totalMs}ms speed=${speedMBs}MB/s"

            return @{
                Success    = $true
                Key        = $Item.Key
                TargetFile = $Item.TargetFile
                GetCount   = $getCount
                DebugLines = $debugLines.ToArray()
                TaskId     = $TaskId
                ThreadId   = $threadId
            }
        }
        catch {
            $sw.Stop()
            $errDetail = $_.Exception.Message
            $errLine   = $_.InvocationInfo.ScriptLineNumber
            $errScript = $_.InvocationInfo.Line.Trim()
            $fullError = "Line ${errLine}: $errScript -> $errDetail"
            FlushDebug "$(Stamp)  [ERROR]  [W$TaskId/T$threadId] GET-FAIL: endpoint=$Endpoint bucket=$Bucket key=$($Item.Key) duration=$([math]::Round($sw.Elapsed.TotalMilliseconds))ms error=$fullError"
            return @{ Success = $false; Key = $Item.Key; TargetFile = $Item.TargetFile; Error = $fullError; DebugLines = $debugLines.ToArray(); TaskId = $TaskId; ThreadId = $threadId }
        }
        finally {
            if ($null -ne $xfer) { try { $xfer.Dispose() } catch {} }
            if ($null -ne $s3Client) { try { $s3Client.Dispose() } catch {} }
        }
    }
    $stopwatch  = [System.Diagnostics.Stopwatch]::StartNew()
    $totalBytes = ($workItems | ForEach-Object { if ($null -eq $_.SizeBytes) { 0L } else { [long]$_.SizeBytes } } | Measure-Object -Sum).Sum
    if ($null -eq $totalBytes) { $totalBytes = 0L }
    [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
    [System.Net.ServicePointManager]::MaxServicePointIdleTime = 1
    Write-DebugLog -Message "DOWNLOAD START: $($workItems.Count) files, $([math]::Round($totalBytes/1MB,2)) MB total, $script:DownloadWorkers workers" -Level PERF

    $pool = [RunspaceFactory]::CreateRunspacePool(1, $script:DownloadWorkers)
    $pool.Open()
    Write-DebugLog -Message "DOWNLOAD runspace pool opened with $script:DownloadWorkers max threads" -Level DEBUG

    $taskId = 0
    $handles = foreach ($item in $workItems) {
        $taskId++
        $ps = [PowerShell]::Create()
        $ps.RunspacePool = $pool
        $null = $ps.AddScript($downloadScript).AddArgument($item).AddArgument($Session.Bucket).AddArgument($Session.Endpoint).AddArgument($Session.Credential).AddArgument($script:DebugLogPath).AddArgument($taskId)
        @{ PS = $ps; Handle = $ps.BeginInvoke(); Item = $item; TaskId = $taskId }
    }

    $done        = 0
    $bytesDone   = 0
    $totalGets   = 0

    foreach ($h in $handles) {
        $rawResult = $h.PS.EndInvoke($h.Handle)
        $result    = if ($rawResult -is [System.Collections.IList] -and $rawResult.Count -gt 0) { $rawResult[0] } else { $rawResult }
        $h.PS.Dispose()
        $done++

        $elapsedNow = [math]::Max($stopwatch.Elapsed.TotalSeconds, 0.1)
        $doneMB     = [math]::Round($bytesDone / 1MB, 2)
        $totalMBNow = [math]::Round($totalBytes / 1MB, 2)
        $speedNow   = [math]::Round($doneMB / $elapsedNow, 2)
        $pctDone    = if ($totalBytes -gt 0) { ($bytesDone / $totalBytes) * 100 } else { ($done / $workItems.Count) * 100 }
        Write-Progress -Activity "Downloading from bucket" `
            -Status "Files $done / $($workItems.Count) | Data ${doneMB}MB / ${totalMBNow}MB | ${speedNow} MB/s | $($h.Item.RelativeKey)" `
            -PercentComplete $pctDone

        $rSuccess    = if ($result -is [hashtable] -and $result.ContainsKey('Success')) { $result['Success'] } elseif ($result.PSObject.Properties['Success']) { $result.Success } elseif ($result.PSObject.Properties['Success']) { $result.Success } else { $false }
        $rError      = if ($result -is [hashtable] -and $result.ContainsKey('Error'))      { $result['Error'] } elseif ($result.PSObject.Properties['Error']) { $result.Error } else { 'unknown error' }
        $rKey        = if ($result -is [hashtable] -and $result.ContainsKey('Key'))        { $result['Key']           } else { $h.Item.Key }
        $rTargetFile = if ($result -is [hashtable] -and $result.ContainsKey('TargetFile')) { $result['TargetFile'] } elseif ($result.PSObject.Properties['TargetFile']) { $result.TargetFile } else { $h.Item.TargetFile }
        $rGetCount   = if ($result -is [hashtable] -and $result.ContainsKey('GetCount'))   { [int]$result['GetCount'] } elseif ($result.PSObject.Properties['GetCount']) { [int]$result.GetCount } else { 1 }
        $rTaskId     = if ($result -is [hashtable] -and $result.ContainsKey('TaskId'))     { [int]$result['TaskId'] } elseif ($result.PSObject.Properties['TaskId']) { [int]$result.TaskId } else { [int]$h.TaskId }
        $rThreadId   = if ($result -is [hashtable] -and $result.ContainsKey('ThreadId'))   { [int]$result['ThreadId'] } elseif ($result.PSObject.Properties['ThreadId']) { [int]$result.ThreadId } else { -1 }

        if ($rSuccess) {
            $itemBytes   = if ($null -eq $h.Item.SizeBytes) { 0L } else { [long]$h.Item.SizeBytes }
            $sizeMB      = [math]::Round($itemBytes / 1MB, 3)
            $sizeDisplay = if ($itemBytes -lt 1KB) { "${itemBytes}B" } elseif ($itemBytes -lt 1MB) { "$([math]::Round($itemBytes/1KB,1))KB" } else { "${sizeMB}MB" }
            $bytesDone  += $itemBytes
            $totalGets  += $rGetCount
            $mpLabel     = if ($rGetCount -gt 1) { " [multipart: $rGetCount parts]" } else { "" }
            Write-Host "  Downloaded: [W$rTaskId/T$rThreadId] $($h.Item.RelativeKey) ($sizeDisplay)$mpLabel"
            $auditItems.Add("GET: [W$rTaskId/T$rThreadId] s3://$($Session.Bucket)/$rKey -> $rTargetFile $sizeDisplay | GETs: $rGetCount$mpLabel")
        } else {
            Write-Host "  FAILED: [W$rTaskId/T$rThreadId] $($h.Item.RelativeKey) - $rError" -ForegroundColor Red
            $errors.Add("DOWNLOAD FAILED: [W$rTaskId/T$rThreadId] $rKey - $rError")
        }
    }

    $pool.Close(); $pool.Dispose()
    $stopwatch.Stop()
    Write-Progress -Activity "Downloading from bucket" -Completed

    $elapsedSec = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
    $totalMB    = [math]::Round($totalBytes / 1MB, 2)
    $speedMBs   = if ($elapsedSec -gt 0) { [math]::Round($totalMB / $elapsedSec, 2) } else { 0 }

    foreach ($entry in $auditItems) { Write-AuditLog -Message $entry -Level ACTION -NoHost }
    foreach ($entry in $errors)     { Write-AuditLog -Message $entry -Level ERROR  -NoHost }

    $downloaded = $workItems.Count - $errors.Count
    $summary    = "Download complete. $downloaded downloaded, $skipped skipped, $($errors.Count) failed. | $totalMB MB in ${elapsedSec}s @ ${speedMBs} MB/s | Workers: $script:DownloadWorkers | API calls: LIST x$listCalls, GET x$totalGets (total: $($listCalls + $totalGets))"
    Write-AuditLog -Message $summary -Level INFO -HostColor Green
    Write-DebugLog -Message "DOWNLOAD COMPLETE: $summary" -Level PERF
}


function Invoke-S3BatchDelete {
    <#
    .SYNOPSIS
        Deletes a list of S3 objects in batches of up to 1000 using the native
        DeleteObjects API, distributed across parallel runspace workers.
    .PARAMETER Session
        S3 session object.
    .PARAMETER Entries
        Array of hashtables with at minimum a 'Key' property.
        For versioned deletes also include 'VersionId'.
    .PARAMETER Activity
        Label shown in the progress bar (e.g. "Deleting objects").
    .OUTPUTS
        Hashtable with DeletedCount, FailedCount, AuditLines (string[]), ErrorLines (string[]).
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [pscustomobject]$Session,
        [Parameter(Mandatory)] [object[]]$Entries,
        [string]$Activity = 'Deleting objects'
    )

    $batchSize   = if ($script:DeleteBatchSize -gt 0) { $script:DeleteBatchSize } else { 100 }
    $allKeys     = $Entries
    $total       = $allKeys.Count
    $auditItems  = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
    $errorItems  = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
    $deletedCount = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()
    $deletedCount['ok']   = 0
    $deletedCount['fail'] = 0

    # Split into batches of 1000
    $batches = [System.Collections.Generic.List[object[]]]::new()
    for ($i = 0; $i -lt $total; $i += $batchSize) {
        $end = [math]::Min($i + $batchSize - 1, $total - 1)
        $batches.Add($allKeys[$i..$end])
    }

    # Raise connection limit before opening the worker pool
    [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
    [System.Net.ServicePointManager]::MaxServicePointIdleTime = 1

    $batchCount = $batches.Count
    $stopwatch  = [System.Diagnostics.Stopwatch]::StartNew()
    Write-Host "  $total object(s) split into $batchCount batch(es) of up to $batchSize, using $script:DeleteWorkers parallel worker(s)." -ForegroundColor Cyan
    Write-DebugLog -Message "DELETE START: $total objects, $batchCount batches, $script:DeleteWorkers workers" -Level PERF

    # Capture debug log path now - $script: variables are not accessible inside runspaces
    $debugLogPath = $script:DebugLogPath

    $batchScript = {
        param($Batch, $Bucket, $Endpoint, $Credential, $DebugLogPath, $BatchId)

        Import-Module AWS.Tools.Common -ErrorAction Stop
        Import-Module AWS.Tools.S3 -ErrorAction Stop

        $deleted    = [System.Collections.Generic.List[string]]::new()
        $failed     = [System.Collections.Generic.List[string]]::new()
        $auditLines = [System.Collections.Generic.List[string]]::new()
        $debugLines = [System.Collections.Generic.List[string]]::new()
        $apiCalls   = 0
        $threadId   = [System.Threading.Thread]::CurrentThread.ManagedThreadId

        function Stamp { return (Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff') }

        function FlushDebug {
            param([string]$Line)
            $debugLines.Add($Line)
            # Write directly to debug log file from within the runspace
            if (-not [string]::IsNullOrWhiteSpace($DebugLogPath)) {
                try {
                    $fs = [System.IO.File]::Open($DebugLogPath, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite)
                    $fw = [System.IO.StreamWriter]::new($fs, [System.Text.Encoding]::UTF8)
                    $fw.WriteLine($Line)
                    $fw.Close(); $fs.Close()
                } catch {}
            }
        }

        # Force unique TCP connection per worker
        [System.Net.ServicePointManager]::DefaultConnectionLimit = 128
        [System.Net.ServicePointManager]::MaxServicePointIdleTime = 1
        $sp = [System.Net.ServicePointManager]::FindServicePoint($Endpoint)
        if ($null -ne $sp) { $sp.ConnectionLimit = 128 }

        # Build a KeyAndVersionCollection for true multi-object batch delete (1 API call per batch)
        $keyCollection = New-Object 'System.Collections.Generic.List[Amazon.S3.Model.KeyVersion]'
        foreach ($entry in $Batch) {
            $kv = New-Object Amazon.S3.Model.KeyVersion
            $kv.Key = $entry.Key
            if ($entry.ContainsKey('VersionId') -and -not [string]::IsNullOrWhiteSpace($entry.VersionId)) {
                $kv.VersionId = $entry.VersionId
            }
            $keyCollection.Add($kv)
        }

        $keyList = ($Batch | ForEach-Object {
            $s = $_.Key
            if ($_.ContainsKey('VersionId') -and -not [string]::IsNullOrWhiteSpace($_.VersionId)) { $s += " [v$($_.VersionId)]" }
            $s
        }) -join ', '

        $batchSw = [System.Diagnostics.Stopwatch]::StartNew()
        FlushDebug "$(Stamp)  [DEBUG]  [B$BatchId/T$threadId] BATCH-DELETE-INIT: endpoint=$Endpoint bucket=$Bucket keys=$($Batch.Count) first='$($Batch[0].Key)'"
        FlushDebug "$(Stamp)  [DEBUG]  [B$BatchId/T$threadId] BATCH-DELETE-CALL: Remove-S3Object -BucketName '$Bucket' -KeyAndVersionCollection [collection of $($Batch.Count) keys] -EndpointUrl '$Endpoint' -Force"

        try {
            $apiCalls++
            $response = Remove-S3Object `
                -BucketName             $Bucket `
                -KeyAndVersionCollection $keyCollection `
                -EndpointUrl            $Endpoint `
                -Credential             $Credential `
                -Force

            $batchSw.Stop()
            $durationMs = [math]::Round($batchSw.Elapsed.TotalMilliseconds)

            # Remove-S3Object returns delete errors if any keys failed; successful keys are the remainder
            $failedKeyInfo = @()
            if ($null -ne $response -and $response -is [Amazon.S3.Model.DeleteObjectsResponse]) {
                $failedKeyInfo = @($response.DeleteErrors | ForEach-Object { [pscustomobject]@{ Key = $_.Key; Detail = "$($_.Key) [$($_.Code): $($_.Message)]" } })
            }

            $failedKeySet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
            foreach ($fk in $failedKeyInfo) { $null = $failedKeySet.Add([string]$fk.Key) }

            $succeededKeys = if ($failedKeyInfo.Count -gt 0) {
                @($Batch | Where-Object {
                    $k = [string]$_.Key
                    -not $failedKeySet.Contains($k)
                } | ForEach-Object { $_.Key })
            } else {
                @($Batch | ForEach-Object { $_.Key })
            }

            foreach ($key in $succeededKeys) {
                $deleted.Add($key)
                $auditLines.Add("DELETE: [B$BatchId/T$threadId] s3://$Bucket/$key | batch=true")
            }
            foreach ($err in $failedKeyInfo) {
                $failed.Add($err.Detail)
                $auditLines.Add("DELETE FAILED: [B$BatchId/T$threadId] s3://$Bucket/$($err.Detail) | batch=true")
            }

            FlushDebug "$(Stamp)  [PERF]   [B$BatchId/T$threadId] BATCH-DELETE-DONE: bucket=$Bucket keys=$($Batch.Count) succeeded=$($succeededKeys.Count) failed=$($failedKeyInfo.Count) duration=${durationMs}ms avgPerKey=$([math]::Round($durationMs / [math]::Max(1,$Batch.Count), 1))ms"
        }
        catch {
            $batchSw.Stop()
            $errDetail = $_.Exception.Message
            $errLine   = $_.InvocationInfo.ScriptLineNumber
            $errScript = $_.InvocationInfo.Line.Trim()
            $fullError = "Line $errLine`: $errScript -> $errDetail"
            FlushDebug "$(Stamp)  [ERROR]  [B$BatchId/T$threadId] BATCH-DELETE-FAIL: bucket=$Bucket keys=$($Batch.Count) duration=$([math]::Round($batchSw.Elapsed.TotalMilliseconds))ms error=$fullError"
            FlushDebug "$(Stamp)  [DEBUG]  [B$BatchId/T$threadId] BATCH-DELETE-FALLBACK: falling back to per-key deletes for this batch"

            # Fallback: per-key delete if the batch API fails
            foreach ($entry in $Batch) {
                $delSw         = [System.Diagnostics.Stopwatch]::StartNew()
                $versionSuffix = if ($entry.ContainsKey('VersionId') -and -not [string]::IsNullOrWhiteSpace($entry.VersionId)) { " versionId=$($entry.VersionId)" } else { '' }
                FlushDebug "$(Stamp)  [DEBUG]  [B$BatchId/T$threadId] DELETE-FALLBACK-CALL: Remove-S3Object -BucketName '$Bucket' -Key '$($entry.Key)' -EndpointUrl '$Endpoint'$versionSuffix -Force"
                try {
                    $params = @{
                        BucketName  = $Bucket
                        Key         = $entry.Key
                        EndpointUrl = $Endpoint
                        Credential  = $Credential
                        Force       = $true
                    }
                    if ($entry.ContainsKey('VersionId') -and -not [string]::IsNullOrWhiteSpace($entry.VersionId)) {
                        $params['VersionId'] = $entry.VersionId
                    }
                    $apiCalls++
                    Remove-S3Object @params | Out-Null
                    $delSw.Stop()
                    FlushDebug "$(Stamp)  [PERF]   [B$BatchId/T$threadId] DELETE-FALLBACK-DONE: key=$($entry.Key) duration=$([math]::Round($delSw.Elapsed.TotalMilliseconds))ms"
                    $deleted.Add($entry.Key)
                    $auditLines.Add("DELETE: [B$BatchId/T$threadId] s3://$Bucket/$($entry.Key)$versionSuffix | batch=false (fallback)")
                }
                catch {
                    $delSw.Stop()
                    $fbErr = "Line $($_.InvocationInfo.ScriptLineNumber)`: $($_.InvocationInfo.Line.Trim()) -> $($_.Exception.Message)"
                    FlushDebug "$(Stamp)  [ERROR]  [B$BatchId/T$threadId] DELETE-FALLBACK-FAIL: key=$($entry.Key) duration=$([math]::Round($delSw.Elapsed.TotalMilliseconds))ms error=$fbErr"
                    $failed.Add("$($entry.Key) [$fbErr]")
                    $auditLines.Add("DELETE FAILED: [B$BatchId/T$threadId] s3://$Bucket/$($entry.Key)$versionSuffix | error=$fbErr")
                }
            }
        }

        return @{
            Success    = $true
            Deleted    = $deleted.ToArray()
            Failed     = $failed.ToArray()
            AuditLines = $auditLines.ToArray()
            DebugLines = $debugLines.ToArray()
            BatchId    = $BatchId
            ThreadId   = $threadId
            ApiCalls   = $apiCalls
        }
    }

    $pool = [RunspaceFactory]::CreateRunspacePool(1, $script:DeleteWorkers)
    $pool.Open()

    $batchId = 0
    $handles = foreach ($batch in $batches) {
        $batchId++
        # Convert to plain hashtables so they cross runspace boundaries safely
        $batchData = @($batch | ForEach-Object {
            $h = @{ Key = $_.Key }
            if ($_ -is [hashtable] -and $_.ContainsKey('VersionId')) { $h['VersionId'] = $_.VersionId }
            elseif ($_.PSObject.Properties['VersionId'])              { $h['VersionId'] = $_.VersionId }
            $h
        })
        $ps = [PowerShell]::Create()
        $ps.RunspacePool = $pool
        $null = $ps.AddScript($batchScript).AddArgument($batchData).AddArgument($Session.Bucket).AddArgument($Session.Endpoint).AddArgument($Session.Credential).AddArgument($debugLogPath).AddArgument($batchId)
        @{ PS = $ps; Handle = $ps.BeginInvoke(); Batch = $batchData; BatchId = $batchId }
    }

    $doneBatches   = 0
    $totalDeleted  = 0
    $totalFailed   = 0
    $deleteApiCalls = 0

    foreach ($h in $handles) {
        $rawResult = $h.PS.EndInvoke($h.Handle)
        $result    = if ($rawResult -is [System.Collections.IList] -and $rawResult.Count -gt 0) { $rawResult[0] } else { $rawResult }
        $h.PS.Dispose()
        $doneBatches++

        Write-Progress -Activity $Activity `
            -Status "Batch $doneBatches / $batchCount  ($totalDeleted deleted so far)" `
            -PercentComplete (($doneBatches / $batchCount) * 100)

        $rSuccess = if ($result -is [hashtable] -and $result.ContainsKey('Success')) { $result['Success'] } elseif ($result.PSObject.Properties['Success']) { $result.Success } elseif ($result.PSObject.Properties['Success']) { $result.Success } else { $false }
        $rDeleted = if ($result -is [hashtable] -and $result.ContainsKey('Deleted')) { $result['Deleted'] } elseif ($result.PSObject.Properties['Deleted']) { $result.Deleted } elseif ($result.PSObject.Properties['Deleted']) { $result.Deleted } else { @() }
        $rFailed  = if ($result -is [hashtable] -and $result.ContainsKey('Failed'))  { $result['Failed'] } elseif ($result.PSObject.Properties['Failed']) { $result.Failed } else { @() }
        $rError   = if ($result -is [hashtable] -and $result.ContainsKey('Error'))   { $result['Error'] } elseif ($result.PSObject.Properties['Error']) { $result.Error } else { 'unknown error' }
        $rBatchId  = if ($result -is [hashtable] -and $result.ContainsKey('BatchId'))  { [int]$result['BatchId'] } elseif ($result.PSObject.Properties['BatchId']) { [int]$result.BatchId } else { [int]$h.BatchId }
        $rThreadId = if ($result -is [hashtable] -and $result.ContainsKey('ThreadId')) { [int]$result['ThreadId'] } elseif ($result.PSObject.Properties['ThreadId']) { [int]$result.ThreadId } else { -1 }
        $rApiCalls  = if ($result -is [hashtable] -and $result.ContainsKey('ApiCalls')) { [int]$result['ApiCalls'] } elseif ($result.PSObject.Properties['ApiCalls']) { [int]$result.ApiCalls } else { 0 }
        $deleteApiCalls += $rApiCalls

        # Collect audit lines from the batch worker
        $rAuditBatch = if ($result -is [hashtable] -and $result.ContainsKey('AuditLines')) { $result['AuditLines'] } elseif ($result.PSObject.Properties['AuditLines']) { $result.AuditLines } elseif ($result.PSObject.Properties['AuditLines']) { $result.AuditLines } else { @() }

        if ($rSuccess) {
            foreach ($key in $rDeleted) {
                $totalDeleted++
                Write-Host "  Deleted: [B$rBatchId/T$rThreadId] $key" -ForegroundColor Gray
            }
            foreach ($auditLine in $rAuditBatch) {
                $auditItems.Add($auditLine)
            }
            foreach ($err in $rFailed) {
                $totalFailed++
                Write-Host "  FAILED: [B$rBatchId/T$rThreadId] $err" -ForegroundColor Red
                $errorItems.Add("DELETE FAILED: [B$rBatchId/T$rThreadId] $err")
            }
        } else {
            foreach ($key in $rFailed) {
                $totalFailed++
                Write-Host "  FAILED: [B$rBatchId/T$rThreadId] $key - $rError" -ForegroundColor Red
                $errorItems.Add("DELETE FAILED: [B$rBatchId/T$rThreadId] $key - $rError")
            }
        }
    }

    $pool.Close(); $pool.Dispose()
    $stopwatch.Stop()
    Write-Progress -Activity $Activity -Completed

    $elapsedSec = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)

    return @{
        DeletedCount    = $totalDeleted
        FailedCount     = $totalFailed
        ElapsedSec      = $elapsedSec
        DeleteApiCalls  = $deleteApiCalls
        AuditLines      = @($auditItems)
        ErrorLines      = @($errorItems)
    }
}

function Remove-BucketContent {
    <#
    .SYNOPSIS
        Deletes all current objects under an optional bucket prefix.
        Uses paged enumeration so very large buckets do not have to be held
        fully in memory before deletion starts.
    #>
    [CmdletBinding(SupportsShouldProcess, ConfirmImpact = 'None')]
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Session
    )

    Write-Section "Delete content from bucket"

    $prefixInput = Read-Host "Enter prefix/folder to delete (leave empty to delete ALL bucket content)"
    $prefix = Format-S3Prefix -Prefix $prefixInput

    Write-Host ""
    $scope = if ([string]::IsNullOrWhiteSpace($prefix)) {
        "ALL objects in bucket '$($Session.Bucket)'"
    } else {
        "all objects under prefix '$prefix' in bucket '$($Session.Bucket)'"
    }

    Write-Host "WARNING: this will permanently delete $scope." -ForegroundColor Red

    $vStatus = Get-S3VersioningStatus -Session $Session
    if ($vStatus -eq 'Enabled' -or $vStatus -eq 'Suspended') {
        Write-Host ""
        Write-Host "NOTE: Versioning is '$vStatus' on this bucket." -ForegroundColor Yellow
        Write-Host "      Objects will receive a delete marker but older versions" -ForegroundColor Yellow
        Write-Host "      will remain and continue to incur storage costs." -ForegroundColor Yellow
        Write-Host "      Use menu option 8 to purge all versions permanently." -ForegroundColor Yellow
    }

    $confirmation = Read-Host "Type YES to continue"
    if ($confirmation -ne 'YES') { Write-Host "Delete cancelled." -ForegroundColor Yellow; return }

    $continuationToken = $null
    $pageNo            = 0
    $listCalls         = 0
    $totalSeen         = 0
    $totalDeleted      = 0
    $totalFailed       = 0
    $deleteApi         = 0
    $foundAny          = $false
    $stopwatch         = [System.Diagnostics.Stopwatch]::StartNew()

    if ($PSCmdlet.ShouldProcess("objects in s3://$($Session.Bucket)", 'Delete')) {
        do {
            $page = Get-S3ObjectPage -Session $Session -Prefix $prefix -ContinuationToken $continuationToken
            $listCalls++
            $pageNo++
            $items = @($page.Items)

            if ($items.Count -gt 0) {
                $foundAny = $true
                $totalSeen += $items.Count
                Write-Host "Processing object page $pageNo ($($items.Count) object(s), $totalSeen discovered so far)..." -ForegroundColor Cyan
                $entries = @($items | ForEach-Object { @{ Key = $_.Key } })
                $result  = Invoke-S3BatchDelete -Session $Session -Entries $entries -Activity "Deleting objects (page $pageNo)"

                $rAuditLines   = if ($result -is [hashtable] -and $result.ContainsKey('AuditLines')) { $result['AuditLines'] } elseif ($result.PSObject.Properties['AuditLines']) { $result.AuditLines } else { @() }
                $rErrorLines   = if ($result -is [hashtable] -and $result.ContainsKey('ErrorLines')) { $result['ErrorLines'] } elseif ($result.PSObject.Properties['ErrorLines']) { $result.ErrorLines } else { @() }
                $rDeletedCount = if ($result -is [hashtable] -and $result.ContainsKey('DeletedCount')) { [int]$result['DeletedCount'] } elseif ($result.PSObject.Properties['DeletedCount']) { [int]$result.DeletedCount } else { 0 }
                $rFailedCount  = if ($result -is [hashtable] -and $result.ContainsKey('FailedCount')) { [int]$result['FailedCount'] } elseif ($result.PSObject.Properties['FailedCount']) { [int]$result.FailedCount } else { 0 }
                $rDeleteApi    = if ($result -is [hashtable] -and $result.ContainsKey('DeleteApiCalls')) { [int]$result['DeleteApiCalls'] } elseif ($result.PSObject.Properties['DeleteApiCalls']) { [int]$result.DeleteApiCalls } else { 0 }

                foreach ($entry in $rAuditLines) { Write-AuditLog -Message $entry -Level ACTION -HostColor Gray }
                foreach ($entry in $rErrorLines) { Write-AuditLog -Message $entry -Level ERROR -HostColor Red }

                $totalDeleted += $rDeletedCount
                $totalFailed  += $rFailedCount
                $deleteApi    += $rDeleteApi
            }

            $continuationToken = $page.NextContinuationToken
        } while ($page.IsTruncated)

        $stopwatch.Stop()

        if (-not $foundAny) {
            Write-Host "No objects found to delete." -ForegroundColor Yellow
            return
        }

        $elapsedSec = [math]::Round($stopwatch.Elapsed.TotalSeconds, 1)
        $totalCalls = $listCalls + $deleteApi
        $summary = "Delete complete. $totalDeleted deleted, $totalFailed failed. | ${elapsedSec}s | Workers: $script:DeleteWorkers | Pages: $pageNo | API calls: LIST x$listCalls, DeleteObjects x$deleteApi (total: $totalCalls) | Batch size: $script:DeleteBatchSize | Streaming: page-by-page | Logs include batch/thread IDs"
        Write-AuditLog -Message $summary -Level ACTION -HostColor Green
        Write-DebugLog -Message "DELETE COMPLETE: $summary" -Level PERF
    }
    else {
        do {
            $page = Get-S3ObjectPage -Session $Session -Prefix $prefix -ContinuationToken $continuationToken
            $pageNo++
            foreach ($object in $page.Items) {
                Write-Host "  What if: Deleting $($object.Key)"
                Write-AuditLog -Message "PREVIEW-DELETE: s3://$($Session.Bucket)/$($object.Key)" -Level PREVIEW -NoHost
                $foundAny = $true
            }
            $continuationToken = $page.NextContinuationToken
        } while ($page.IsTruncated)

        if (-not $foundAny) { Write-Host "No objects found to delete." -ForegroundColor Yellow }
    }
}

# ---------------------------------------------------------------------------
# Region: Menu
# ---------------------------------------------------------------------------

function Invoke-WorkerCountPrompt {
    param([string]$Label, [int]$Current)
    $val = Read-Host "  $Label [current: $Current, Enter to keep]"
    if ([string]::IsNullOrWhiteSpace($val)) { return $Current }
    $n = 0
    if (-not [int]::TryParse($val, [ref]$n) -or $n -lt 1 -or $n -gt 10) {
        Write-Host "  Invalid - keeping $Current." -ForegroundColor Yellow
        return $Current
    }
    return $n
}

function Invoke-WorkerConfigSave {
    param()
    $cfgPath = Get-S3ConfigPath
    try {
        if (Test-Path -LiteralPath $cfgPath) {
            $cfg = Get-Content -LiteralPath $cfgPath -Raw | ConvertFrom-Json
        } else {
            $cfg = New-Object PSObject
        }
        $cfg | Add-Member -NotePropertyName 'ParallelWorkers'  -NotePropertyValue $script:ParallelWorkers  -Force
        $cfg | Add-Member -NotePropertyName 'DownloadWorkers'  -NotePropertyValue $script:DownloadWorkers  -Force
        $cfg | Add-Member -NotePropertyName 'DeleteWorkers'    -NotePropertyValue $script:DeleteWorkers    -Force
        $cfg | Add-Member -NotePropertyName 'DeleteBatchSize'  -NotePropertyValue $script:DeleteBatchSize  -Force
        $cfg | ConvertTo-Json | Set-Content -LiteralPath $cfgPath -Encoding UTF8
    } catch {}
}

function Set-WorkerCounts {
    <#
    .SYNOPSIS
        Interactively sets parallel worker counts for upload, download and delete independently.
    #>
    [CmdletBinding()]
    param()

    Write-Section "Parallel worker configuration"

    Write-Host "  Upload workers  : $script:ParallelWorkers  (each handles one file via TransferUtility)"
    Write-Host "  Download workers: $script:DownloadWorkers  (each handles one file via TransferUtility)"
    Write-Host "  Delete workers  : $script:DeleteWorkers    (each handles one batch)"
    Write-Host "  Delete batch size: $script:DeleteBatchSize keys per batch  (lower = more parallel batches)"
    Write-Host "  Force path style : $script:ForcePathStyle"
    Write-Host ""
    Write-Host "  1  = sequential, easiest to debug"
    Write-Host "  2  = default, good for most connections"
    Write-Host "  4+ = fast/LAN connections"
    Write-Host "  10 = maximum"
    Write-Host ""

    $script:ParallelWorkers = Invoke-WorkerCountPrompt -Label "Upload workers  " -Current $script:ParallelWorkers
    $script:DownloadWorkers = Invoke-WorkerCountPrompt -Label "Download workers" -Current $script:DownloadWorkers
    $script:DeleteWorkers   = Invoke-WorkerCountPrompt -Label "Delete workers  " -Current $script:DeleteWorkers

    Write-Host ""
    $batchInput = Read-Host "  Delete batch size [current: $script:DeleteBatchSize, Enter to keep]"
    if (-not [string]::IsNullOrWhiteSpace($batchInput)) {
        $n = 0
        if ([int]::TryParse($batchInput, [ref]$n) -and $n -ge 1 -and $n -le 1000) {
            $script:DeleteBatchSize = $n
        } else {
            Write-Host "  Invalid - must be 1-1000. Keeping $script:DeleteBatchSize." -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host "  Upload: $script:ParallelWorkers  Download: $script:DownloadWorkers  Delete: $script:DeleteWorkers  BatchSize: $script:DeleteBatchSize" -ForegroundColor Green
    Write-AuditLog -Message "Worker counts set - Upload: $script:ParallelWorkers  Download: $script:DownloadWorkers  Delete: $script:DeleteWorkers" -Level INFO -NoHost

    Invoke-WorkerConfigSave
}



function Invoke-LogConfigSave {
    <#
    .SYNOPSIS
        Persists log settings to the config file.
        Creates a minimal config entry if none exists yet.
    #>
    param()
    $cfgPath = Get-S3ConfigPath
    try {
        if (Test-Path -LiteralPath $cfgPath) {
            $cfg = Get-Content -LiteralPath $cfgPath -Raw | ConvertFrom-Json
        } else {
            $cfg = New-Object PSObject
        }
        $cfg | Add-Member -NotePropertyName 'AuditLogPath'    -NotePropertyValue (if ($script:AuditLogPath)  { $script:AuditLogPath }  else { '' }) -Force
        $cfg | Add-Member -NotePropertyName 'AuditLogEnabled' -NotePropertyValue $script:AuditLogEnabled -Force
        $cfg | Add-Member -NotePropertyName 'DebugLogPath'    -NotePropertyValue (if ($script:DebugLogPath)  { $script:DebugLogPath }  else { '' }) -Force
        $cfg | Add-Member -NotePropertyName 'DebugLogEnabled' -NotePropertyValue $script:DebugLogEnabled -Force
        $cfg | ConvertTo-Json | Set-Content -LiteralPath $cfgPath -Encoding UTF8
    } catch {}
}

function Set-AuditLog {
    <#
    .SYNOPSIS
        Configures audit logging - toggle, change path, or open log folder.
    #>
    [CmdletBinding()]
    param(
        [pscustomobject]$Session
    )

    Write-Section "Audit log configuration"

    $status = if ($script:AuditLogEnabled) { 'ON' } else { 'OFF' }
    Write-Host "  Status : $status"
    Write-Host "  Path   : $script:AuditLogPath"
    Write-Host ""
    Write-Host "  1. Toggle ON/OFF"
    Write-Host "  2. Change log path"
    Write-Host "  3. Open log folder in Explorer"
    Write-Host "  Enter  = cancel"
    Write-Host ""

    $sub = Read-Host "Choose"

    switch ($sub) {
        '1' {
            $script:AuditLogEnabled = -not $script:AuditLogEnabled
            $state = if ($script:AuditLogEnabled) { 'ON' } else { 'OFF' }
            Write-Host "Audit logging is now $state." -ForegroundColor $(if ($script:AuditLogEnabled) { 'Green' } else { 'Yellow' })
            Invoke-LogConfigSave
            if ($script:AuditLogEnabled -and $Session) {
                Write-AuditHeader -Bucket $Session.Bucket -Endpoint $Session.Endpoint
            }
        }
        '2' {
            $newPath = Read-Host "Enter full path for the audit log (e.g. C:\Logs\s3-audit.log)"
            if ([string]::IsNullOrWhiteSpace($newPath)) {
                Write-Host "No change." -ForegroundColor Yellow
                return
            }
            $dir = Split-Path -Path $newPath -Parent
            if (-not [string]::IsNullOrWhiteSpace($dir) -and -not (Test-Path -LiteralPath $dir)) {
                $create = Read-Host "Directory '$dir' does not exist. Create it? [Y/N]"
                if ($create -notin @('Y','y')) {
                    Write-Host "No change." -ForegroundColor Yellow
                    return
                }
                New-Item -ItemType Directory -Path $dir -Force | Out-Null
            }
            $script:AuditLogPath    = $newPath
            $script:AuditLogEnabled = $true
            Write-Host "Audit log path set to: $script:AuditLogPath" -ForegroundColor Green
            Invoke-LogConfigSave
            if ($Session) {
                Write-AuditHeader -Bucket $Session.Bucket -Endpoint $Session.Endpoint
            }
        }
        '3' {
            $dir = Split-Path -Path $script:AuditLogPath -Parent
            if (-not [string]::IsNullOrWhiteSpace($dir) -and (Test-Path -LiteralPath $dir)) {
                Start-Process explorer.exe $dir
            } else {
                Write-Host "Directory does not exist yet: $dir" -ForegroundColor Yellow
            }
        }
        default {
            Write-Host "Cancelled." -ForegroundColor Yellow
        }
    }
}

function Set-DebugLog {
    <#
    .SYNOPSIS
        Configures debug logging - toggle, change path, or open log folder.
    #>
    [CmdletBinding()]
    param()

    Write-Section "Debug log configuration"

    $status = if ($script:DebugLogEnabled) { 'ON' } else { 'OFF' }
    Write-Host "  Status : $status"
    Write-Host "  Path   : $script:DebugLogPath"
    Write-Host ""
    Write-Host "  1. Toggle ON/OFF"
    Write-Host "  2. Change log path"
    Write-Host "  3. Open log folder in Explorer"
    Write-Host "  Enter  = cancel"
    Write-Host ""

    $sub = Read-Host "Choose"

    switch ($sub) {
        '1' {
            $script:DebugLogEnabled = -not $script:DebugLogEnabled
            $state = if ($script:DebugLogEnabled) { 'ON' } else { 'OFF' }
            Write-Host "Debug logging is now $state." -ForegroundColor $(if ($script:DebugLogEnabled) { 'Green' } else { 'Yellow' })
            Invoke-LogConfigSave
        }
        '2' {
            $newPath = Read-Host "Enter full path for the debug log (e.g. C:\Logs\s3-debug.log)"
            if ([string]::IsNullOrWhiteSpace($newPath)) {
                Write-Host "No change." -ForegroundColor Yellow
                return
            }
            $dir = Split-Path -Path $newPath -Parent
            if (-not [string]::IsNullOrWhiteSpace($dir) -and -not (Test-Path -LiteralPath $dir)) {
                $create = Read-Host "Directory '$dir' does not exist. Create it? [Y/N]"
                if ($create -notin @('Y','y')) {
                    Write-Host "No change." -ForegroundColor Yellow
                    return
                }
                New-Item -ItemType Directory -Path $dir -Force | Out-Null
            }
            $script:DebugLogPath    = $newPath
            $script:DebugLogEnabled = $true
            Write-Host "Debug log path set to: $script:DebugLogPath" -ForegroundColor Green
            Invoke-LogConfigSave
        }
        '3' {
            $dir = Split-Path -Path $script:DebugLogPath -Parent
            if (-not [string]::IsNullOrWhiteSpace($dir) -and (Test-Path -LiteralPath $dir)) {
                Start-Process explorer.exe $dir
            } else {
                Write-Host "Directory does not exist yet: $dir" -ForegroundColor Yellow
            }
        }
        default {
            Write-Host "Cancelled." -ForegroundColor Yellow
        }
    }
}

function Show-Help {
    Write-Host ""
    Write-Host "============================" -ForegroundColor Cyan
    Write-Host "  S3 Manager - Help"          -ForegroundColor Cyan
    Write-Host "============================" -ForegroundColor Cyan

    Write-Host ""
    Write-Host "OVERVIEW" -ForegroundColor White
    Write-Host "  S3 Manager is an interactive PowerShell tool for S3-compatible storage."
    Write-Host "  It is designed for daily operational work: connect, test, list, upload,"
    Write-Host "  download, delete, inspect versioning, and permanently purge versions."
    Write-Host "  The script uses AWS.Tools.S3 / AWS.Tools.Common and also works with"
    Write-Host "  non-AWS endpoints such as StorageGRID, ActiveScale, Wasabi, MinIO, etc."

    Write-Host ""
    Write-Host "HOW THE SCRIPT WORKS" -ForegroundColor White
    Write-Host "  1. You configure one session containing endpoint, bucket and credentials."
    Write-Host "  2. The script builds an S3 client against that endpoint."
    Write-Host "  3. Read operations use LIST / GET calls against the bucket."
    Write-Host "  4. Write operations use PUT, DeleteObjects, and version-specific delete calls."
    Write-Host "  5. Large upload/download jobs are distributed across parallel runspaces."
    Write-Host "  6. Optional audit and debug logs record exactly what happened." 

    Write-Host ""
    Write-Host "GETTING STARTED" -ForegroundColor White
    Write-Host "  Always run option 1 first to configure your connection."
    Write-Host "  You will need:"
    Write-Host "    - Access Key       Your S3 access key ID"
    Write-Host "    - Secret Key       Your S3 secret access key (masked on input)"
    Write-Host "    - Endpoint URL     e.g. https://s3-nl03.cloud.dm-p.com"
    Write-Host "    - Bucket name      The target bucket"
    Write-Host "    - ForcePathStyle   Usually ON for many S3-compatible platforms"

    Write-Host ""
    Write-Host "RECOMMENDED WORKFLOW" -ForegroundColor White
    Write-Host "  1. Configure connection"
    Write-Host "  2. Test connection"
    Write-Host "  3. List bucket content or check versioning status"
    Write-Host "  4. Enable audit log and optionally debug log"
    Write-Host "  5. Run upload/download/delete"
    Write-Host "  6. For destructive actions, enable WhatIf first"

    Write-Host ""
    Write-Host "MENU OPTIONS" -ForegroundColor White

    $options = @(
        @{ N='1. Configure connection';           D='Enter credentials, endpoint, bucket and path-style setting. Values can be saved to AppData.' }
        @{ N='2. Test connection';                D='Checks whether the endpoint can be reached and whether the credentials are valid. Read-only.' }
        @{ N='3. List bucket content';            D='Lists objects under an optional prefix, including size and timestamp.' }
        @{ N='4. Upload folder to bucket';        D='Recursively scans a local folder and uploads files. Existing objects can be checked before overwrite.' }
        @{ N='5. Download folder from bucket';    D='Lists objects under a prefix, maps them to local paths, then downloads them in parallel.' }
        @{ N='6. Delete content from bucket';     D='Deletes objects under a prefix. On versioned buckets this usually creates delete markers, not true purge.' }
        @{ N='7. Show versioning status';         D='Displays whether bucket versioning is Enabled, Suspended, or never enabled.' }
        @{ N='8. List all object versions';       D='Shows normal versions and delete markers so you can inspect the true state of the bucket.' }
        @{ N='9. Purge all versions permanently'; D='Hard delete of all versions and delete markers. This is irreversible and requires multiple confirmations.' }
        @{ N='T. Workers';                        D='Tune parallel worker counts for upload, download and delete, plus delete batch size.' }
        @{ N='A. Audit log';                      D='Enable or disable human-readable operational logging to a text file.' }
        @{ N='D. Debug log';                      D='Enable or disable debug/performance logging, change the log path, or open the log folder.' }
        @{ N='C. Clear config';                   D='Removes the saved config file so the next session starts clean.' }
        @{ N='W. WhatIf';                         D='Preview mode. Upload, download and delete actions are simulated and logged, but nothing is changed.' }
        @{ N='?. Help';                           D='Shows this built-in help screen.' }
        @{ N='0. Exit';                           D='Exits the script.' }
    )

    foreach ($opt in $options) {
        Write-Host ""
        Write-Host "  $($opt.N)" -ForegroundColor Cyan
        Write-Host "    $($opt.D)"
    }

    Write-Host ""
    Write-Host "UPLOAD / DOWNLOAD DESIGN" -ForegroundColor White
    Write-Host "  Upload and download jobs are first resolved into work items on the main"
    Write-Host "  thread. After that, the actual transfer is done by parallel runspaces."
    Write-Host "  This keeps prompts and path handling predictable while still allowing"
    Write-Host "  higher throughput. Large files can use multipart transfer logic."

    Write-Host ""
    Write-Host "DELETE DESIGN" -ForegroundColor White
    Write-Host "  Standard delete uses the S3 DeleteObjects API in batches."
    Write-Host "  On non-versioned buckets this removes objects. On versioned buckets it"
    Write-Host "  normally creates delete markers only. Permanent cleanup of all history"
    Write-Host "  requires option 9, which enumerates versions and delete markers first."

    Write-Host ""
    Write-Host "VERSIONING NOTES" -ForegroundColor White
    Write-Host "  When versioning is Enabled on a bucket, deleting an object with option 6"
    Write-Host "  usually adds a delete marker, while previous versions remain present and"
    Write-Host "  may continue to consume storage. Use option 8 to inspect versions and"
    Write-Host "  option 9 only when you intentionally want a permanent purge."

    Write-Host ""
    Write-Host "PREVIEW (WHATIF) MODE" -ForegroundColor White
    Write-Host "  Press W from the menu to toggle preview mode on or off."
    Write-Host "  When active, destructive or transfer actions are logged as previews only."
    Write-Host "  This is the safest way to validate prefixes, target paths and scope"
    Write-Host "  before doing real uploads, downloads, deletes or version purges."

    Write-Host ""
    Write-Host "LOGGING" -ForegroundColor White
    Write-Host "  Audit log: operational record of actions, previews, warnings and errors."
    Write-Host "  Debug log: lower-level troubleshooting and performance details such as"
    Write-Host "  workers, multipart behavior, API activity and timing information."

    Write-Host ""
    Write-Host "SECURITY" -ForegroundColor White
    Write-Host "  Secret keys are read as SecureString and zeroed from memory immediately."
    Write-Host "  Plain-text credentials are not intentionally persisted in the session object."
    Write-Host ""
}

function Show-Menu {
    param([bool]$WhatIfActive = $false)

    # Build mode tag for header
    $modeTags = @()
    if ($WhatIfActive)           { $modeTags += 'PREVIEW' }
    if ($script:DebugLogEnabled) { $modeTags += 'DEBUG'   }
    $modeStr   = if ($modeTags.Count -gt 0) { '  [' + ($modeTags -join '] [') + ']' } else { '' }
    $hdrColor  = if ($WhatIfActive) { 'Yellow' } else { 'Cyan' }

    # Settings inline values
    $workerVal = "Upload:$script:ParallelWorkers  Download:$script:DownloadWorkers  Delete:$script:DeleteWorkers  Batch:$script:DeleteBatchSize  ForcePathStyle:$script:ForcePathStyle"

    $auditVal  = if ($script:AuditLogEnabled) {
        "ON  - $script:AuditLogPath"
    } else { "OFF" }
    $auditColor = if ($script:AuditLogEnabled) { 'Green' } else { 'DarkGray' }

    $debugVal  = if ($script:DebugLogEnabled) {
        "ON  - $script:DebugLogPath"
    } else { "OFF" }
    $debugColor = if ($script:DebugLogEnabled) { 'Green' } else { 'DarkGray' }

    $whatIfVal   = if ($WhatIfActive) { 'ON' } else { 'OFF' }
    $whatIfColor = if ($WhatIfActive) { 'Yellow' } else { 'DarkGray' }

    Write-Host ""
    Write-Host "================================" -ForegroundColor $hdrColor
    Write-Host "  S3 Manager v2.1$modeStr"          -ForegroundColor $hdrColor
    Write-Host "================================" -ForegroundColor $hdrColor
    Write-Host "  1. Configure connection"
    Write-Host "  2. Test connection"
    Write-Host ""
    Write-Host "  -- Bucket operations --"        -ForegroundColor DarkCyan
    Write-Host "  3. List bucket content"
    Write-Host "  4. Upload folder"
    Write-Host "  5. Download folder"
    Write-Host "  6. Delete content"
    Write-Host ""
    Write-Host "  -- Versioning --"               -ForegroundColor DarkCyan
    Write-Host "  7. Versioning status"
    Write-Host "  8. List object versions"
    Write-Host "  9. Purge all versions"
    Write-Host ""
    Write-Host "  -- Settings --"                 -ForegroundColor DarkCyan
    Write-Host "  T. Workers    [$workerVal]"
    Write-Host "  A. Audit log  [$auditVal]"      -ForegroundColor $auditColor
    Write-Host "  D. Debug log  [$debugVal]"      -ForegroundColor $debugColor
    Write-Host "  W. WhatIf     [$whatIfVal]"     -ForegroundColor $whatIfColor
    Write-Host "  C. Clear saved config"
    Write-Host ""
    Write-Host "  0. Exit  |  ?. Help"
    Write-Host "================================" -ForegroundColor $hdrColor
    Write-Host ""
}

# ---------------------------------------------------------------------------
# Region: Entry point
# ---------------------------------------------------------------------------

try {
    Import-AwsModules
}
catch {
    Write-Host "Failed to load required AWS modules: $($_.Exception.Message)" -ForegroundColor Red
    return
}

$session    = $null
$whatIfMode = $false

# Restore all settings from config at startup so they are active immediately
$_startupCfg = try { $cp = Get-S3ConfigPath; if (Test-Path -LiteralPath $cp) { Get-Content -LiteralPath $cp -Raw | ConvertFrom-Json } else { $null } } catch { $null }
if ($_startupCfg) {
    if ($_startupCfg.PSObject.Properties['ParallelWorkers']  -and [int]$_startupCfg.ParallelWorkers  -gt 0) { $script:ParallelWorkers = [int]$_startupCfg.ParallelWorkers }
    if ($_startupCfg.PSObject.Properties['DownloadWorkers']  -and [int]$_startupCfg.DownloadWorkers  -gt 0) { $script:DownloadWorkers = [int]$_startupCfg.DownloadWorkers }
    if ($_startupCfg.PSObject.Properties['DeleteWorkers']    -and [int]$_startupCfg.DeleteWorkers    -gt 0) { $script:DeleteWorkers   = [int]$_startupCfg.DeleteWorkers }
    if ($_startupCfg.PSObject.Properties['DeleteBatchSize'] -and [int]$_startupCfg.DeleteBatchSize -gt 0) { $script:DeleteBatchSize = [int]$_startupCfg.DeleteBatchSize }
if ($_startupCfg.PSObject.Properties['ForcePathStyle']) { $script:ForcePathStyle = [bool]$_startupCfg.ForcePathStyle }
    if ($_startupCfg.PSObject.Properties['AuditLogEnabled']  -and $_startupCfg.AuditLogEnabled -and
        $_startupCfg.PSObject.Properties['AuditLogPath']     -and -not [string]::IsNullOrWhiteSpace($_startupCfg.AuditLogPath)) {
        $script:AuditLogEnabled = $true
        $script:AuditLogPath    = $_startupCfg.AuditLogPath
    }
    if ($_startupCfg.PSObject.Properties['DebugLogEnabled']) { $script:DebugLogEnabled = [bool]$_startupCfg.DebugLogEnabled }
    if ($_startupCfg.PSObject.Properties['DebugLogPath']     -and -not [string]::IsNullOrWhiteSpace($_startupCfg.DebugLogPath)) {
        $script:DebugLogPath = $_startupCfg.DebugLogPath
    }
}

Write-DebugLog -Message "S3 Manager started. PID: $PID Host: $([Environment]::MachineName) User: $([Environment]::UserName) PS: $($PSVersionTable.PSVersion)" -Level DEBUG
Write-DebugLog -Message "Debug log: $script:DebugLogPath" -Level DEBUG
Write-Host "Debug logging active: $script:DebugLogPath" -ForegroundColor DarkGray

do {
    Show-Menu -WhatIfActive $whatIfMode
    $choice = Read-Host "Choose an option"

    try {
        switch ($choice) {
            '1' {
                $session = Get-S3Session
                Write-Host "Connection settings saved." -ForegroundColor Green
            }
            '2' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                Test-S3Connection -Session $session | Out-Null
            }
            '3' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                Show-BucketContent -Session $session
            }
            '4' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                if ($whatIfMode) { Copy-FolderToS3Bucket -Session $session -WhatIf }
                else             { Copy-FolderToS3Bucket -Session $session }
            }
            '5' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                if ($whatIfMode) { Copy-S3BucketToFolder -Session $session -WhatIf }
                else             { Copy-S3BucketToFolder -Session $session }
            }
            '6' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                if ($whatIfMode) { Remove-BucketContent -Session $session -WhatIf }
                else             { Remove-BucketContent -Session $session }
            }
            '7' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                Show-VersioningStatus -Session $session
            }
            '8' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                Show-BucketVersions -Session $session
            }
            '9' {
                if (-not $session) { throw "Please configure the connection first (option 1)." }
                if ($whatIfMode) { Remove-AllObjectVersions -Session $session -WhatIf }
                else             { Remove-AllObjectVersions -Session $session }
            }
            { $_ -in @('W','w') } {
                $whatIfMode = -not $whatIfMode
                $state = if ($whatIfMode) { 'ON  - operations will be previewed, nothing will be changed.' }
                         else             { 'OFF - operations will make real changes.' }
                Write-Host "Preview (WhatIf) mode is now $state" -ForegroundColor Yellow
                Write-AuditLog -Message "Preview (WhatIf) mode set to: $state" -Level INFO -NoHost
            }
            { $_ -in @('T','t') } {
                Set-WorkerCounts
            }
            { $_ -in @('A','a') } {
                Set-AuditLog -Session $session
            }
            { $_ -in @('D','d') } {
                Set-DebugLog
            }
            '0' {
                Write-AuditLog -Message "Session ended by user." -Level INFO -NoHost
                Write-Host "Goodbye." -ForegroundColor Cyan
            }
            '?' {
                Show-Help
            }
            { $_ -in @('C','c') } {
                Remove-S3Config
            }
            default {
                Write-Host "Invalid choice - use 1-9, or T/A/D/W/C/?." -ForegroundColor Yellow
            }
        }
    }
    catch {
        Write-AuditLog -Message "ERROR: $($_.Exception.Message)" -Level ERROR -NoHost
        Write-Host ""
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    }

} while ($choice -ne '0')
