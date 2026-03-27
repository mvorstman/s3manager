param(
    [int]$Count = 100,
    [string]$OutputPath = "C:\temp\s3-test-data",
    [int]$FileSizeBytes = 1024,
    [int]$Depth = 0
)

Write-Host "Generating $Count files..."
Write-Host "Output path: $OutputPath"
Write-Host "File size: $FileSizeBytes bytes"
Write-Host "Folder depth: $Depth"

# Create base directory
New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null

# Function to generate random folder structure
function Get-RandomSubPath {
    param([int]$Depth)

    if ($Depth -le 0) {
        return ""
    }

    $path = $null

    for ($i = 0; $i -lt $Depth; $i++) {
        $folder = "folder$((Get-Random -Minimum 1 -Maximum 10))"

        if (-not $path) {
            # First folder → just assign
            $path = $folder
        } else {
            # Subsequent folders → use Join-Path
            $path = Join-Path $path $folder
        }
    }

    return $path
}

# Generate files
for ($i = 1; $i -le $Count; $i++) {

    # Optional nested folders
    $subPath = Get-RandomSubPath -Depth $Depth
    $fullDir = Join-Path $OutputPath $subPath

    # Ensure directory exists
    New-Item -ItemType Directory -Path $fullDir -Force | Out-Null

    # File name
    $fileName = "file_{0:D6}.txt" -f $i
    $filePath = Join-Path $fullDir $fileName

    # Generate random bytes
    $bytes = New-Object byte[] $FileSizeBytes
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)

    # Write file
    [System.IO.File]::WriteAllBytes($filePath, $bytes)

    if ($i % 100 -eq 0) {
        Write-Host "Created $i files..."
    }
}

Write-Host "Done. Created $Count files."