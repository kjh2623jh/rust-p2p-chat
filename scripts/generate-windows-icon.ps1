param(
    [string]$SourcePng = "assets/app-icon.png",
    [string]$OutputIco = "assets/app-icon.ico"
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing

function New-ResizedPngBytes {
    param(
        [System.Drawing.Image]$Source,
        [int]$Size
    )

    $bitmap = New-Object System.Drawing.Bitmap($Size, $Size, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    try {
        $graphics.CompositingMode = [System.Drawing.Drawing2D.CompositingMode]::SourceCopy
        $graphics.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
        $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
        $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
        $graphics.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
        $graphics.DrawImage($Source, 0, 0, $Size, $Size)

        $stream = New-Object System.IO.MemoryStream
        try {
            $bitmap.Save($stream, [System.Drawing.Imaging.ImageFormat]::Png)
            return ,$stream.ToArray()
        }
        finally {
            $stream.Dispose()
        }
    }
    finally {
        $graphics.Dispose()
        $bitmap.Dispose()
    }
}

$sourcePath = (Resolve-Path -LiteralPath $SourcePng).Path
$outputPath = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $OutputIco))
$sourceImage = [System.Drawing.Image]::FromFile($sourcePath)

try {
    if ($sourceImage.Width -ne 1024 -or $sourceImage.Height -ne 1024) {
        $normalizedBytes = New-ResizedPngBytes -Source $sourceImage -Size 1024
        $sourceImage.Dispose()
        $sourceImage = $null
        [System.IO.File]::WriteAllBytes($sourcePath, $normalizedBytes)
        $sourceImage = [System.Drawing.Image]::FromFile($sourcePath)
    }

    $sizes = @(16, 24, 32, 48, 64, 128, 256)
    $images = foreach ($size in $sizes) {
        [PSCustomObject]@{
            Size = $size
            Data = [byte[]](New-ResizedPngBytes -Source $sourceImage -Size $size)
        }
    }

    $outputDirectory = [System.IO.Path]::GetDirectoryName($outputPath)
    [System.IO.Directory]::CreateDirectory($outputDirectory) | Out-Null
    $fileStream = [System.IO.File]::Create($outputPath)
    $writer = New-Object System.IO.BinaryWriter($fileStream)
    try {
        $writer.Write([uint16]0)
        $writer.Write([uint16]1)
        $writer.Write([uint16]$images.Count)

        $offset = 6 + (16 * $images.Count)
        foreach ($image in $images) {
            $dimension = if ($image.Size -eq 256) { 0 } else { $image.Size }
            $writer.Write([byte]$dimension)
            $writer.Write([byte]$dimension)
            $writer.Write([byte]0)
            $writer.Write([byte]0)
            $writer.Write([uint16]1)
            $writer.Write([uint16]32)
            $writer.Write([uint32]$image.Data.Length)
            $writer.Write([uint32]$offset)
            $offset += $image.Data.Length
        }

        foreach ($image in $images) {
            $writer.Write($image.Data)
        }
    }
    finally {
        $writer.Dispose()
        $fileStream.Dispose()
    }
}
finally {
    if ($null -ne $sourceImage) {
        $sourceImage.Dispose()
    }
}

Write-Host "Generated $sourcePath (1024x1024) and $outputPath"
