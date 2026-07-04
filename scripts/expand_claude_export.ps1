param(
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputDir
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ZipPath -PathType Leaf)) {
    throw "Claude export zip not found: $ZipPath"
}

if (Test-Path -LiteralPath $OutputDir -PathType Leaf) {
    throw "OutputDir is a file: $OutputDir"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Test-KnownMetadataEntry {
    param([Parameter(Mandatory = $true)][string]$EntryName)
    return $EntryName -eq "users.json" -or $EntryName -eq "memories.json"
}

function Test-ConversationObject {
    param([Parameter(Mandatory = $true)]$Value)
    $properties = @($Value.PSObject.Properties.Name)
    return $properties.Contains("uuid") -and (
        $properties.Contains("messages") -or $properties.Contains("chat_messages")
    )
}

$archive = [System.IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $ZipPath))
try {
    $count = 0
    $seenConversationIds = [System.Collections.Generic.HashSet[string]]::new()
    $invalidFileNameChars = [System.IO.Path]::GetInvalidFileNameChars()
    foreach ($entry in $archive.Entries) {
        if (-not $entry.FullName.EndsWith(".json", [StringComparison]::OrdinalIgnoreCase)) {
            continue
        }

        $reader = [System.IO.StreamReader]::new($entry.Open())
        try {
            $json = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }

        $parsed = $json | ConvertFrom-Json -Depth 100
        $conversations = [System.Collections.ArrayList]::new()
        if ($parsed -is [array]) {
            foreach ($conversation in @($parsed)) {
                if (-not (Test-ConversationObject -Value $conversation)) {
                    throw "JSON entry contains a non-conversation item: $($entry.FullName)"
                }
                [void]$conversations.Add($conversation)
            }
        }
        elseif ($null -ne $parsed.conversations) {
            foreach ($conversation in @($parsed.conversations)) {
                [void]$conversations.Add($conversation)
            }
        }
        elseif (Test-ConversationObject -Value $parsed) {
            [void]$conversations.Add($parsed)
        }
        elseif (-not (Test-KnownMetadataEntry -EntryName $entry.FullName)) {
            throw "Unsupported Claude export JSON entry: $($entry.FullName)"
        }

        foreach ($conversation in $conversations) {
            if ([string]::IsNullOrWhiteSpace($conversation.uuid)) {
                throw "Conversation uuid is missing in $($entry.FullName)"
            }
            if ($conversation.uuid.IndexOfAny($invalidFileNameChars) -ge 0) {
                throw "Conversation uuid contains invalid filename characters: $($conversation.uuid)"
            }
            if (-not $seenConversationIds.Add([string]$conversation.uuid)) {
                throw "Duplicate conversation uuid in export: $($conversation.uuid)"
            }
            if ($null -eq $conversation.messages -and $null -ne $conversation.chat_messages) {
                $conversation | Add-Member -NotePropertyName "messages" -NotePropertyValue $conversation.chat_messages
                $conversation.PSObject.Properties.Remove("chat_messages")
            }
            elseif ($null -eq $conversation.messages) {
                throw "Conversation messages are missing in $($entry.FullName): $($conversation.uuid)"
            }
            $fileName = "$($conversation.uuid).json"
            $target = Join-Path $OutputDir $fileName
            $conversation | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $target -Encoding utf8NoBOM
            $count += 1
        }
    }

    if ($count -eq 0) {
        throw "No conversations were expanded from $ZipPath"
    }

    Write-Output "expanded_conversations=$count"
}
finally {
    $archive.Dispose()
}
