$lines = Select-String -Path 'Docs/log (5).txt' -Pattern 'TryHandleIncoming.*PayloadLen='
$sizes = $lines | ForEach-Object { if ($_.Line -match 'PayloadLen=(\d+)') { [int]$Matches[1] } }
Write-Host "Total packets: $($sizes.Count)"
Write-Host "Total bytes incoming: $(($sizes | Measure-Object -Sum).Sum)"
Write-Host ""
Write-Host "Top payload sizes:"
$sizes | Group-Object | Sort-Object Count -Descending | Select-Object -First 10 | Format-Table

Write-Host ""
Write-Host "WinSize distribution:"
$winsizes = $lines | ForEach-Object { if ($_.Line -match 'WinSize=(\d+)') { [int]$Matches[1] } }
$winsizes | Group-Object | Sort-Object Count -Descending | Select-Object -First 10 | Format-Table

Write-Host ""
Write-Host "Unique connections:"
$conns = $lines | ForEach-Object { if ($_.Line -match '\[Conn ([0-9>-]+)\]') { $Matches[1] } } | Sort-Object -Unique
Write-Host "Count: $($conns.Count)"

Write-Host ""
Write-Host "Sequences with WinSize=256 (scaled?):"
$winsizes | Where-Object { $_ -lt 1000 } | Measure-Object | Select-Object Count, Sum
Write-Host ""
Write-Host "Sample lines with small windows:"
$lines | Where-Object { $_.Line -match 'WinSize=([0-9]{1,3})\b' } | Select-Object -First 3 | ForEach-Object { $_.Line }
