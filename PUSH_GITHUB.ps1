# PUSH_GITHUB.ps1
# Skrip automasi untuk upload ke GitHub

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "   BODA8 BOT - GITHUB UPLOADER   " -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Saya dah masukkan link boss siap-siap
$RepoUrl = "https://github.com/panadol94/telegrammultibot.git"

Write-Host "Target GitHub: $RepoUrl" -ForegroundColor Green
Write-Host "Setup: Secure (Secret files excluded)" -ForegroundColor Green
Write-Host ""
Write-Host "Sedang setup Git..." -ForegroundColor Yellow

# Reset remote jika ada (supaya link sentiasa betul)
git remote remove origin 2>$null
git remote add origin $RepoUrl

# Rename branch ke main
git branch -M main

Write-Host "Sedang upload ke GitHub..." -ForegroundColor Yellow
Write-Host "NOTA: Kalau keluar popup login, sila login akaun GitHub anda." -ForegroundColor Yellow

try {
    git push -u origin main
    Write-Host ""
    Write-Host "✅ SIAP! Code dah ada dalam GitHub." -ForegroundColor Green
}
catch {
    Write-Host ""
    Write-Host "❌ GAGAL. Sila pastikan anda login bila diminta." -ForegroundColor Red
    Write-Host $_
}

Write-Host ""
Write-Host "Tekan Enter untuk tutup..."
pause
