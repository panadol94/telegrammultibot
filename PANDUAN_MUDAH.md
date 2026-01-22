# Panduan Mudah Pindah ke VPS (Untuk Beginner) 🔰

Jangan risau boss, kita buat pelan-pelan. Tak perlu jadi pakar coding pun boleh buat. Kita guna *tools* yang ada "button" supaya senang nampak.

Kita akan guna software nama **WinSCP** (percuma). Dia macam "File Manager" tapi boleh connect ke VPS.

---

## Langkah 1: Persediaan (Download Tools)

1. **Download WinSCP**:
    * Pergi ke Google, cari "Download WinSCP" atau [klik sini](https://winscp.net/eng/download.php).
    * Install macam biasa (Next > Next > Install).

2. **Download PuTTY**:
    * WinSCP biasanya akan tanya nak install PuTTY sekali, kalau ada cakap "Yes".
    * Kalau tak, cari "Download PuTTY" kat Google dan install. Ini untuk kita taip command.

---

## Langkah 2: Masuk ke VPS

1. Buka **WinSCP**.
2. Dia akan keluar kotak "Login". Isi maklumat VPS boss:
    * **Host name**: (Masukkan IP Address VPS, contoh: `123.45.67.89`)
    * **User name**: `root`
    * **Password**: (Password VPS boss)
3. Tekan **Login**.
4. Kalau dia tanya "Warning" (kunci/key), tekan **Yes** atau **Accept**.

Sekarang boss akan nampak dua belah:

* **Kiri**: Fail dalam Laptop boss.
* **Kanan**: Fail dalam VPS (biasanya kosong).

---

## Langkah 3: Upload Fail

1. Di sebelah **Kiri** (Laptop), cari fail zip yang kita buat tadi:
    * `C:\Users\Acer\boda8-migration.zip`
2. **Tarik (Drag)** fail zip tu ke sebelah **Kanan** (VPS).
3. Tunggu sampai siap upload.

---

## Langkah 4: Run Command (Guna PuTTY)

1. Dalam WinSCP tadi, tengok bar kat atas, ada icon macam **"Dua Plug Bersambung"** atau icon **"Terminal Hitam"** (Open Session in PuTTY). Klik icon tu.
2. Satu skrin hitam (Terminal) akan keluar.
3. Sekarang kita cuma perlu **Copy & Paste** command ni satu-satu. (Nak paste dalam PuTTY, cuma **klik kanan mouse**).

### A. Install Docker (Kalau belum ada)

Copy semua ni sekali harung, lepas tu klik kanan kat skrin hitam tu, tekan Enter:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh
```

*(Tunggu sampai dia berhenti bergerak)*

### B. Unzip Fail & Setup

Copy ni (tekan Enter lepas paste):

```bash
# Install unzip
apt install unzip -y

# Buka fail zip tadi
unzip boda8-migration.zip -d boda8-bot

# Masuk dalam folder
cd boda8-bot
```

### C. Run Bot

Sekarang kita hidupkan bot tu. Copy ni:

```bash
docker compose up -d
```

*(Dia akan download banyak benda, tunggu sampai siap dan nampak tulisan "Started" warna hijau)*

---

## Langkah 5: Restore Database (Penting!)

Sekarang bot dah hidup, tapi kosong. Kita masukkan data lama.

Copy ni dan paste:

```bash
# Copy fail backup masuk dalam database
docker cp backup.sql boda8-db:/backup.sql

# Restore data (masukkan data lama)
docker exec -i boda8-db psql -U boda8 -d boda8db -f /backup.sql
```

Kalau nampak banyak tulisan laju-laju keluar macam `CREATE TABLE`, `ALTER TABLE`... maknanya **BERJAYA!** ✅

---

## Langkah Terakhir: Tukar Webhook

Sekarang kita bagitahu Telegram "Hantar mesej ke VPS baru ni, jangan hantar ke Cloud lama".

Ganti `<TOKEN_BOSS>` dengan token bot boss, dan `<IP_VPS>` dengan IP VPS boss. Copy link ni masuk browser (Chrome/Edge) pun boleh:

`https://api.telegram.org/bot<TOKEN_BOSS>/setWebhook?url=http://<IP_VPS>:8080/telegram/webhook`

*(Contoh: <http://123.45.67.89:8080/telegram/webhook>)*

Kalau browser tulis `Webhook was set`, maknanya **SIAP!** 🎉

---

## Kalau Stuck?

Kalau jam atau tak jadi, bagitahu saya step mana yang boss sangkut. Boleh screenshot error tu kalau ada.
