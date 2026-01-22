# Panduan Deploy guna Coolify + GitHub (Cara Pro) 🚀

Okay boss, ini cara paling kemas & moden. Kita guna GitHub supaya Coolify boleh tarik code terus.

---

## Langkah 1: Setup GitHub (Di Laptop Boss)

Kita kena masukkan code ni ke dalam GitHub dulu.

1. **Login GitHub**: Pergi ke [github.com](https://github.com) dan login.
2. **Buat Repo Baru**:
    * Tekan butang **(+)** dekat atas kanan -> **New repository**.
    * Nama: `boda8-bot` (atau apa-apa boss suka).
    * **Private**: Pilih "Private" (Sebab kita tak nak orang tengok code boss).
    * Tekan **Create repository**.
3. **Upload Code**:
    * Boss akan nampak page yang ada arahan `git remote add origin...`. **Copy** link HTTPS repo tu (contoh: `https://github.com/Start-X/boda8-bot.git`).
    * Buka terminal di folder `boda8-src` (tempat boss ada code sekarang), taip ni:

    ```powershell
    # Tukar URL bawah ni dengan URL Repo boss sendiri!
    git remote add origin https://github.com/Start-X/boda8-bot.git
    
    git branch -M main
    git push -u origin main
    ```

    *(Mungkin dia akan minta username/password GitHub. Kalau password tak jalan, guna Personal Access Token).*

---

## Langkah 2: Setup Coolify (Di Browser)

1. **Buka Coolify**: Masuk URL Coolify boss.
2. **Create Project**:
    * Projects -> **+ New** -> namakan `Boda8 Project`.
3. **Add Resource (Database dulu)**:
    * Tekan **+ New** -> **PostgreSQL**.
    * Destination: `localhost` (atau server boss).
    * **Penting**: Copy **Password** user `postgres` yang dia bagi. Simpan notepad.
    * Start database tu.
4. **Add Resource (Bot)**:
    * Tekan **+ New** -> **Private Repository (with App Token)**.
    * (Dia akan minta connect GitHub Apps, ikut je arahan dia sampai repo `boda8-bot` tu muncul).
    * Pilih repo `boda8-bot`.
    * Branch: `main`.
    * Build Pack: **Docker Compose** (Sebab kita dah ada fail `docker-compose.yml`).
    * **Environment Variables**:
        Masuk dalam menu "Environment Variables", tambah ni:
        * `TOKEN`: (Token Bot Boss)
        * `PUBLIC_BASE_URL`: (Domain Coolify bagi nanti, cth `https://boda8.coolify.domain.com`)
        * `DATABASE_URL`: `postgresql://postgres:PASSWORD_TADI@host.docker.internal:5432/boda8db`
            *(Ganti `PASSWORD_TADI` dengan password database step 3. `host.docker.internal` tu cara Coolify connect DB internal)*.
5. **Deploy**:
    * Tekan butang **Deploy**.

---

## Langkah 3: Restore Database (Paling Mencabar sikit)

Sebab database dalam Coolify ni "tersembunyi" sikit, kita kena inject manual.

1. Upload fail `backup.sql` ke VPS guna **WinSCP** (letak kat `/root/`).
2. Guna **Dua-dua Terminal** (WinSCP punya PuTTY):
    * Dapatkan ID Container Database:

        ```bash
        docker ps | grep postgres
        ```

        *(Ambil ID depan sekali, contoh `a1b2c3d4`)*

    * Copy & Restore:

        ```bash
        # Copy fail masuk container
        docker cp /root/backup.sql <ID_CONTAINER>:/backup.sql
        
        # Restore
        docker exec -i <ID_CONTAINER> psql -U postgres -d postgres -f /backup.sql
        ```

---

## Langkah 4: Webhook

Bila Coolify dah bagi link `https://...`, boss set webhook macam biasa:

`https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<DOMAIN_COOLIFY>/telegram/webhook`

Siap! 🔥
