# Tailscale Setup Guide

Setiap anggota tim harus install Tailscale agar container Docker bisa saling berkomunikasi antar laptop.

## 1. Install Tailscale

**Linux (Ubuntu/Debian):**
```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

**macOS:**
```bash
brew install tailscale
# atau download dari https://tailscale.com/download
```

**Windows:**
Download installer dari https://tailscale.com/download

## 2. Join Tailnet

Semua anggota harus login ke akun Tailscale yang **sama** (satu orang buat, yang lain join).

```bash
tailscale up
# Ikutin link login dari browser
```

## 3. Cek IP masing-masing

```bash
tailscale ip
# Output: 100.x.x.x (setiap orang dapat IP unik)
```

Catat IP tiap anggota:

| Anggota | Role | Tailscale IP |
|---------|------|-------------|
| Yoeke | Data Ingestion | `100.x.x.1` |
| Dio | Stream Processing | `100.x.x.2` |
| Fatih | ML & MLOps | `100.x.x.3` |
| Angel | Serving & Monitoring | `100.x.x.4` |

## 4. Test koneksi

Dari laptop masing-masing, coba ping anggota lain:

```bash
ping 100.x.x.1  # ping Yoeke
ping 100.x.x.4  # ping Angel
```

Jika reply, koneksi Tailscale berhasil.

## 5. Template .env

Salin file `.env.person<N>` menjadi `.env` di root proyek, lalu isi Tailscale IP yang sesuai.

```bash
# Contoh untuk Fatih (Person 3)
cp .env.person3 .env
# Edit .env, ganti 100.x.x.x dengan IP asli tiap anggota
```

## Catatan Penting

- Tailscale harus **running** saat `docker compose up`
- Pastikan firewall tidak blokir port yang di-expose
- Untuk testing, semua service bisa dijalankan di 1 laptop dulu tanpa Tailscale
