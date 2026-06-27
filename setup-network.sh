#!/bin/bash
# Setup jaringan untuk Gold Prediction System
# ============================================
# Opsi 1: Semua service di 1 laptop (Docker network)
# Opsi 2: Multi-mesin via Tailscale (tidak perlu Docker network)

echo "=== Gold Prediction System — Network Setup ==="
echo ""
echo "Pilih mode deployment:"
echo "1) Single machine — semua service di laptop ini"
echo "2) Multi-mesin via Tailscale — tiap anggota jalanin service masing-masing"
echo ""
read -p "Pilihan [1/2]: " mode

if [ "$mode" = "1" ]; then
    echo "Creating Docker network: gold-network..."
    docker network create gold-network 2>/dev/null && echo "OK" || echo "Already exists"
    echo ""
    echo "📋 Semua service bisa jalan di laptop ini."
    echo "   Run: docker compose -f docker-compose-person1.yaml up -d"
    echo "   Run: docker compose -f docker-compose-person2.yaml up -d"
    echo "   Run: docker compose -f docker-compose-person3.yaml up -d"
    echo "   Run: docker compose -f docker-compose-person4.yaml up -d"
elif [ "$mode" = "2" ]; then
    echo ""
    echo "📋 Mode Tailscale terpilih."
    echo ""
    echo "Pastikan:"
    echo "  ✅ Tailscale terinstall di semua laptop"
    echo "  ✅ Semua anggota join ke Tailnet yang sama"
    echo "  ✅ Masing-masing sudah catat IP Tailscale (tailscale ip)"
    echo "  ✅ File .env sudah diisi dengan IP Tailscale tiap anggota"
    echo ""
    echo "Lihat panduan lengkap: TAILSCALE-SETUP.md"
else
    echo "Pilihan tidak valid."
    exit 1
fi
