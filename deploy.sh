#!/bin/bash
# Скрипт автоматической установки бота на сервере

set -e

echo "=== 1. Установка зависимостей ==="
apt update
apt install -y python3-pip nginx certbot python3-certbot-nginx git

echo "=== 2. Установка Python библиотек ==="
pip3 install aiogram aiohttp --break-system-packages

echo "=== 3. Скачивание бота ==="
mkdir -p /opt/fingoal
cd /opt/fingoal
if [ -d ".git" ]; then
    git pull
else
    git clone https://github.com/sergeychistiy14-ai/money_app.git .
fi

echo "=== 4. Создание systemd сервиса ==="
cat > /etc/systemd/system/fingoal-bot.service << 'EOF'
[Unit]
Description=FinGoal Telegram Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/fingoal
ExecStart=/usr/bin/python3 bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable fingoal-bot
systemctl restart fingoal-bot

echo "=== 5. Настройка Nginx ==="
cat > /etc/nginx/sites-available/fingoal << 'EOF'
server {
    listen 80;
    server_name fingoal.ru;
    
    location /api/ {
        proxy_pass http://127.0.0.1:8080/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

ln -sf /etc/nginx/sites-available/fingoal /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

echo "=== 6. Получение SSL сертификата ==="
certbot --nginx -d fingoal.ru --non-interactive --agree-tos --register-unsafely-without-email || echo "SSL ошибка, продолжаем..."

echo "=== ГОТОВО! ==="
echo "Статус бота:"
systemctl status fingoal-bot --no-pager
