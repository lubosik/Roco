#!/bin/bash
# Run this once on the VPS to set up Nginx reverse proxy
# Usage: bash scripts/setupNginx.sh your-domain.com
set -e

DOMAIN=${1:-"YOUR_DOMAIN_HERE"}

apt-get install -y nginx certbot python3-certbot-nginx

cat > /etc/nginx/sites-available/roco << EOF
server {
    listen 80;
    server_name ${DOMAIN};

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_cache_bypass \$http_upgrade;
    }
}
EOF

# Enable the site
if [ ! -L /etc/nginx/sites-enabled/roco ]; then
  ln -s /etc/nginx/sites-available/roco /etc/nginx/sites-enabled/
fi

# Remove default site if present
rm -f /etc/nginx/sites-enabled/default

nginx -t && systemctl reload nginx

echo "Nginx configured for domain: ${DOMAIN}"
echo ""
echo "If your domain is pointed at this VPS, run:"
echo "  certbot --nginx -d ${DOMAIN}"
echo ""
echo "To set up HTTPS automatically."
