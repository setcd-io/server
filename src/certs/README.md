# PKI Certificates

Self-signed PKI for localhost development and testing.

## Files

- `ca.key` - CA private key (keep secret)
- `ca.crt` - CA certificate (used as trusted CA)
- `localhost.key` - Server private key
- `localhost.crt` - Server certificate (signed by CA)

## Regenerate

```bash
# 1. Generate CA
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -subj "/CN=sEtcd CA/O=sEtcd" -out ca.crt

# 2. Generate server key
openssl genrsa -out localhost.key 4096

# 3. Create CSR config
cat > localhost.cnf << 'EOF'
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = localhost

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = etcd
DNS.3 = host.docker.internal
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

# 4. Generate and sign server certificate
openssl req -new -key localhost.key -out localhost.csr -config localhost.cnf
openssl x509 -req -in localhost.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out localhost.crt -days 3650 -sha256 -extfile localhost.cnf -extensions v3_req

# 5. Cleanup
rm localhost.csr localhost.cnf
```

## Environment Variables

Server (sEtcd):
- `ETCD_TRUSTED_CA_FILE=/certs/ca.crt`
- `ETCD_CERT_FILE=/certs/localhost.crt`
- `ETCD_KEY_FILE=/certs/localhost.key`

Client (etcdctl):
- `ETCDCTL_CACERT=/certs/ca.crt`
- `ETCDCTL_CERT=/certs/localhost.crt`
- `ETCDCTL_KEY=/certs/localhost.key`
