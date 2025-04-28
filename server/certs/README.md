# Generated with `mkcert`

1. `brew install mkcert`
1. `mkcert -install`
1. `mkcert localhost 127.0.0.1 ::1`

# Generated with `openssl`

```bash
openssl req -x509 -newkey rsa:4096 -nodes -keyout localhost.key -out localhost.crt -days 3650 \
  -subj "/CN=localhost" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:host.docker.internal"
```
