# sEtcd Server

A ground-up rewrite of `etcd` in NodeJS using ConnectRPC. Designed to work in server-less environments.

## Testing

### `microsoft/etcd3` tests

```
make test SERVE=dynamodb-local RUN=microsoft-etcd3
```

### `kubernetes/apiserver/.../etcd3` tests

```
make test SERVE=dynamodb-local RUN=kubernetes-etcd3
```

### Patches

1. Run `make test SERVE=dynamodb-local RUN=kubernetes-etcd3`
1. Go into `.cache/kubernetes-etcd3`
1. Edit the code to desired
1. Run `make patches SERVE=dynamodb-local RUN=kubernetes-etcd3`

## Maintainers

- [Scaffoldly](https://github.com/scaffoldly)

## License

[FSL-1.1-ALv2](LICENSE.md)

Copyright 2025 Scaffoldly LLC
