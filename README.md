# CNS Orchestrator

## Setup

```
npm install

npm run start
```

## Environment Variables

- `CNS_HOST` - The ETCD Host (http://etcd.cns.dev)
- `CNS_PORT` - The ETCD Port (2379)
- `CNS_USERNAME`
- `CNS_PASSWORD`

## Kubernetes

```
kubectl create secret generic \
  -n <NAMESPACE> \
  --from-literal CNS_PASSWORD=<PASSWORD> \
  cns-password

kubectl apply -f kubernetes/deployment.yaml
```

