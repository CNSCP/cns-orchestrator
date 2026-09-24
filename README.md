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
- `CP_REGISTRY_URL` - The CP Registry that Connection Profiles are resolved from (https://cp.cnscp.io).
  Point it at a local Registry instance to resolve without reaching the internet.
  `CNS_PROFILES` (the old profile server) is no longer read.
- `CNS_RECONCILE_INTERVAL` - How often, in ms, the store is re-read and held Profiles are revalidated (30000)

## Tests

```
npm test
```

Runs the Registry resolver's tests against recorded Registry answers; no network needed.

## Kubernetes

```
kubectl create secret generic \
  -n <NAMESPACE> \
  --from-literal CNS_PASSWORD=<PASSWORD> \
  cns-password

kubectl apply -f kubernetes/deployment.yaml
```
