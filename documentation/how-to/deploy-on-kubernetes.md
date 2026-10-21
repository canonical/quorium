# Deploy on Kubernetes

This guide runs a service that embeds Quorium as a Kubernetes StatefulSet with three replicas.

## 1. Configure discovery

`Discovery::Kubernetes` resolves `<service>.<namespace>.svc.cluster.local` through DNS, again and again, and uses every address it gets as a seed:

```rust
let pod_ip: std::net::IpAddr = std::env::var("POD_IP")?.parse()?;
let node = Quorium::builder()
    .data_dir("/var/lib/shop")
    .bind("0.0.0.0:7000".parse()?)
    .advertise(std::net::SocketAddr::new(pod_ip, 7000))
    .datacenter(&std::env::var("ZONE")?)
    .discovery(Discovery::Kubernetes { service: "shop".into(), namespace: "prod".into(), port: 7000 })
    .replicas(3)
    .tls(tls()?)
    .start()
    .await?;
```

- `bind` is the local listening address. `advertise` is the address other nodes use to reach this one, so it must be the pod IP, not `0.0.0.0`.
- `datacenter` should be the pod's zone, so that leadership and escrow copies respect failure domains.

## 2. Create a headless service

DNS must return the address of every pod, including pods that are not ready yet: the cluster has to form before your service becomes ready.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: shop
  namespace: prod
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: shop
  ports:
    - name: quorium-quic
      port: 7000
      protocol: UDP
    - name: quorium-tcp
      port: 7000
      protocol: TCP
```

QUIC, the default transport, uses UDP. Open UDP on the port, and TCP as well if you use `Transport::Tcp`.

## 3. Give each pod a persistent volume

A node's identity and data live in `data_dir`. Mount a persistent volume there through `volumeClaimTemplates`. If a pod comes back with an empty volume, it joins as a new node, and the old identity is eventually replaced.

## 4. Pass the pod IP and zone

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: ZONE
    valueFrom:
      fieldRef:
        fieldPath: metadata.labels['topology.kubernetes.io/zone']
```

The zone label is not on pods by default. Copy it there, or use another source for it, such as a downward API annotation set by your deployment tooling.

## 5. Spread pods across zones

Add a `topologySpreadConstraints` entry on `topology.kubernetes.io/zone`. With three voters in three zones, losing one zone keeps every shard available.

## 6. Scale

- The first `replicas` pods (3 or 5) become voters. Additional pods join as learners, which serve as standby replacements.
- During a rolling update, Kubernetes restarts one pod at a time, which is what Quorium needs. See [Upgrade a cluster](upgrade-a-cluster.md).

## Related

- [Secure a cluster with TLS](secure-a-cluster-with-tls.md)
- [Replace a node](replace-a-node.md)
- [Builder options](../reference/builder.md)
