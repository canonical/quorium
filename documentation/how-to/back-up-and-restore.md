# Back up and restore

## Take a backup

```rust
node.export(Path::new("/backups/2026-10-21.qbak")).await?;
```

`export` reads every shard through a linearizable read and writes one checksummed archive. The file appears atomically: a crash during export never leaves a partial archive under that name. You can call it on any node.

Schedule it with a [singleton job](run-a-singleton-job.md) so that one node takes each backup:

```rust
let exporter = node.clone();
let _backups = node.singleton("backup", move |_lease| {
    let node = exporter.clone();
    async move {
        loop {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
            let name = format!("/backups/{now}.qbak");
            if let Err(e) = node.export(Path::new(&name)).await {
                eprintln!("backup failed: {e}");
            }
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
});
```

## Restore into a new cluster

1. Start a new cluster with the same shard count as the one that was backed up, and the same state types registered.
2. On one node, import the archive:

```rust
new_node.import(Path::new("/backups/2026-10-21.qbak")).await?;
```

`import` replicates the archive's state into the cluster. It refuses shards that already hold data, so you cannot overwrite a live cluster by mistake.

## Limits

- Each shard's state travels in one message, so a shard must hold less than 16 MiB to be exported. Use more shards for larger datasets.
- A backup is a consistent snapshot of each shard, but shards are read one after the other, not at a single instant. A write that touches two entities in different shards may appear half-applied in the backup, exactly as it can appear to any reader.

## Related

- [Data directory and formats](../reference/data-directory.md)
