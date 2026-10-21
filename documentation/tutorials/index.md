# Tutorials

Tutorials are lessons. Follow them in order, typing every step; each one ends with something that works.

1. [Your first cluster](first-cluster.md): build a replicated guestbook, run it as three processes, and watch it survive the loss of a node.
2. [Test your service against failures](test-your-service.md): write tests that kill nodes and partition the network, both on real processes and on the deterministic simulator.

You need Rust 1.88 or newer and about 15 minutes for each tutorial.

```{toctree}
:hidden:

first-cluster
test-your-service
```
