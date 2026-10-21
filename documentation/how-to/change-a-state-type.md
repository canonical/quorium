# Change a state type safely

State types are stored on disk and their write methods travel through the replicated log, so some changes need care. This guide lists the safe way to make each kind of change.

## Pin the identities first

Two identities must never change once data exists:

- The type's storage name. It defaults to the type name; set it explicitly so you can rename the Rust type later:

  ```{code-block} rust
  #[state(key = u64, name = "bank.Account")]
  ```

- Each write method's ID. It defaults to a hash of the method name; pin it so you can rename the method later:

  ```{code-block} rust
  #[quorium(id = 1)]
  pub fn withdraw(&mut self, amount: i64) -> Result<i64, AccountError> { /* ... */ }
  ```

## Add a write method

1. Add the method.
2. Roll the new binary out to every node, one at a time.

Until every voter of a shard runs a binary that knows the method, calls to it return `Error::Unavailable`, which is retryable. This stops a new method from running on some replicas and failing on others.

## Change a write method's behaviour

Every replica must compute the same result from the same log. During a rolling upgrade, old and new binaries apply the same entries side by side, so a method whose logic changes must keep its old ID and behave the same on old entries. When in doubt, add a new method with a new ID and stop calling the old one.

## Change the stored fields

Adding, removing or retyping a field changes the encoding. Bump the version and supply a migration from every older version:

```rust
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = u64, name = "bank.Account", version = 2, migrate = migrate_account)]
pub struct Account {
    balance: i64,
    currency: String,
}

#[derive(Deserialize)]
struct AccountV1 {
    balance: i64,
}

fn migrate_account(from: u32, bytes: &[u8]) -> Result<Account, MigrationError> {
    match from {
        1 => {
            let old: AccountV1 = postcard::from_bytes(bytes).map_err(|e| MigrationError::new(e.to_string()))?;
            Ok(Account { balance: old.balance, currency: "EUR".into() })
        }
        v => Err(MigrationError::new(format!("unknown Account version {v}"))),
    }
}
```

Stored state is encoded with [postcard](https://crates.io/crates/postcard), so decode old versions with it. Old state is migrated the next time it is read or written.

Postcard has no optional fields: even adding a field at the end needs a new version.

## Remove a state type

Do not remove a type while stored state still contains it. A node refuses to start if its data holds a type the binary does not register, and names the type in the error. Delete every entity of the type first, then remove it in a later release.

## Related

- [`#[state]` reference](../reference/state-macros.md)
- [Upgrade a cluster](upgrade-a-cluster.md)
