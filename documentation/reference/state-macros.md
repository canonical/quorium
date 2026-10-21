# State macros

A state type is declared with two macros: a derive on the struct and an attribute on its `impl` block.

```{code-block} rust
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Cart {
    items: Vec<String>,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, item: String) -> usize { /* write */ }
    pub fn total(&self) -> usize { /* read */ }
}
```

## `#[derive(quorium::State)]`

The struct must also derive or implement `Serialize`, `Deserialize` and `Default`. It must not be generic.

### `#[state(...)]` options

| Option | Default | Meaning |
|---|---|---|
| `key = Type` | `String` | Key type. Allowed: `String`, `u8`–`u128`, `i8`–`i128`, `bool`, `char`, `()`, tuples of 2 or 3 allowed types, and `uuid::Uuid` with the `uuid` feature |
| `name = "..."` | the type's name | Storage identity. Must stay the same forever |
| `version = N` | `1` | Encoding version. Bump it when the stored fields change |
| `migrate = path` | none | `fn(from: u32, bytes: &[u8]) -> Result<Self, MigrationError>`, called for state stored at an older version |
| `locality = path` | none | `fn(&Key) -> String`. Entities with the same locality are placed together |

A type with `key = ()` has one instance per cluster, reached with `node.global::<T>()`.

## `#[quorium::state]`

Turns each `pub fn` of the block into a method on the type's handle.

| Method form | Becomes | Runs |
|---|---|---|
| `fn m(&mut self, ...)` | Replicated write | On every replica, in log order |
| `fn m(&self, ...)` | Read | On local state, after a linearizable check |

Handle methods are `async`. A method returning `Result<T, E>` returns `Result<T, quorium::Error<E>>` on the handle; any other return type `T` returns `Result<T, quorium::Error<Infallible>>`.

### Rules checked at compile time

| Rule | Error when broken |
|---|---|
| Write methods are synchronous | `write methods (&mut self) must be synchronous` |
| Write methods are not generic | `write methods (&mut self) cannot be generic` |
| The state type is not generic | `#[derive(quorium::State)] does not support generic types` |
| Arguments are `Serialize + DeserializeOwned + Send`, and return values `Serialize + DeserializeOwned` | A trait-bound error naming the type |
| `ctx: &Ctx` is only taken by write methods | `ctx: &Ctx is only available to write methods` |
| Write method IDs are unique | `write method ... has ID N which is already used by ...` |
| `#[state]` options are known | `unknown #[state] option` |

### Method IDs

Each write method has a stable numeric ID: a hash of its name by default. Pin it with `#[quorium(id = N)]` so that renaming the method does not change its identity in the log:

```{code-block} rust
#[quorium(id = 1)]
pub fn withdraw(&mut self, amount: i64) -> Result<i64, AccountError> { /* ... */ }
```

## `Ctx`

A write method may take `ctx: &Ctx` as its first parameter after `self`. Quorium supplies it; callers do not pass it.

| Method | Returns |
|---|---|
| `ctx.index()` | The write's position in the log, unique per shard |
| `ctx.now()` | A timestamp chosen when the write was proposed, the same on every replica |
| `ctx.random()` | A pseudo-random `u64`, the same on every replica. Each call returns a new value |

Never read the system clock or a random generator inside a write: replicas would diverge.

## `Escrow<T>` fields

A field of type `quorium::Escrow<T>`, with `T` one of `u8`, `u16`, `u32`, `u64`, adds a method named after the field to the handle. See [Escrow](escrow.md).
