#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Wrapper<T> {
    inner: T,
}

fn main() {}
