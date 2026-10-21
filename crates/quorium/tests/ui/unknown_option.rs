#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
#[state(colour = "blue")]
struct Paint {
    litres: u64,
}

fn main() {}
