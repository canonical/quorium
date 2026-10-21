struct NotSerializable;

#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Store {
    n: u64,
}

#[quorium::state]
impl Store {
    fn set(&mut self, _value: NotSerializable) {
        self.n += 1;
    }
}

fn main() {}
