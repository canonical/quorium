#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Bag {
    count: u64,
}

#[quorium::state]
impl Bag {
    fn put<T: serde::Serialize>(&mut self, _item: T) {
        self.count += 1;
    }
}

fn main() {}
