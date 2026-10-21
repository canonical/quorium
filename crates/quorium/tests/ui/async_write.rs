#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Account {
    balance: u64,
}

#[quorium::state]
impl Account {
    async fn deposit(&mut self, amount: u64) {
        self.balance += amount;
    }
}

fn main() {}
