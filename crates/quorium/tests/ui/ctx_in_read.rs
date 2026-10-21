#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Clock {
    ticks: u64,
}

#[quorium::state]
impl Clock {
    fn peek(&self, ctx: &quorium::Ctx) -> u64 {
        ctx.index() + self.ticks
    }
}

fn main() {}
