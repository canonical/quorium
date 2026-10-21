#[derive(quorium::State, serde::Serialize, serde::Deserialize, Default)]
struct Light {
    on: bool,
}

#[quorium::state]
impl Light {
    #[quorium(id = 1)]
    fn on(&mut self) {
        self.on = true;
    }

    #[quorium(id = 1)]
    fn off(&mut self) {
        self.on = false;
    }
}

fn main() {}
