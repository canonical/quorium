use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String)]
pub struct Guestbook {
    names: Vec<String>,
}

#[quorium::state]
impl Guestbook {
    pub fn sign(&mut self, name: String) -> usize {
        self.names.push(name);
        self.names.len()
    }

    pub fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}
