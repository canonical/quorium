use std::collections::BTreeMap;

use quorium::{Ctx, Error, Quorium};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShopError {
    SoldOut(String),
    CheckedOut,
    EmptyCart,
    Unavailable(String),
}

impl<E: std::fmt::Debug> From<Error<E>> for ShopError {
    fn from(e: Error<E>) -> Self {
        ShopError::Unavailable(e.to_string())
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[state(key = String, name = "shop.Inventory")]
pub struct Inventory {
    stock: u64,
}

#[quorium::state]
impl Inventory {
    pub fn restock(&mut self, units: u64) -> u64 {
        self.stock += units;
        self.stock
    }

    pub fn reserve(&mut self, units: u64) -> Result<u64, ShopError> {
        if units > self.stock {
            return Err(ShopError::SoldOut(String::new()));
        }
        self.stock -= units;
        Ok(self.stock)
    }

    pub fn release(&mut self, units: u64) -> u64 {
        self.stock += units;
        self.stock
    }

    pub fn available(&self) -> u64 {
        self.stock
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CartStatus {
    #[default]
    Open,
    CheckedOut,
}

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[state(key = String, name = "shop.Cart")]
pub struct Cart {
    lines: BTreeMap<String, u64>,
    status: CartStatus,
}

#[quorium::state]
impl Cart {
    pub fn add(&mut self, sku: String, quantity: u64) -> Result<u64, ShopError> {
        if self.status == CartStatus::CheckedOut {
            return Err(ShopError::CheckedOut);
        }
        let line = self.lines.entry(sku).or_default();
        *line += quantity;
        Ok(*line)
    }

    pub fn remove(&mut self, sku: String) -> Result<(), ShopError> {
        if self.status == CartStatus::CheckedOut {
            return Err(ShopError::CheckedOut);
        }
        self.lines.remove(&sku);
        Ok(())
    }

    pub fn close(&mut self, ctx: &Ctx) -> Result<u64, ShopError> {
        if self.status == CartStatus::CheckedOut {
            return Err(ShopError::CheckedOut);
        }
        self.status = CartStatus::CheckedOut;
        Ok(ctx.random())
    }

    pub fn lines(&self) -> BTreeMap<String, u64> {
        self.lines.clone()
    }
}

#[derive(quorium::State, Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[state(key = u64, name = "shop.Order")]
pub struct Order {
    pub lines: BTreeMap<String, u64>,
    pub placed_at_ms: u64,
}

#[quorium::state]
impl Order {
    pub fn place(&mut self, ctx: &Ctx, lines: BTreeMap<String, u64>) -> bool {
        if !self.lines.is_empty() {
            return false;
        }
        self.lines = lines;
        self.placed_at_ms = ctx.now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
        true
    }
}

pub async fn checkout(node: &Quorium, user: &str) -> Result<u64, ShopError> {
    let cart = node.entity::<Cart>(user.to_string());
    let lines = cart.lines().await?;
    if lines.is_empty() {
        return Err(ShopError::EmptyCart);
    }
    let mut reserved: Vec<(&String, u64)> = Vec::new();
    for (sku, quantity) in &lines {
        match node.entity::<Inventory>(sku.clone()).reserve(*quantity).await {
            Ok(_) => reserved.push((sku, *quantity)),
            Err(e) => {
                for (done, units) in reserved {
                    node.entity::<Inventory>(done.clone()).release(units).await?;
                }
                return Err(match e {
                    Error::Rejected(ShopError::SoldOut(_)) => ShopError::SoldOut(sku.clone()),
                    Error::Rejected(other) => other,
                    other => other.into(),
                });
            }
        }
    }
    let order_id = match cart.close().await {
        Ok(id) => id,
        Err(e) => {
            for (sku, units) in reserved {
                node.entity::<Inventory>(sku.clone()).release(units).await?;
            }
            return Err(match e {
                Error::Rejected(inner) => inner,
                other => other.into(),
            });
        }
    };
    node.entity::<Order>(order_id).place(lines).await?;
    Ok(order_id)
}
