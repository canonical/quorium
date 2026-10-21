use std::marker::PhantomData;

use quorium_core::kv::{EscrowAction, EscrowView};
use serde::{Deserialize, Serialize};

pub trait EscrowUnits: Copy + Into<u64> + TryFrom<u64> + Send + Sync + 'static {
    fn saturate(units: u64) -> Self;
}

macro_rules! units {
    ($($t:ty),*) => {
        $(impl EscrowUnits for $t {
            fn saturate(units: u64) -> Self {
                <$t>::try_from(units).unwrap_or(<$t>::MAX)
            }
        })*
    };
}

units!(u8, u16, u32, u64);

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Escrow<T: EscrowUnits = u64> {
    view: EscrowView,
    #[serde(skip)]
    units: PhantomData<T>,
}

impl<T: EscrowUnits> Escrow<T> {
    pub fn refill(&mut self, units: T) {
        let _ = self.view.apply(0, EscrowAction::Refill { units: units.into() });
    }

    pub fn estimate(&self) -> T {
        T::saturate(self.view.estimate())
    }

    pub fn sold(&self) -> T {
        T::saturate(self.view.sold)
    }

    pub fn __view(&self) -> &EscrowView {
        &self.view
    }

    pub fn __view_mut(&mut self) -> &mut EscrowView {
        &mut self.view
    }
}
