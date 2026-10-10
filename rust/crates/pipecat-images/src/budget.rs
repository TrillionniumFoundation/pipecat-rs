//! Allocation-free, depth-bounded admission accounting for serializable frames.
use serde::ser::{self, Serialize};
use std::fmt;

#[derive(Debug)]
pub(crate) struct BudgetError;
impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frame is unaccountable or exceeds its budget")
    }
}
impl std::error::Error for BudgetError {}
impl ser::Error for BudgetError {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self
    }
}
type Result<T> = std::result::Result<T, BudgetError>;
pub(crate) struct Budget {
    pub remaining: usize,
    depth: usize,
}
impl Budget {
    pub fn new(bytes: usize) -> Self {
        Self {
            remaining: bytes,
            depth: 0,
        }
    }
    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or(BudgetError)?;
        Ok(())
    }
    fn compound(&mut self, length: Option<usize>) -> Result<Compound<'_>> {
        if self.depth >= 64 {
            return Err(BudgetError);
        }
        self.charge(64usize.saturating_add(length.unwrap_or(0).saturating_mul(16)))?;
        self.depth += 1;
        Ok(Compound(self))
    }
}
pub(crate) struct Compound<'a>(&'a mut Budget);
impl Drop for Compound<'_> {
    fn drop(&mut self) {
        self.0.depth -= 1;
    }
}
macro_rules! scalar {
    ($($method:ident($ty:ty)),*)=>{$(fn $method(self,_:$ty)->Result<()>{self.charge(16)})*};
}
impl<'a> ser::Serializer for &'a mut Budget {
    type Ok = ();
    type Error = BudgetError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;
    scalar!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char)
    );
    fn serialize_str(self, value: &str) -> Result<()> {
        self.charge(32usize.saturating_add(value.len()))
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<()> {
        self.charge(32usize.saturating_add(value.len()))
    }
    fn serialize_none(self) -> Result<()> {
        self.charge(16)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        let compound = self.compound(Some(1))?;
        value.serialize(&mut *compound.0)
    }
    fn serialize_unit(self) -> Result<()> {
        self.charge(16)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        self.charge(16)
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        self.charge(32)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<()> {
        self.serialize_some(value)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<()> {
        self.serialize_some(value)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.compound(len)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.compound(Some(len))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.compound(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.compound(Some(len))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        self.compound(len.map(|n| n.saturating_mul(64)))
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.compound(Some(len))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.compound(Some(len))
    }
    fn collect_str<T: ?Sized + fmt::Display>(self, _: &T) -> Result<()> {
        Err(BudgetError)
    }
}
macro_rules! sequence {
    ($trait:ident,$method:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = ();
            type Error = BudgetError;
            fn $method<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
                value.serialize(&mut *self.0)
            }
            fn end(self) -> Result<()> {
                Ok(())
            }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);
impl ser::SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = BudgetError;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut *self.0)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut *self.0)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}
macro_rules! structure {
    ($trait:ident) => {
        impl ser::$trait for Compound<'_> {
            type Ok = ();
            type Error = BudgetError;
            fn serialize_field<T: ?Sized + Serialize>(
                &mut self,
                _: &'static str,
                value: &T,
            ) -> Result<()> {
                value.serialize(&mut *self.0)
            }
            fn end(self) -> Result<()> {
                Ok(())
            }
        }
    };
}
structure!(SerializeStruct);
structure!(SerializeStructVariant);
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deep_or_wide_metadata_fails_without_recursing_unboundedly() {
        let mut value = serde_json::Value::Null;
        for _ in 0..100 {
            value = serde_json::json!([value]);
        }
        assert!(value.serialize(&mut Budget::new(1_000_000)).is_err());
        assert!(serde_json::json!([null, null, null])
            .serialize(&mut Budget::new(16))
            .is_err());
    }
}
