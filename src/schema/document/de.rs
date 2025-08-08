//! Document binary deserialization API
//!
//! The deserialization API is strongly inspired by serde's API but with
//! some tweaks, mostly around some of the types being concrete (errors)
//! and some more specific types being visited (Ips, datetime, etc...)
//!
//! The motivation behind this API is to provide a easy to implement and
//! efficient way of deserializing a potentially arbitrarily nested object.

use std::fmt::Display;
use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

use common::DateTime;

use crate::schema::{Facet, Field};
use crate::tokenizer::PreTokenizedString;

#[derive(Debug, thiserror::Error, Clone)]
/// An error which occurs while attempting to deserialize a given value
/// by using the provided value visitor.
pub enum DeserializeError {
    #[error("Unsupported Type: {0:?} cannot be deserialized from the given visitor")]
    /// The value cannot be deserialized from the given type.
    UnsupportedType(ValueType),
    #[error("Type Mismatch: Expected {expected:?} but found {actual:?}")]
    /// The value cannot be deserialized from the given type.
    TypeMismatch {
        /// The expected value type.
        expected: ValueType,
        /// The actual value type read.
        actual: ValueType,
    },
    #[error("The value could not be read: {0}")]
    /// The value was unable to be read due to the error.
    CorruptedValue(Arc<io::Error>),
    #[error("{0}")]
    /// A custom error message.
    Custom(String),
    #[error("Version {0}, Max version supported: {1}")]
    /// Unsupported version error.
    UnsupportedVersion(u32, u32),
}

impl DeserializeError {
    /// Creates a new custom deserialize error.
    pub fn custom(msg: impl Display) -> Self {
        Self::Custom(msg.to_string())
    }
}

impl From<io::Error> for DeserializeError {
    fn from(error: io::Error) -> Self {
        Self::CorruptedValue(Arc::new(error))
    }
}

/// Trait implemented by types that can be constructed from a serialized
/// document.
///
/// Implementations pull values from a [`DocumentDeserializer`] and
/// build the type from the extracted fields.
pub trait DocumentDeserialize: Sized {
    /// Attempts to deserialize Self from a given document deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where
        D: DocumentDeserializer<'de>;
}

/// A deserializer that can walk through each entry in the document.
pub trait DocumentDeserializer<'de> {
    /// A indicator as to how many values are in the document.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next field in the document.
    fn next_field<V: ValueDeserialize>(&mut self) -> Result<Option<(Field, V)>, DeserializeError>;
}

/// Trait implemented by types that can be deserialized from a single
/// value within a document.
///
/// This is used by [`DocumentDeserializer`] to materialize the value
/// associated with each field.
pub trait ValueDeserialize: Sized {
    /// Attempts to deserialize Self from a given value deserializer.
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, DeserializeError>
    where
        D: ValueDeserializer<'de>;
}

/// A value deserializer.
pub trait ValueDeserializer<'de> {
    /// Attempts to deserialize a null value from the deserializer.
    fn deserialize_null(self) -> Result<(), DeserializeError>;

    /// Attempts to deserialize a string value from the deserializer.
    fn deserialize_string(self) -> Result<String, DeserializeError>;

    /// Attempts to deserialize a u64 value from the deserializer.
    fn deserialize_u64(self) -> Result<u64, DeserializeError>;

    /// Attempts to deserialize an i64 value from the deserializer.
    fn deserialize_i64(self) -> Result<i64, DeserializeError>;

    /// Attempts to deserialize a f64 value from the deserializer.
    fn deserialize_f64(self) -> Result<f64, DeserializeError>;

    /// Attempts to deserialize a datetime value from the deserializer.
    fn deserialize_datetime(self) -> Result<DateTime, DeserializeError>;

    /// Attempts to deserialize a facet value from the deserializer.
    fn deserialize_facet(self) -> Result<Facet, DeserializeError>;

    /// Attempts to deserialize a bytes value from the deserializer.
    fn deserialize_bytes(self) -> Result<Vec<u8>, DeserializeError>;

    /// Attempts to deserialize an IP address value from the deserializer.
    fn deserialize_ip_address(self) -> Result<Ipv6Addr, DeserializeError>;

    /// Attempts to deserialize a bool value from the deserializer.
    fn deserialize_bool(self) -> Result<bool, DeserializeError>;

    /// Attempts to deserialize a pre-tokenized string value from the deserializer.
    fn deserialize_pre_tokenized_string(self) -> Result<PreTokenizedString, DeserializeError>;

    /// Attempts to deserialize the value using a given visitor.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DeserializeError>
    where
        V: ValueVisitor;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The type of the value attempting to be deserialized.
pub enum ValueType {
    /// A null value.
    Null,
    /// A string value.
    String,
    /// A u64 value.
    U64,
    /// A i64 value.
    I64,
    /// A f64 value.
    F64,
    /// A datetime value.
    DateTime,
    /// A facet value.
    Facet,
    /// A bytes value.
    Bytes,
    /// A IP address value.
    IpAddr,
    /// A boolean value.
    Bool,
    /// A pre-tokenized string value.
    PreTokStr,
    /// An array of value.
    Array,
    /// A dynamic object value.
    Object,
    /// A JSON object value. Deprecated.
    #[deprecated(note = "We keep this for backwards compatibility, use Object instead")]
    JSONObject,
}

/// A value visitor for deserializing a document value.
///
/// This is strongly inspired by serde but has a few extra types.
///
/// TODO: Improve docs
pub trait ValueVisitor {
    /// The value produced by the visitor.
    type Value;

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_null(&self) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Null))
    }

    #[inline]
    /// Called when the deserializer visits a string value.
    fn visit_string(&self, _val: String) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::String))
    }

    #[inline]
    /// Called when the deserializer visits a u64 value.
    fn visit_u64(&self, _val: u64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::U64))
    }

    #[inline]
    /// Called when the deserializer visits a i64 value.
    fn visit_i64(&self, _val: i64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::I64))
    }

    #[inline]
    /// Called when the deserializer visits a f64 value.
    fn visit_f64(&self, _val: f64) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::F64))
    }

    #[inline]
    /// Called when the deserializer visits a bool value.
    fn visit_bool(&self, _val: bool) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bool))
    }

    #[inline]
    /// Called when the deserializer visits a datetime value.
    fn visit_datetime(&self, _val: DateTime) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::DateTime))
    }

    #[inline]
    /// Called when the deserializer visits an IP address value.
    fn visit_ip_address(&self, _val: Ipv6Addr) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::IpAddr))
    }

    #[inline]
    /// Called when the deserializer visits a facet value.
    fn visit_facet(&self, _val: Facet) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Facet))
    }

    #[inline]
    /// Called when the deserializer visits a bytes value.
    fn visit_bytes(&self, _val: Vec<u8>) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::Bytes))
    }

    #[inline]
    /// Called when the deserializer visits a pre-tokenized string value.
    fn visit_pre_tokenized_string(
        &self,
        _val: PreTokenizedString,
    ) -> Result<Self::Value, DeserializeError> {
        Err(DeserializeError::UnsupportedType(ValueType::PreTokStr))
    }

    #[inline]
    /// Called when the deserializer visits an array.
    fn visit_array<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where
        A: ArrayAccess<'de>,
    {
        Err(DeserializeError::UnsupportedType(ValueType::Array))
    }

    #[inline]
    /// Called when the deserializer visits a object value.
    fn visit_object<'de, A>(&self, _access: A) -> Result<Self::Value, DeserializeError>
    where
        A: ObjectAccess<'de>,
    {
        Err(DeserializeError::UnsupportedType(ValueType::Object))
    }
}

/// Access to a sequence of values which can be deserialized.
pub trait ArrayAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next element in the sequence.
    fn next_element<V: ValueDeserialize>(&mut self) -> Result<Option<V>, DeserializeError>;
}

/// TODO: Improve docs
pub trait ObjectAccess<'de> {
    /// A indicator as to how many values are in the object.
    ///
    /// This can be used to pre-allocate entries but should not
    /// be depended on as a fixed size.
    fn size_hint(&self) -> usize;

    /// Attempts to deserialize the next key-value pair in the object.
    fn next_entry<V: ValueDeserialize>(&mut self) -> Result<Option<(String, V)>, DeserializeError>;
}
