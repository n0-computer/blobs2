pub mod channel;
pub mod io;
pub mod temp_tag;
pub mod serde {
    // Module that handles io::Error serialization/deserialization
    pub mod io_error_serde {
        use std::{fmt, io};

        use serde::{
            de::{self, Visitor},
            Deserializer, Serializer,
        };

        pub fn serialize<S>(error: &io::Error, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            // Serialize the error kind and message
            serializer.serialize_str(&format!("{:?}:{}", error.kind(), error))
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<io::Error, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct IoErrorVisitor;

            impl<'de> Visitor<'de> for IoErrorVisitor {
                type Value = io::Error;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("an io::Error string representation")
                }

                fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    // For simplicity, create a generic error
                    // In a real app, you might want to parse the kind from the string
                    Ok(io::Error::new(io::ErrorKind::Other, value))
                }
            }

            deserializer.deserialize_str(IoErrorVisitor)
        }
    }
}
