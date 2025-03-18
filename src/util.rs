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
                    Ok(io::Error::other(value))
                }
            }

            deserializer.deserialize_str(IoErrorVisitor)
        }
    }

    // Module that handles io::Error serialization/deserialization
    pub mod chunk_ranges_serde {

        use std::{fmt, io};

        use bao_tree::ChunkRanges;
        use serde::{
            de::{self, Visitor},
            ser::SerializeSeq,
            Deserializer, Serializer,
        };
        use smallvec::SmallVec;

        pub fn serialize<S>(value: &ChunkRanges, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let value = value.boundaries();
            let mut seq = serializer.serialize_seq(Some(value.len()))?;
            for boundary in value {
                seq.serialize_element(&boundary.0)?;
            }
            seq.end()
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<ChunkRanges, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct ChunkRangesVisitor;

            impl<'de> Visitor<'de> for ChunkRangesVisitor {
                type Value = ChunkRanges;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a list of chunk boundaries")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: de::SeqAccess<'de>,
                {
                    let mut boundaries = SmallVec::new();
                    while let Some(boundary) = seq.next_element()? {
                        boundaries.push(boundary);
                    }
                    ChunkRanges::new(boundaries)
                        .ok_or_else(|| de::Error::custom("invalid chunk ranges"))
                }
            }

            deserializer.deserialize_seq(ChunkRangesVisitor)
        }
    }
}
