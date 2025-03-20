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

        use std::fmt;

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

pub mod outboard_with_progress {
    use std::{
        future::Future,
        io::{self, BufReader, Read},
    };

    use bao_tree::{
        blake3,
        io::{
            outboard::PreOrderOutboard,
            sync::{OutboardMut, WriteAt},
        },
        iter::BaoChunk,
        BaoTree, ChunkNum,
    };
    use blake3::guts::{hash_subtree, parent_cv};
    use smallvec::SmallVec;

    pub trait Progress {
        type Error;
        fn progress(
            &mut self,
            offset: ChunkNum,
        ) -> impl Future<Output = std::result::Result<(), Self::Error>>;
    }

    pub async fn init_outboard<R, W, P>(
        data: R,
        outboard: &mut PreOrderOutboard<W>,
        progress: &mut P,
    ) -> std::io::Result<std::result::Result<(), P::Error>>
    where
        W: WriteAt,
        R: Read,
        P: Progress,
    {
        // wrap the reader in a buffered reader, so we read in large chunks
        // this reduces the number of io ops
        let size = usize::try_from(outboard.tree.size()).unwrap_or(usize::MAX);
        let read_buf_size = size.min(1024 * 1024);
        let chunk_buf_size = size.min(outboard.tree.block_size().bytes());
        let reader = BufReader::with_capacity(read_buf_size, data);
        let mut buffer = SmallVec::<[u8; 128]>::from_elem(0u8, chunk_buf_size);
        let res = init_impl(outboard.tree, reader, outboard, &mut buffer, progress).await?;
        Ok(res)
    }

    async fn init_impl<W, P>(
        tree: BaoTree,
        mut data: impl Read,
        outboard: &mut PreOrderOutboard<W>,
        buffer: &mut [u8],
        progress: &mut P,
    ) -> io::Result<std::result::Result<(), P::Error>>
    where
        W: WriteAt,
        P: Progress,
    {
        // do not allocate for small trees
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        // debug_assert!(buffer.len() == tree.chunk_group_bytes());
        for item in tree.post_order_chunks_iter() {
            match item {
                BaoChunk::Parent { is_root, node, .. } => {
                    let right_hash = stack.pop().unwrap();
                    let left_hash = stack.pop().unwrap();
                    outboard.save(node, &(left_hash, right_hash))?;
                    let parent = parent_cv(&left_hash, &right_hash, is_root);
                    stack.push(parent);
                }
                BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                    ..
                } => {
                    if let Err(err) = progress.progress(start_chunk).await {
                        return Ok(Err(err));
                    }
                    let buf = &mut buffer[..size];
                    data.read_exact(buf)?;
                    let hash = hash_subtree(start_chunk.0, buf, is_root);
                    stack.push(hash);
                }
            }
        }
        debug_assert_eq!(stack.len(), 1);
        outboard.root = stack.pop().unwrap();
        Ok(Ok(()))
    }
}
