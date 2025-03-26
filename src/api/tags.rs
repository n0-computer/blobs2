use std::{
    io,
    ops::{Bound, RangeBounds},
};

use n0_future::{Stream, StreamExt};
use quic_rpc::{channel::oneshot, ServiceRequest};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use tracing::trace;

use super::{ApiSender, Scope, Tags};
use crate::{store::util::Tag, BlobFormat, Hash, HashAndFormat};

/// Information about a tag.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagInfo {
    /// Name of the tag
    pub name: Tag,
    /// Format of the data
    pub format: BlobFormat,
    /// Hash of the data
    pub hash: Hash,
}

impl TagInfo {
    /// Create a new tag info.
    pub fn new(name: impl AsRef<[u8]>, value: impl Into<HashAndFormat>) -> Self {
        let name = name.as_ref();
        let value = value.into();
        Self {
            name: Tag::from(name),
            hash: value.hash,
            format: value.format,
        }
    }

    /// Get the hash and format of the tag.
    pub fn hash_and_format(&self) -> HashAndFormat {
        HashAndFormat {
            hash: self.hash,
            format: self.format,
        }
    }
}

/// Options for a list operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTags {
    /// List tags to hash seqs
    pub hash_seq: bool,
    /// List tags to raw blobs
    pub raw: bool,
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl ListTags {
    /// List a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self {
            from,
            to,
            raw: true,
            hash_seq: true,
        }
    }

    /// List tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            raw: true,
            hash_seq: true,
            from: Some(from),
            to,
        }
    }

    /// List a single tag
    pub fn single(name: &[u8]) -> Self {
        let from = Tag::from(name);
        Self {
            to: Some(from.successor()),
            from: Some(from),
            raw: true,
            hash_seq: true,
        }
    }

    /// List all tags
    pub fn all() -> Self {
        Self {
            raw: true,
            hash_seq: true,
            from: None,
            to: None,
        }
    }

    /// List raw tags
    pub fn raw() -> Self {
        Self {
            raw: true,
            hash_seq: false,
            from: None,
            to: None,
        }
    }

    /// List hash seq tags
    pub fn hash_seq() -> Self {
        Self {
            raw: false,
            hash_seq: true,
            from: None,
            to: None,
        }
    }
}

/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTempTagRequest {
    pub scope: Scope,
    pub value: HashAndFormat,
}

/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTempTagsRequest;

/// Rename a tag atomically
#[derive(Debug, Serialize, Deserialize)]
pub struct Rename {
    /// Old tag name
    pub from: Tag,
    /// New tag name
    pub to: Tag,
}

/// Options for a delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delete {
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl Delete {
    /// Delete a single tag
    pub fn single(name: &[u8]) -> Self {
        let name = Tag::from(name);
        Self {
            to: Some(name.successor()),
            from: Some(name),
        }
    }

    /// Delete a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self { from, to }
    }

    /// Delete tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            from: Some(from),
            to,
        }
    }
}

fn tags_from_range<R, E>(range: R) -> (Option<Tag>, Option<Tag>)
where
    R: RangeBounds<E>,
    E: AsRef<[u8]>,
{
    let from = match range.start_bound() {
        Bound::Included(start) => Some(Tag::from(start.as_ref())),
        Bound::Excluded(start) => Some(Tag::from(start.as_ref()).successor()),
        Bound::Unbounded => None,
    };
    let to = match range.end_bound() {
        Bound::Included(end) => Some(Tag::from(end.as_ref()).successor()),
        Bound::Excluded(end) => Some(Tag::from(end.as_ref())),
        Bound::Unbounded => None,
    };
    (from, to)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetTag {
    pub name: Tag,
    pub value: HashAndFormat,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTagRequest {
    pub value: HashAndFormat,
}

impl Tags {
    pub(crate) fn ref_from_sender(sender: &ApiSender) -> &Self {
        Self::ref_cast(sender)
    }

    pub async fn list_temp_tags(&self) -> super::RequestResult<impl Stream<Item = HashAndFormat>> {
        let options = ListTempTagsRequest;
        trace!("{:?}", options);
        let rx = match self.sender.request().await? {
            ServiceRequest::Remote(r) => {
                let (_, rx) = r.write(options).await?;
                rx.into()
            }
            ServiceRequest::Local(l) => {
                let (tx, rx) = oneshot::channel();
                l.send((options, tx)).await?;
                rx
            }
        };
        let res = rx.await?;
        Ok(futures_lite::stream::iter(res))
    }

    /// List all tags with options.
    ///
    /// This is the most flexible way to list tags. All the other list methods are just convenience
    /// methods that call this one with the appropriate options.
    pub async fn list_with_opts(
        &self,
        options: ListTags,
    ) -> super::RequestResult<impl Stream<Item = super::Result<TagInfo>>> {
        trace!("{:?}", options);
        let rx = match self.sender.request().await? {
            ServiceRequest::Remote(r) => {
                let (_, rx) = r.write(options).await?;
                rx.into()
            }
            ServiceRequest::Local(l) => {
                let (tx, rx) = oneshot::channel();
                l.send((options, tx)).await?;
                rx
            }
        };
        let res = rx.await?;
        Ok(futures_lite::stream::iter(res))
    }

    /// Get the value of a single tag
    pub async fn get(
        &self,
        name: impl AsRef<[u8]>,
    ) -> super::FallibleRequestResult<Option<TagInfo>> {
        let mut stream = self.list_with_opts(ListTags::single(name.as_ref())).await?;
        Ok(stream.next().await.transpose()?)
    }

    pub async fn set_with_opts(&self, options: SetTag) -> super::FallibleRequestResult<()> {
        trace!("{:?}", options);
        let rx = match self.sender.request().await? {
            ServiceRequest::Local(c) => {
                let (tx, rx) = oneshot::channel();
                c.send((options, tx)).await?;
                rx
            }
            ServiceRequest::Remote(r) => {
                let (_, rx) = r.write(options).await?;
                rx.into()
            }
        };
        rx.await??;
        Ok(())
    }

    pub async fn set(
        &self,
        name: impl AsRef<[u8]>,
        value: impl Into<HashAndFormat>,
    ) -> super::FallibleRequestResult<()> {
        self.set_with_opts(SetTag {
            name: Tag::from(name.as_ref()),
            value: value.into(),
        })
        .await
    }

    /// List a range of tags
    pub async fn list_range<R, E>(
        &self,
        range: R,
    ) -> super::RequestResult<impl Stream<Item = super::Result<TagInfo>>>
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        self.list_with_opts(ListTags::range(range)).await
    }

    /// Lists all tags with the given prefix.
    pub async fn list_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> super::RequestResult<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListTags::prefix(prefix.as_ref())).await
    }

    /// Lists all tags.
    pub async fn list(&self) -> super::RequestResult<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListTags::all()).await
    }

    /// Lists all tags with a hash_seq format.
    pub async fn list_hash_seq(
        &self,
    ) -> super::RequestResult<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListTags::hash_seq()).await
    }

    /// Deletes a tag.
    pub async fn delete_with_opts(&self, options: Delete) -> super::FallibleRequestResult<()> {
        trace!("{:?}", options);
        let rx = match self.sender.request().await? {
            ServiceRequest::Local(c) => {
                let (tx, rx) = quic_rpc::channel::oneshot::channel();
                c.send((options, tx)).await?;
                rx
            }
            ServiceRequest::Remote(r) => {
                let (_, rx) = r.write(options).await?;
                rx.into()
            }
        };
        rx.await??;
        Ok(())
    }

    /// Deletes a tag.
    pub async fn delete(&self, name: impl AsRef<[u8]>) -> super::FallibleRequestResult<()> {
        self.delete_with_opts(Delete::single(name.as_ref())).await
    }

    /// Deletes a range of tags.
    pub async fn delete_range<R, E>(&self, range: R) -> super::FallibleRequestResult<()>
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        self.delete_with_opts(Delete::range(range)).await
    }

    /// Delete all tags with the given prefix.
    pub async fn delete_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> super::FallibleRequestResult<()> {
        self.delete_with_opts(Delete::prefix(prefix.as_ref())).await
    }

    /// Delete all tags. Use with care. After this, all data will be garbage collected.
    pub async fn delete_all(&self) -> super::FallibleRequestResult<()> {
        self.delete_with_opts(Delete {
            from: None,
            to: None,
        })
        .await
    }

    /// Rename a tag atomically
    ///
    /// If the tag does not exist, this will return an error.
    pub async fn rename_with_opts(&self, options: Rename) -> super::FallibleRequestResult<()> {
        trace!("{:?}", options);
        let rx = match self.sender.request().await? {
            ServiceRequest::Local(c) => {
                let (tx, rx) = quic_rpc::channel::oneshot::channel();
                c.send((options, tx)).await?;
                rx
            }
            ServiceRequest::Remote(r) => {
                let (_, rx) = r.write(options).await?;
                rx.into()
            }
        };
        rx.await??;
        Ok(())
    }

    /// Rename a tag atomically
    ///
    /// If the tag does not exist, this will return an error.
    pub async fn rename(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> super::FallibleRequestResult<()> {
        self.rename_with_opts(Rename {
            from: Tag::from(from.as_ref()),
            to: Tag::from(to.as_ref()),
        })
        .await
    }
}
