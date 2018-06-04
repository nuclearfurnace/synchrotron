use futures::stream::Stream;

mod batch;
pub use self::batch::Batch;

impl<T: ?Sized> StreamExt for T
where
    T: Stream,
{
}

/// An extension trait for `Stream`s that provides necessary combinators specific to synchrotron.
pub trait StreamExt: Stream {
    /// Converts this stream into a batched stream.
    ///
    /// Items from the underlying stream will be batched, up to `capacity`, and returned as a
    /// `Vec<T>`.
    ///
    /// Unlike standard combinators that perform grouping, batches are collected opportunistically.
    /// The combinator will take up to `capacity` items from the underlying stream in a single
    /// `poll`, but will return the currently batched items -- if any have been batched since the
    /// last batch was emitted -- either when capacity is reached or the underlying stream reports
    /// that it is no longer ready.
    ///
    /// If the underlying stream signals that it is not ready, and no items have been batched, then
    /// the stream will emit nothing.
    fn batch(self, capacity: usize) -> batch::Batch<Self>
    where
        Self: Sized,
    {
        batch::new(self, capacity)
    }
}

/// Flattens and orders a list of messages.
///
/// Results for pipelined requests come back in the form of Vec<(u64, T)>, where each tuple
/// represents a response, and the order it needs to be sent back to the client in relation to
/// other responses.
///
/// Since we may send requests to multiple servers, we end up with Vec<Vec<(u64, T)>>, and so here
/// we're flattening out those nested vectors, and then ensuring a global order of the messages.
pub fn flatten_ordered_messages<T>(msgs: Vec<Vec<(u64, T)>>) -> Vec<T>
where
    T: Sized,
{
    let mut items = msgs.into_iter().flatten().collect::<Vec<_>>();
    items.sort_by(|(a, _), (b, _)| a.cmp(b));
    items.into_iter().map(|(_, item)| item).collect()
}
