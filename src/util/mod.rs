use futures::stream::Stream;

mod batch;
pub use self::batch::Batch;

impl<T: ?Sized> StreamExt for T where T: Stream {}

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
        where Self: Sized
    {
        batch::new(self, capacity)
    }
}
