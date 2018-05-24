use futures::stream::Stream;
use super::batch;

impl<T: ?Sized> StreamExt for T where T: Stream {}

pub trait StreamExt: Stream {
    fn batch(self, capacity: usize) -> batch::Batch<Self>
        where Self: Sized
    {
        batch::new(self, capacity)
    }
}
