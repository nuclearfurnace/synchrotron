mod random;
pub use self::random::RandomDistributor;

/// A placeholder for backends.  This lets us avoid holding references to the actual backends.
pub struct BackendDescriptor;

impl BackendDescriptor {
    pub fn new() -> BackendDescriptor {
        BackendDescriptor {}
    }
}

/// Distributes items amongst a set of backends.
///
/// After being seeded with a set of backends, one of them can be chosen by mapping a point amongst
/// them.  This could be by modulo division (point % backend count), libketama, or others.
pub trait Distributor {
    fn seed(&mut self, backends: Vec<BackendDescriptor>);

    /// Chooses a backend based on the given point.
    ///
    /// The return value is the list position, zero-indexed, based on the list of backends given to
    /// `seed`.
    fn choose(&self, point: u64) -> usize;
}
