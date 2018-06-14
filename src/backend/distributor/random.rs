use super::{BackendDescriptor, Distributor};
use rand::{thread_rng, Rng};

/// Provides a randomized distribution of requests.
pub struct RandomDistributor {
    backends: Vec<BackendDescriptor>,
    backend_count: usize,
}

impl RandomDistributor {
    pub fn new() -> RandomDistributor {
        RandomDistributor {
            backends: vec![],
            backend_count: 0,
        }
    }
}

impl Distributor for RandomDistributor {
    fn seed(&mut self, backends: Vec<BackendDescriptor>) {
        self.backends = backends;
        self.backend_count = self.backends.len();
    }

    fn choose(&self, _point: u64) -> usize {
        let mut rng = thread_rng();
        rng.gen_range(0, self.backend_count)
    }
}
