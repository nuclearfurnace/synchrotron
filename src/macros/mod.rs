macro_rules! try_transport_ready {
    ($e:expr, $done:expr) => {
        match $e {
            Ok(Async::Ready(t)) => t,
            Ok(Async::NotReady) => {
                if $done {
                    return Ok(Async::Ready(None));
                }

                return Ok(Async::NotReady);
            },
            Err(e) => return Err(From::from(e)),
        }
    };
}
