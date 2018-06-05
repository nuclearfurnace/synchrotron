# synchrotron

[![Build Status](https://travis-ci.org/nuclearfurnace/synchrotron.svg?branch=master)](https://travis-ci.org/nuclearfurnace/synchrotron)

Synchrotron is a caching layer load balancer, in the spirit of [Twemproxy](https://github.com/twitter/twemproxy) and [mcrouter](https://github.com/facebook/mcrouter).

# Why another one?

There's a few things here:
- I wanted to write a real piece of software in Rust, not just toy programs!
- Twemproxy is basically deprecated
- mcrouter is advanced but only supports memcached

Essentially, this project aims to be a mix of Twemproxy and mcrouter: memcached _and_ Redis support with advanced features like traffic shadowing, pool warm up, and online reconfiguration... while being written in Rust: a systems programming language whose community, IMO, is second to none.

# What's done?

Here is a non-exhaustive checklist of what's done and what is a serious target:

- [x] Redis support
- [ ] memcached support
- [x] Redis pipelining support
- [x] basic connection multiplexing (M client conns over N server conns; configurable server connection limit)
- [ ] advanced connection multiplexing (server backoff after failure, timeout on backend operations, etc)
- [x] basic routing strategies (single pool, traffic shadowing)
- [ ] advanced routing strategies (warm up [cold before warm],  prefix routing, fallthrough, majority, fastest response)
- [x] distribution (modulo vs ketama) and hashing (md5 vs sha vs fnv1a) support
- [ ] online reconfiguration
- [ ] metrics collection
- [ ] TLS support
