#!/bin/bash

# Gotta have Synchrotron available.
cargo build

pushd synchrotron-test
cargo test
popd
