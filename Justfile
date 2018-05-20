build:
    cargo +nightly build

build-release:
    cargo +nightly build --release

profile: build
    sudo dtrace -c './target/debug/synchrotron' -o out.stacks -n 'profile-997 /execname == "synchrotron"/ { @[ustack(100)] = count(); }'

profile-release: build-release
    sudo dtrace -c './target/release/synchrotron' -o out.stacks -n 'profile-997 /execname == "synchrotron"/ { @[ustack(100)] = count(); }'

profile-svg:
    test -d .flamegraph || git clone https://github.com/brendangregg/FlameGraph.git .flamegraph
    .flamegraph/stackcollapse.pl out.stacks | .flamegraph/flamegraph.pl > profile.svg
    open profile.svg
