build:
    cargo +nightly build

build-release:
    cargo +nightly build --release

profile: build-release
    sudo dtrace -c './target/release/synchrotron' -o out.stacks -n 'profile-997 /execname == "synchrotron"/ { @[ustack(100)] = count(); }'

profile-svg:
    git clone https://github.com/brendangregg/FlameGraph.git .flamegraph
    .flamegraph/stackcollapse.pl out.stacks | .flamegraph/flamegraph.pl > profile.svg
    open profile.svg
