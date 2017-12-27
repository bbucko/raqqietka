#!/bin/bash
set -ev

export PATH=$HOME/.cargo/bin:$PATH
[ -x "$(command -v cargo-coveralls)" ] || cargo install --force cargo-travis
[ -x "$(command -v cargo-clippy)" ] || cargo install --force clippy