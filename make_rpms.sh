#!/bin/bash

mkdir -p build
git archive --format=tar.gz --prefix=libs3-bull/ --output=build/libs3-bull.tar.gz HEAD
(cd build && rpmbuild -ta libs3-bull.tar.gz )

