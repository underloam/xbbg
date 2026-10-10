# Pinned tag tracks upstream manylinux refreshes; bump when rebuilding wheels.
FROM quay.io/pypa/manylinux_2_28_x86_64:2026.10.09-1

RUN dnf install -y clang-devel git \
    && dnf clean all
