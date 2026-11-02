# Pinned tag tracks upstream manylinux refreshes; bump when rebuilding wheels.
FROM quay.io/pypa/manylinux_2_28_x86_64:2026.10.09-1

RUN dnf install -y clang-devel git \
    && dnf clean all

# The release workflow builds free-threaded wheels from these interpreters; fail
# the image build (or install them) rather than lose a release leg on a refresh.
RUN manylinux-interpreters ensure cp314-cp314t cp315-cp315t
