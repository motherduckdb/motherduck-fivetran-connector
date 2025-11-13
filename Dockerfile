##### Stage 1: Builder #####
FROM ubuntu:22.04 AS builder

ENV CC=clang
ENV CXX=clang++

ENV CCACHE_DIR=/root/.ccache
ENV CMAKE_C_COMPILER_LAUNCHER=ccache
ENV CMAKE_CXX_COMPILER_LAUNCHER=ccache

# zlib is required for OpenSSL
# lsb-release, software-properties-common and gnupg are required to install Clang
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    ccache \
    git \
    lsb-release \
    software-properties-common gnupg \
    wget \
    unzip \
    tar \
    ca-certificates \
    curl \
    zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Install CMake 3.28.6
# Available architectures are aarch64 and x86_64 - coincidentally the same as the return values of `uname -m`
RUN wget --output-document=install_cmake.sh --no-verbose https://github.com/Kitware/CMake/releases/download/v3.28.6/cmake-3.28.6-linux-$(uname -m).sh && \
    chmod +x install_cmake.sh && \
    ./install_cmake.sh --skip-license --prefix=/usr/local && \
    rm install_cmake.sh

# Install Clang 16
RUN wget --output-document=install_clang.sh --no-verbose https://apt.llvm.org/llvm.sh && \
    chmod +x install_clang.sh && \
    ./install_clang.sh 16 && \
    rm install_clang.sh

WORKDIR /app

# Copy only files required for build_dependencies to ensure the Docker image layer gets cached
COPY Makefile ./
COPY dependencies-patches/ ./dependencies-patches/
COPY scripts/ ./scripts/

RUN --mount=type=cache,target=/root/.ccache make build_dependencies

# Copy files required for build_connector
COPY CMakeLists.txt ./
COPY proto_helper.cmake ./
COPY src/ ./src/
COPY includes/ ./includes/

# Build argument to override git commit SHA
# Set this because we don't copy the .git directory
ARG GIT_COMMIT_SHA_OVERRIDE

RUN --mount=type=cache,target=/root/.ccache \
    make build_connector GIT_COMMIT_SHA_OVERRIDE=${GIT_COMMIT_SHA_OVERRIDE}

##### Stage 2: Final image #####
FROM ubuntu:22.04

WORKDIR /app

COPY --from=builder /app/build/Release/motherduck_destination ./build/Release/

# The roots.pem file needs to be copied until we update to gRPC 1.63 or later.
# In older version, it might not pick up system root certificates.
# See https://github.com/grpc/grpc/issues/35511.
COPY --from=builder /app/sources/grpc/etc/roots.pem /usr/share/grpc/roots.pem

EXPOSE 50052

CMD ["./build/Release/motherduck_destination", "--port", "50052"]
