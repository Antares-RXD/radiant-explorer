# Radiant Node (radiantd) - Built from Radiant-Core
# https://github.com/Radiant-Core/Radiant-Core
# Build with: docker build -f Dockerfile.node -t radiantd .

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libtool \
    autotools-dev \
    automake \
    pkg-config \
    libssl-dev \
    libevent-dev \
    bsdmainutils \
    libboost-all-dev \
    libzmq3-dev \
    libminiupnpc-dev \
    git \
    cmake \
    ninja-build \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone Radiant-Core from the official repository
RUN git clone --depth 1 --branch v2.1.2 https://github.com/Radiant-Core/Radiant-Core.git radiant
WORKDIR /build/radiant

# Build radiantd
RUN cmake -B build -G Ninja \
    -DBUILD_RADIANT_QT=OFF \
    -DBUILD_RADIANT_WALLET=OFF \
    -DENABLE_MAN=OFF \
    -DCMAKE_BUILD_TYPE=Release

RUN cmake --build build --target radiantd radiant-cli -j$(nproc)

# Install binaries
RUN install -m 755 build/src/radiantd /usr/local/bin/ && \
    install -m 755 build/src/radiant-cli /usr/local/bin/

# Cleanup build files to reduce image size
RUN rm -rf /build

# Create data directory
RUN mkdir -p /root/.radiant

EXPOSE 7332 7333

VOLUME ["/root/.radiant"]

ENTRYPOINT ["radiantd"]
CMD ["-server=1", "-printtoconsole", "-txindex=1"]
