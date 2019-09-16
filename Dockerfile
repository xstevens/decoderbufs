FROM postgres:9.6

RUN apt-get update -qq \
  && apt-get install -y --force-yes git-core build-essential dh-autoreconf pkg-config wget zlib1g-dev \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/google/protobuf/releases/download/v3.1.0/protobuf-cpp-3.1.0.tar.gz \
  && tar -xzf protobuf-cpp-3.1.0.tar.gz && cd protobuf-3.1.0 \
  && ./autogen.sh && ./configure && make && make check && make install && ldconfig \
  && cd .. && rm -rf protobuf-*

RUN git clone --depth 1 --branch v1.2.1 https://github.com/protobuf-c/protobuf-c.git && cd protobuf-c \
  && ./autogen.sh && ./configure && make && make install && ldconfig \
  && cd .. && rm -rf protobuf-c

RUN apt-get update -qq \
  && apt-get install -y --force-yes postgresql-server-dev-10 \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /build
ADD ./ /build
WORKDIR /build

RUN make proto && make && make install
