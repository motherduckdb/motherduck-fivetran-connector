
MAKEFILE:=$(firstword $(MAKEFILE_LIST))
ROOT_DIR:=$(shell dirname $(realpath ${MAKEFILE}))
CORES=$(shell grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
MD_FIVETRAN_DEPENDENCIES_DIR ?= $(strip ${ROOT_DIR})/install
MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR = $(strip ${ROOT_DIR})/sources
MD_FIVETRAN_DEPENDENCIES_BUILD_DIR = $(strip ${ROOT_DIR})/build

SOURCE_DIR="${ROOT_DIR}"
BUILD_DIR="${ROOT_DIR}/build"
INSTALL_DIR="${ROOT_DIR}/install"

GRPC_VERSION=v1.59.1
OPENSSL_VERSION=3.1.3
ARROW_VERSION=15.0.2
DUCKDB_VERSION=v1.1.3
CATCH2_VERSION=v3.5.1

info:
	echo "root dir = " ${ROOT_DIR}
	echo "dependencies install dir = " ${MD_FIVETRAN_DEPENDENCIES_DIR}
	echo "dependencies source dir = " ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	echo "dependencies build dir = " ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}

check_dependencies:
	if [ ! -d '${MD_FIVETRAN_DEPENDENCIES_DIR}/arrow' ]; then \
  		echo "ERROR: Please run 'make build_dependencies' first."; \
  		exit 1; \
  	fi

build_connector: check_dependencies get_fivetran_protos
	echo "dependencies: ${MD_FIVETRAN_DEPENDENCIES_DIR}"
	cmake -S ${SOURCE_DIR} -B ${BUILD_DIR}/Release \
    		-DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR}/Release \
    		-DMD_FIVETRAN_DEPENDENCIES_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}
	cmake --build ${BUILD_DIR}/Release -j${CORES} --config Release
	cmake --install ${BUILD_DIR}/Release --config Release

build_connector_debug: check_dependencies get_fivetran_protos
	cmake -S ${SOURCE_DIR} -B ${BUILD_DIR}/Debug \
    		-DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR}/Debug \
    		-DMD_FIVETRAN_DEPENDENCIES_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}
	cmake --build ${BUILD_DIR}/Debug -j${CORES} --config Debug
	cmake --install ${BUILD_DIR}/Debug --config Debug

build_openssl_native:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}
	cd ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR} && \
	  wget -q -O openssl-${OPENSSL_VERSION}.tar.gz https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz && \
	  tar -xf openssl-${OPENSSL_VERSION}.tar.gz && \
	  mv openssl-${OPENSSL_VERSION} openssl && \
	  cd openssl && \
	  ./config --prefix=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl --openssldir=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl --libdir=lib no-shared zlib-dynamic no-tests && \
	  make -j${CORES} && \
	  make install_sw

build_grpc:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
	  git clone --recursive --depth=1 --branch ${GRPC_VERSION} https://github.com/grpc/grpc.git
	OPENSSL_ROOT_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl cmake -S ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc -B${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc \
	  ${OSX_BUILD_UNIVERSAL_FLAG} \
	  -DgRPC_SSL_PROVIDER=package \
	  -DCMAKE_CXX_STANDARD=14 \
	  -DCMAKE_INSTALL_PREFIX=${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc
	cd ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc && make -j${CORES} && cmake --install .


build_arrow:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/arrow
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
	git clone --branch apache-arrow-${ARROW_VERSION} https://github.com/apache/arrow.git && \
	cmake -S ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/arrow/cpp -B${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow \
	-DCMAKE_INSTALL_PREFIX=${MD_FIVETRAN_DEPENDENCIES_DIR}/arrow -DARROW_CSV=ON -DARROW_WITH_ZSTD=ON
	cd ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow && make -j${CORES} && cmake --install .

get_duckdb:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
		wget -q -O libduckdb-src.zip https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-src.zip && \
		unzip -o -d ../libduckdb-src libduckdb-src.zip


get_fivetran_protos:
	mkdir -p protos
	curl -o protos/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/destination_sdk.proto
	curl -o protos/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/production/common.proto

build_dependencies: get_duckdb build_openssl_native build_grpc build_arrow build_test_dependencies

# Repo-wide C++ formatting
# For local formatter use:
#   $ make format CLANG_FORMATTER=clang-format
CLANG_FORMATTER=docker run --rm -v `pwd`:/mnt/code -w /mnt/code --platform=linux/x86_64 --init ghcr.io/jidicula/clang-format:16
format_params:
	$(CLANG_FORMATTER) $(FORMAT_OPTS) `find includes -name '*.hpp'`
	$(CLANG_FORMATTER) $(FORMAT_OPTS) `find src -name '*.cpp'`
	$(CLANG_FORMATTER) $(FORMAT_OPTS) `find test -name '*.cpp'`

format:
	FORMAT_OPTS='-i --verbose' make format_params

check_format:
	FORMAT_OPTS='--dry-run --Werror' make format_params


build_test_dependencies:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/Catch2
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
		git clone https://github.com/catchorg/Catch2.git --branch ${CATCH2_VERSION} && \
		cd Catch2 && \
		cmake -S ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/Catch2 -B${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/Catch2 \
			-DCMAKE_INSTALL_PREFIX=${MD_FIVETRAN_DEPENDENCIES_DIR}/Catch2 -DBUILD_TESTING=OFF
	cd ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/Catch2 && make -j${CORES} && cmake --install .

