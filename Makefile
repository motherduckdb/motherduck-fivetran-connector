
MAKEFILE:=$(firstword $(MAKEFILE_LIST))
ROOT_DIR:=$(shell dirname $(realpath ${MAKEFILE}))
CORES=$(shell grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
MD_FIVETRAN_DEPENDENCIES_DIR ?= $(strip ${ROOT_DIR})/install
MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR = $(strip ${ROOT_DIR})/sources
MD_FIVETRAN_DEPENDENCIES_BUILD_DIR = $(strip ${ROOT_DIR})/build

SOURCE_DIR=${ROOT_DIR}
BUILD_DIR=${ROOT_DIR}/build
INSTALL_DIR=${ROOT_DIR}/install

GRPC_VERSION=v1.61.1
OPENSSL_VERSION=3.1.3
ARROW_VERSION=15.0.2
DUCKDB_VERSION=v1.3.2
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
    		-DDEPENDENCIES_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}
	cmake --build ${BUILD_DIR}/Release -j${CORES} --config Release

build_connector_debug: check_dependencies get_fivetran_protos
	cmake -S ${SOURCE_DIR} -B ${BUILD_DIR}/Debug \
    		-DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR}/Debug \
    		-DDEPENDENCIES_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}
	cmake --build ${BUILD_DIR}/Debug -j${CORES} --config Debug

build_openssl_native:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	wget -q -O ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl-${OPENSSL_VERSION}.tar.gz https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz
	tar --extract --gunzip --file ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl-${OPENSSL_VERSION}.tar.gz --directory ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl-${OPENSSL_VERSION}.tar.gz
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl
	mv ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl-${OPENSSL_VERSION} ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/openssl && \
	  ./config --prefix=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl --openssldir=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl --libdir=lib no-shared zlib-dynamic no-tests && \
	  make -j${CORES} && \
	  make install_sw

# Uses -DCMAKE_POLICY_VERSION_MINIMUM=3.5 because third_party/cares has minimum version set to 3.1.0
build_grpc:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc ${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc

	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
	  git clone --branch ${GRPC_VERSION} --depth=1 --recurse-submodules --shallow-submodules https://github.com/grpc/grpc.git
	# We need at least zlib 1.3.1 for the build to work on newer Macs (same issue as https://github.com/bulletphysics/bullet3/issues/4607)
	# Undo the following once grpc has been bumped to a version that has zlib 1.3.1 or newer
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc/third_party/zlib && \
	  git fetch --unshallow origin && \
	  git checkout f1f503da85d52e56aae11557b4d79a42bcaa2b86
	# abseil is broken too (see https://github.com/abseil/abseil-cpp/issues/1241), patch until bumped to fix
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc/third_party/abseil-cpp && \
	  git apply ${ROOT_DIR}/dependencies-patches/abseil.patch

	OPENSSL_ROOT_DIR=${MD_FIVETRAN_DEPENDENCIES_DIR}/openssl cmake -S ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/grpc -B ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc \
	  -DgRPC_BUILD_TESTS=OFF \
	  -DgRPC_INSTALL=ON \
	  -DgRPC_SSL_PROVIDER=package \
	  -DCMAKE_CXX_STANDARD=14 \
	  -DCMAKE_INSTALL_PREFIX=${MD_FIVETRAN_DEPENDENCIES_DIR}/grpc \
	  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
	  -DCMAKE_CXX_FLAGS="-Wno-missing-template-arg-list-after-template-kw"

	cd ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/grpc && make -j${CORES} && cmake --install .


build_arrow:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	rm -rf ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/arrow ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow ${MD_FIVETRAN_DEPENDENCIES_DIR}/arrow
	git clone --branch apache-arrow-${ARROW_VERSION} --depth 1 https://github.com/apache/arrow.git ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/arrow
	cmake -S ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}/arrow/cpp -B${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow \
	  -DARROW_BUILD_STATIC=ON -DARROW_CSV=ON -DARROW_WITH_ZSTD=ON \
	  -DCMAKE_INSTALL_PREFIX=${MD_FIVETRAN_DEPENDENCIES_DIR}/arrow \
	  -DCMAKE_POLICY_VERSION_MINIMUM=3.5

	CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake --build ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow
	cmake --install ${MD_FIVETRAN_DEPENDENCIES_BUILD_DIR}/arrow

get_duckdb:
	mkdir -p ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR}
	cd ${MD_FIVETRAN_DEPENDENCIES_SOURCE_DIR} && \
		wget -q -O libduckdb-src.zip https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/libduckdb-src.zip && \
		unzip -o -d ../libduckdb-src libduckdb-src.zip


get_fivetran_protos:
	mkdir -p protos
	curl -o protos/destination_sdk.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/destination_sdk.proto
	curl -o protos/common.proto https://raw.githubusercontent.com/fivetran/fivetran_sdk/v2/common.proto

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

