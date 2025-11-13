#!/usr/bin/env bash

case $(uname -s) in
    Linux*)     OS="linux";;
    Darwin*)    OS="osx";;
    *)          echo "Unsupported operating system: $(uname -s)" >&2; exit 1;;
esac

case $(uname -m) in
  x86_64)     ARCH="amd64";;
  aarch64)    ARCH="arm64";;
  arm64)      ARCH="arm64";;
  *)          echo "Unsupported architecture: $(uname -m)" >&2; exit 1;;
esac

echo "${OS}-${ARCH}"