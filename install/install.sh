#!/bin/sh
set -e

OS="$(uname -s 2>/dev/null)"
case "$OS" in
  Linux)
    cargo install ringdrop
    ;;
  Darwin)
    brew tap rikettsie/tap
    if brew list rdrop &>/dev/null; then
      brew upgrade rdrop
    else
      brew install rdrop
    fi
    ;;
  MINGW*|MSYS*|CYGWIN*)
    scoop bucket list | grep -q rikettsie || scoop bucket add rikettsie https://github.com/rikettsie/scoop-bucket
    if scoop list | grep -q rdrop; then
      scoop update rdrop
    else
      scoop install rdrop
    fi
    ;;
  *)
    echo "error: unsupported OS: $OS" >&2
    exit 1
    ;;
esac
