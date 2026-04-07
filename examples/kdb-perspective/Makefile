# Perspective KDB+ WebSocket Server — Build System
#
# Compiles the nanopb C bridge (nanopb_bridge.so) that enables the Q layer
# to encode/decode Perspective's binary protobuf protocol.
#
# Prerequisites:
#   - KDB+ 4.1 installed, QHOME env var set (e.g. /opt/kdb or ~/q)
#   - Python 3 with nanopb: pip install nanopb
#   - arrowkdb 3.x installed in QHOME (https://github.com/KxSystems/arrowkdb)
#
# Usage:
#   make           — build lib/nanopb_bridge.so (or .dylib on macOS)
#   make clean     — remove build artefacts (proto/ and lib/ directories)
#   make help      — show this message

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Absolute path to the perspective.proto file in the parent repository.
PROTO_SRC  := $(abspath ../../rust/perspective-client/perspective.proto)
PROTO_DIR  := $(dir $(PROTO_SRC))

# KDB+ installation root (must contain <arch>/k.h)
QHOME      ?= $(HOME)/q

# nanopb Python package location (auto-detected from pip install nanopb)
NANOPB_PKG := $(shell python3 -c \
    "import nanopb, os; print(os.path.dirname(os.path.abspath(nanopb.__file__)))")

ifeq ($(NANOPB_PKG),)
$(error "nanopb Python package not found. Run: pip install nanopb")
endif

# nanopb C runtime sources are bundled with the Python package
NANOPB_C   := $(NANOPB_PKG)/pb_common.c $(NANOPB_PKG)/pb_decode.c $(NANOPB_PKG)/pb_encode.c
NANOPB_INC := $(NANOPB_PKG)

# nanopb ships Google well-known protos for the generator
NANOPB_GOOGLE_PROTO := $(NANOPB_PKG)/generator/proto

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

OS   := $(shell uname -s)
ARCH := $(shell uname -m)

ifeq ($(OS),Darwin)
  LIB_EXT  := dylib
  CC_FLAGS := -dynamiclib -fPIC
  ifeq ($(ARCH),arm64)
    QARCH := m64
  else
    QARCH := m64
  endif
else
  LIB_EXT  := so
  CC_FLAGS := -shared -fPIC
  ifeq ($(ARCH),aarch64)
    QARCH := a64
  else
    QARCH := l64
  endif
endif

QINC     := $(QHOME)/$(QARCH)

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

PROTO_C  := proto/perspective.pb.c
PROTO_H  := proto/perspective.pb.h
LIB_OUT  := lib/nanopb_bridge.$(LIB_EXT)

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

.PHONY: all clean help

all: $(LIB_OUT)

help:
	@echo "Perspective KDB+ WebSocket Server — make targets:"
	@echo "  make         Build lib/nanopb_bridge.so"
	@echo "  make clean   Remove generated proto/ and lib/ directories"
	@echo ""
	@echo "Required env vars:"
	@echo "  QHOME        KDB+ installation root (default: ~/q)"

# Generate perspective.pb.c / perspective.pb.h from the repo proto file.
# The .options file constrains nanopb's static buffer sizes.
$(PROTO_C) $(PROTO_H): $(PROTO_SRC) perspective.nanopb.options
	@mkdir -p proto
	@echo "[nanopb] generating protobuf codec for perspective.proto ..."
	python3 -m nanopb_generator \
	    --output-dir=proto \
	    --proto-path=$(PROTO_DIR) \
	    --proto-path=$(NANOPB_GOOGLE_PROTO) \
	    --options-file=perspective.nanopb.options \
	    $(PROTO_SRC)
	@echo "[nanopb] generated: $(PROTO_C) $(PROTO_H)"

# Compile the shared library: nanopb runtime + generated codec + bridge
$(LIB_OUT): $(PROTO_C) $(PROTO_H) src/nanopb_bridge.c
	@mkdir -p lib
	@echo "[cc] compiling nanopb_bridge.$(LIB_EXT) ..."
	$(CC) $(CC_FLAGS) -O2 \
	    -I$(QINC) \
	    -I$(NANOPB_INC) \
	    -Iproto \
	    $(NANOPB_C) \
	    $(PROTO_C) \
	    src/nanopb_bridge.c \
	    -o $(LIB_OUT)
	@echo "[cc] built: $(LIB_OUT)"

clean:
	rm -rf proto lib
