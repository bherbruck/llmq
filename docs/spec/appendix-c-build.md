# Appendix C: Build System

## C.1 Overview

The broker builds as a single static binary with no external dependencies. The build uses **Clang/LLVM** (preferred) or GCC with freestanding mode (no libc).

### C.1.1 Why Clang

| Feature | Clang | GCC |
|---------|-------|-----|
| Error messages | Excellent | Good |
| Compile speed | Faster | Slower |
| Static analysis | Built-in | Requires extras |
| Cross-compilation | Clean | Requires toolchain |
| Runtime library | compiler-rt | libgcc |
| Binary size | Slightly smaller | Slightly larger |

For a zero-dependency freestanding build, Clang's `compiler-rt` builtins integrate cleanly.

### C.1.2 Requirements

For Clang build:
```bash
# Ubuntu/Debian
apt install clang lld llvm

# Arch
pacman -S clang lld llvm

# Fedora
dnf install clang lld llvm
```

For GCC build (fallback):
```bash
# Ubuntu/Debian
apt install gcc

# The build only needs gcc, not glibc-dev
```

## C.2 Directory Structure

```
broker/
├── build.sh                 # Build script
├── Makefile                 # Optional make-based build
├── docs/
│   └── spec/               # This specification
└── src/
    ├── main.c              # Entry point
    ├── sys/
    │   ├── types.h         # Primitive types
    │   ├── syscall.h       # Syscall wrappers
    │   └── io_uring.h      # io_uring structures
    ├── mem/
    │   ├── pool.h          # Buffer pool
    │   └── atomic.h        # Atomic operations
    ├── net/
    │   ├── socket.h        # Socket wrappers
    │   └── tcp.h           # TCP constants
    ├── mqtt/
    │   ├── proto.h         # MQTT constants
    │   ├── parse.h         # Packet parsing
    │   └── encode.h        # Packet encoding
    └── broker/
        ├── broker.h        # Main state
        ├── conn.h          # Connection management
        ├── trie.h          # Topic trie
        ├── fanout.h        # Message fan-out
        └── ref.h           # Message refcounting
```

## C.3 Build Script

```bash
#!/bin/bash
# build.sh

set -e

# Default to Clang, fall back to GCC
if command -v clang &> /dev/null; then
    CC="${CC:-clang}"
else
    CC="${CC:-gcc}"
fi

# Base flags (works for both Clang and GCC)
CFLAGS="-std=c11 -Wall -Wextra -Wpedantic"
CFLAGS="$CFLAGS -ffreestanding -nostdlib"
CFLAGS="$CFLAGS -fno-stack-protector"
CFLAGS="$CFLAGS -I./src"

# Compiler-specific flags
if [[ "$CC" == *"clang"* ]]; then
    CFLAGS="$CFLAGS -nostdinc"
    CFLAGS="$CFLAGS -fno-pic"
    CFLAGS="$CFLAGS --target=x86_64-unknown-linux-gnu"
    # Use compiler-rt builtins for atomics, memcpy, etc.
    LDFLAGS="-static -nostdlib -fuse-ld=lld"
    RTLIB="--rtlib=compiler-rt"
else
    CFLAGS="$CFLAGS -nostdinc"
    CFLAGS="$CFLAGS -fno-pie -no-pie"
    LDFLAGS="-static -nostdlib"
    RTLIB="-lgcc"
fi

# Optimization level
OPT="${OPT:--O3}"

# Debug build
if [ "$DEBUG" = "1" ]; then
    CFLAGS="$CFLAGS -g -DDEBUG -O0"
    OPT=""
fi

# Output directory
mkdir -p bin
OUT="bin/${OUT:-broker}"

echo "Building $OUT with $CC..."
$CC $CFLAGS $OPT $LDFLAGS $RTLIB -o $OUT src/main.c

# Strip if not debug
if [ "$DEBUG" != "1" ]; then
    strip $OUT
fi

SIZE=$(stat -f%z "$OUT" 2>/dev/null || stat -c%s "$OUT")
echo "Built: $OUT ($SIZE bytes)"
```

## C.4 Makefile

```makefile
# Makefile

CC      ?= clang
LD      ?= lld

# Detect compiler type
IS_CLANG := $(findstring clang,$(CC))

# Base flags
CFLAGS  := -std=c11 -Wall -Wextra -Wpedantic
CFLAGS  += -ffreestanding -nostdlib -nostdinc
CFLAGS  += -fno-stack-protector
CFLAGS  += -I./src

# Compiler-specific
ifdef IS_CLANG
    CFLAGS  += -fno-pic --target=x86_64-unknown-linux-gnu
    LDFLAGS := -static -nostdlib -fuse-ld=$(LD) --rtlib=compiler-rt
else
    CFLAGS  += -fno-pie -no-pie
    LDFLAGS := -static -nostdlib -lgcc
endif

SRC     := src/main.c
BINDIR  := bin
OUT     := $(BINDIR)/broker

# Release build (default)
release: CFLAGS += -O3 -DNDEBUG -flto
release: $(OUT)
	strip $(OUT)

# Debug build
debug: CFLAGS += -g -O0 -DDEBUG
debug: $(OUT)

$(OUT): $(SRC) | $(BINDIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

$(BINDIR):
	mkdir -p $(BINDIR)

clean:
	rm -rf $(BINDIR)

.PHONY: release debug clean
```

## C.5 Compiler Flags

### C.5.1 Common Flags

| Flag | Purpose |
|------|---------|
| `-std=c11` | Use C11 standard (required for `_Atomic`) |
| `-ffreestanding` | Freestanding environment (no hosted assumptions) |
| `-nostdlib` | Don't link standard library |
| `-nostdinc` | Don't search standard include paths |
| `-fno-stack-protector` | Disable stack canaries (no libc support) |
| `-static` | Static linking |

### C.5.2 Clang-Specific Flags

| Flag | Purpose |
|------|---------|
| `-fno-pic` | Disable position-independent code |
| `--target=x86_64-unknown-linux-gnu` | Explicit target triple |
| `-fuse-ld=lld` | Use LLVM linker (faster, better errors) |
| `--rtlib=compiler-rt` | Use compiler-rt for builtins (atomics, memcpy) |

### C.5.3 GCC-Specific Flags

| Flag | Purpose |
|------|---------|
| `-fno-pie -no-pie` | Disable position-independent executable |
| `-lgcc` | Link GCC support library (for atomics, etc.) |

## C.6 Optimization Flags

For maximum performance:

```bash
CFLAGS="$CFLAGS -O3"
CFLAGS="$CFLAGS -march=native"           # Target current CPU
CFLAGS="$CFLAGS -mtune=native"
CFLAGS="$CFLAGS -flto"                   # Link-time optimization
CFLAGS="$CFLAGS -fomit-frame-pointer"    # More registers
CFLAGS="$CFLAGS -funroll-loops"          # Loop unrolling
```

## C.7 Debug Build

```bash
CFLAGS="$CFLAGS -g"                      # Debug symbols
CFLAGS="$CFLAGS -O0"                     # No optimization
CFLAGS="$CFLAGS -DDEBUG"                 # Enable debug code
CFLAGS="$CFLAGS -fsanitize=undefined"    # UB sanitizer (optional)
```

Note: Address sanitizer requires libc, so cannot be used with freestanding build.

## C.8 Cross Compilation

Clang makes cross-compilation trivial - just change the target:

```bash
# ARM64 with Clang (no separate toolchain needed)
CC=clang
CFLAGS="$CFLAGS --target=aarch64-unknown-linux-gnu"
# Update SYS_* numbers in sys/syscall.h for ARM64
```

```bash
# ARM64 with GCC (requires cross-toolchain)
CC=aarch64-linux-gnu-gcc
# Update SYS_* numbers in sys/syscall.h for ARM64
```

Syscall numbers differ by architecture. Use conditional compilation:

```c
// sys/syscall.h
#if defined(__x86_64__)
    #define SYS_read        0
    #define SYS_write       1
    #define SYS_mmap        9
    // ...
#elif defined(__aarch64__)
    #define SYS_read        63
    #define SYS_write       64
    #define SYS_mmap        222
    // ...
#else
    #error "Unsupported architecture"
#endif
```

## C.9 Kernel Version Check

The build can verify minimum kernel version:

```c
// In main.c
#include <sys/utsname.h>  // Need to implement via syscall

int check_kernel_version(void) {
    struct utsname uts;
    if (uname(&uts) < 0) return -1;
    
    int major, minor;
    if (sscanf(uts.release, "%d.%d", &major, &minor) != 2) {
        return -1;
    }
    
    // Require 6.6+
    if (major < 6 || (major == 6 && minor < 6)) {
        write_str("Error: kernel 6.6+ required\n");
        return -1;
    }
    
    return 0;
}
```

## C.10 Testing Build

```bash
#!/bin/bash
# test.sh

# Build
./build.sh

# Run with test configuration
./broker --port 1883 --max-conns 1000 &
BROKER_PID=$!

# Wait for startup
sleep 1

# Run tests
mosquitto_sub -t "test/#" -C 1 &
mosquitto_pub -t "test/hello" -m "world"

# Cleanup
kill $BROKER_PID
```

## C.11 Packaging

### C.11.1 Static Binary

The output is a single static binary with no dependencies:

```bash
$ file broker
broker: ELF 64-bit LSB executable, x86-64, version 1 (SYSV), statically linked, stripped

$ ldd broker
    not a dynamic executable

$ ls -lh broker
-rwxr-xr-x 1 user user 84K Feb  2 12:00 broker
```

### C.11.2 Container Image

```dockerfile
# Dockerfile
FROM scratch
COPY broker /broker
EXPOSE 1883
ENTRYPOINT ["/broker"]
```

Build:

```bash
docker build -t broker .
docker run -p 1883:1883 broker
```

### C.11.3 systemd Service

```ini
# /etc/systemd/system/broker.service
[Unit]
Description=MQTT Broker
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/broker --port 1883
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

## C.12 Benchmarking Build

For benchmarking, add profiling support:

```bash
CFLAGS="$CFLAGS -pg"                     # gprof profiling
# or
CFLAGS="$CFLAGS -fno-omit-frame-pointer" # For perf
```

Run with perf:

```bash
perf record -g ./broker &
# ... run benchmark ...
perf report
```
