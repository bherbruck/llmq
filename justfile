# llmq - io_uring MQTT broker
# Build with: just build
# Lint with:  just lint

# Default recipe
default: build

# === Configuration ===

cc := "clang"
ld := "lld"
target := "x86_64-unknown-linux-gnu"
srcdir := "src"
bindir := "bin"
out := bindir / "broker"

# Source files

sources := srcdir / "main.c " + srcdir / "sys/io_uring.c " + srcdir / "mem/string.c"

# Base compiler flags

cflags := "-std=c11 -Wall -Wextra -Wpedantic -Wno-unused-function -ffreestanding -nostdlib -fno-stack-protector -fno-pic -fno-builtin -I" + srcdir

# Clang-specific flags

clang_flags := "--target=" + target + " -fuse-ld=" + ld + " -Wl,--no-pie -static"

# === Build Recipes ===

# Build binary (type: release, debug, profile, profile-inline)
build type="release": (_build type)

_build type:
    @just _build-{{ type }}

_build-release: _bindir
    {{ cc }} {{ cflags }} {{ clang_flags }} -O3 -flto -DNDEBUG -o {{ out }} {{ sources }}
    strip {{ out }}
    @echo "Built: {{ out }} ($(stat -c%s {{ out }}) bytes)"

_build-debug: _bindir
    {{ cc }} {{ cflags }} {{ clang_flags }} -g -O1 -fno-omit-frame-pointer -DDEBUG -o {{ out }} {{ sources }}
    @echo "Built: {{ out }} (debug)"

_build-profile: _bindir
    {{ cc }} {{ cflags }} {{ clang_flags }} -g -O2 -fno-inline-functions -fno-omit-frame-pointer -DNDEBUG -DPROFILE_BUILD -o {{ out }} {{ sources }}
    @echo "Built: {{ out }} (profile - O2, functions visible, with symbols)"

_build-profile-inline: _bindir
    {{ cc }} {{ cflags }} {{ clang_flags }} -g -O3 -flto -fno-omit-frame-pointer -DNDEBUG -o {{ out }} {{ sources }}
    @echo "Built: {{ out }} (profile-inline - O3+LTO with symbols, use 'perf annotate' for source)"

# Build with sanitizers (requires libc, for testing only)
sanitize: _bindir
    clang -std=c11 -Wall -Wextra -g -O1 \
        -fsanitize=address,undefined \
        -fno-omit-frame-pointer \
        -I{{ srcdir }} \
        -o {{ out }}-san {{ srcdir }}/main.c
    @echo "Built: {{ out }}-san (sanitizers enabled)"

# === Lint & Analysis ===

# Run clang-tidy on all source files (excluding tests)
lint:
    find {{ srcdir }} \( -name '*.c' -o -name '*.h' \) ! -name '*.test.*' | xargs clang-tidy --config-file=.clang-tidy

# Run clang static analyzer
analyze: _bindir
    {{ cc }} {{ cflags }} --analyze -Xanalyzer -analyzer-output=text {{ srcdir }}/main.c

# Check formatting (dry-run)
fmt-check:
    find {{ srcdir }} -name '*.c' -o -name '*.h' | xargs clang-format --dry-run --Werror

# Format all source files
fmt:
    find {{ srcdir }} -name '*.c' -o -name '*.h' | xargs clang-format -i

# Full check: lint + analyze + build
check: lint analyze build
    @echo "All checks passed"

# === Development ===

# Watch for changes and rebuild
watch:
    watchexec -e c,h just build

# Generate compile_commands.json for LSP (includes test files)
compile-commands: _bindir
    #!/usr/bin/env bash
    echo "[" > compile_commands.json
    first=1
    # Main sources
    for f in {{ sources }}; do
        [ $first -eq 0 ] && echo "," >> compile_commands.json
        first=0
        echo '  {"directory": "'$(pwd)'", "file": "'$f'", "command": "{{ cc }} {{ cflags }} -c '$f'"}' >> compile_commands.json
    done
    # Test files
    for f in $(find src -name '*.test.c'); do
        echo "," >> compile_commands.json
        echo '  {"directory": "'$(pwd)'", "file": "'$f'", "command": "{{ cc }} {{ cflags }} -c '$f'"}' >> compile_commands.json
    done
    echo "]" >> compile_commands.json
    @echo "Generated compile_commands.json"

# === Running ===

# Stop any running broker instance
stop:
    -pkill -x broker
    -pkill -9 -x broker

# Run the broker
# Examples: just run, just run debug
run type="release": stop (build type)
    {{ out }}

# Run with strace to see syscalls
strace: stop (build "debug")
    strace -f -e trace=io_uring_setup,io_uring_enter,io_uring_register {{ out }}

# === Profiling ===

# Basic CPU profile - where is time spent?
# Use 'just run profile' for function-level breakdown, 'just run profile-inline' for realistic perf
perf seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running. Start with:"
        echo "  just run profile         # function names visible"
        echo "  just run profile-inline  # realistic perf, use perf-annotate"
        exit 1
    fi
    echo "Attaching to broker PID $BROKER_PID for {{ seconds }}s..."
    perf record -F 7000 -g -p $BROKER_PID -o {{ bindir }}/perf.data -- sleep {{ seconds }}
    echo ""
    echo "=== Top Functions (flat) ==="
    perf report -i {{ bindir }}/perf.data --stdio --no-children -n --percent-limit 0.5 | head -80
    echo ""
    echo "=== Call Graph (who calls what) ==="
    perf report -i {{ bindir }}/perf.data --stdio --children -n --percent-limit 2 | head -80

# Show hottest source lines
perf-lines:
    perf report -i {{ bindir }}/perf.data --stdio --sort=srcline --no-children -n --percent-limit 0.5 | head -60

# Annotate hot function with source + asm (use less to navigate)
perf-annotate func="broker_main":
    perf annotate -i {{ bindir }}/perf.data --stdio --source -s {{ func }} 2>&1 | less

# List available symbols for annotation
perf-symbols:
    perf report -i {{ bindir }}/perf.data --stdio --no-children -n | grep -E '^\s+[0-9]' | head -30

# Cache misses - are you thrashing memory?
perf-cache seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    echo "Recording cache events for {{ seconds }}s..."
    perf stat -e cache-references,cache-misses,L1-dcache-load-misses,LLC-load-misses \
        -p $BROKER_PID -- sleep {{ seconds }}

# Branch mispredictions - are your if/else unpredictable?
perf-branch seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf stat -e branches,branch-misses,instructions,cycles \
        -p $BROKER_PID -- sleep {{ seconds }}

# Full stat dump - overview of everything
perf-stat seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf stat -d -d -d -p $BROKER_PID -- sleep {{ seconds }}

# Record with LBR (Last Branch Record) for better call graphs

# Requires Intel CPU with LBR support
perf-lbr seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf record -F 7000 --call-graph lbr -p $BROKER_PID -o {{ bindir }}/perf.data -- sleep {{ seconds }}
    perf report -i {{ bindir }}/perf.data --stdio --no-children -n --percent-limit 1 | head -60

# Flamegraph - visual call stack (needs flamegraph tools)
perf-flame seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf record -F 7000 -g -p $BROKER_PID -o {{ bindir }}/perf.data -- sleep {{ seconds }}
    perf script -i {{ bindir }}/perf.data | stackcollapse-perf.pl | flamegraph.pl > {{ bindir }}/flame.svg
    echo "Flamegraph: {{ bindir }}/flame.svg"

# Interactive TUI
perf-report:
    perf report -i {{ bindir }}/perf.data

# What syscalls are being made? (should be almost none with io_uring)
perf-syscalls seconds="10":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    strace -c -p $BROKER_PID -f 2>&1 &
    STRACE_PID=$!
    sleep {{ seconds }}
    kill -INT $STRACE_PID 2>/dev/null
    wait $STRACE_PID 2>/dev/null

# io_uring specific - see submission/completion rates
perf-uring seconds="10":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf trace -e 'io_uring:*' -p $BROKER_PID -- sleep {{ seconds }} 2>&1 | tail -50

# Save current profile as baseline (before optimization)
perf-baseline seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf record -F 7000 -g -p $BROKER_PID -o {{ bindir }}/perf-baseline.data -- sleep {{ seconds }}
    echo "Saved baseline: {{ bindir }}/perf-baseline.data"

# Compare two runs - useful for A/B testing optimizations
perf-diff baseline="perf-baseline.data" seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running"
        exit 1
    fi
    perf record -F 7000 -g -p $BROKER_PID -o {{ bindir }}/perf.data -- sleep {{ seconds }}
    perf diff {{ bindir }}/{{ baseline }} {{ bindir }}/perf.data | head -40

# === Utilities ===

# Show binary info
info: build
    @echo "=== File ==="
    file {{ out }}
    @echo "\n=== Size ==="
    size {{ out }}
    @echo "\n=== Symbols ==="
    nm {{ out }} | head -20
    @echo "\n=== Dependencies ==="
    ldd {{ out }} || echo "Not a dynamic executable (good!)"

# Disassemble hot functions
disasm: build
    objdump -d {{ out }} | less

# === Testing ===

# Run all tests (*.test.c files)
test: _bindir
    #!/usr/bin/env bash
    echo "Finding tests..."
    for f in $(find src -name '*.test.c'); do
        name=$(basename "$f" .test.c)
        echo -e "\n=== $name ==="
        {{ cc }} {{ cflags }} {{ clang_flags }} -Wno-unused-function -o {{ bindir }}/test-$name "$f" {{ srcdir }}/mem/string.c || exit 1
        ./{{ bindir }}/test-$name || exit 1
    done
    echo -e "\n=== All tests passed ==="

# Clean build artifacts
clean:
    rm -rf {{ bindir }}
    rm -f compile_commands.json

# Create bin directory
_bindir:
    @mkdir -p {{ bindir }}
