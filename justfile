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

# Build binary (type: release, debug, profile)
build type="release": (_build type)

_build type:
    @just _build-{{type}}

_build-release: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -O3 -flto -DNDEBUG -o {{out}} {{sources}}
    strip {{out}}
    @echo "Built: {{out}} ($(stat -c%s {{out}}) bytes)"

_build-debug: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -g -O1 -fno-omit-frame-pointer -DDEBUG -o {{out}} {{sources}}
    @echo "Built: {{out}} (debug)"

_build-profile: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -g -O3 -fno-omit-frame-pointer -DNDEBUG -o {{out}} {{sources}}
    @echo "Built: {{out}} (profile - optimized with symbols)"

# Build with sanitizers (requires libc, for testing only)
sanitize: _bindir
    clang -std=c11 -Wall -Wextra -g -O1 \
        -fsanitize=address,undefined \
        -fno-omit-frame-pointer \
        -I{{srcdir}} \
        -o {{out}}-san {{srcdir}}/main.c
    @echo "Built: {{out}}-san (sanitizers enabled)"

# === Lint & Analysis ===

# Run clang-tidy on all source files (excluding tests)
lint:
    find {{srcdir}} \( -name '*.c' -o -name '*.h' \) ! -name '*.test.*' | xargs clang-tidy --config-file=.clang-tidy

# Run clang static analyzer
analyze: _bindir
    {{cc}} {{cflags}} --analyze -Xanalyzer -analyzer-output=text {{srcdir}}/main.c

# Check formatting (dry-run)
fmt-check:
    find {{srcdir}} -name '*.c' -o -name '*.h' | xargs clang-format --dry-run --Werror

# Format all source files
fmt:
    find {{srcdir}} -name '*.c' -o -name '*.h' | xargs clang-format -i

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
    for f in {{sources}}; do
        [ $first -eq 0 ] && echo "," >> compile_commands.json
        first=0
        echo '  {"directory": "'$(pwd)'", "file": "'$f'", "command": "{{cc}} {{cflags}} -c '$f'"}' >> compile_commands.json
    done
    # Test files
    for f in $(find src -name '*.test.c'); do
        echo "," >> compile_commands.json
        echo '  {"directory": "'$(pwd)'", "file": "'$f'", "command": "{{cc}} {{cflags}} -c '$f'"}' >> compile_commands.json
    done
    echo "]" >> compile_commands.json
    @echo "Generated compile_commands.json"

# === Testing ===

# Run the broker (type: release, debug, profile)
run type="release": (build type)
    {{out}}

# Run with strace to see syscalls
strace: (build "debug")
    strace -f -e trace=io_uring_setup,io_uring_enter,io_uring_register {{out}}

# Profile running broker with perf (attach to existing process)
# Usage: just profile && ./bin/broker, connect clients, then run `just perf 30`
perf seconds="30":
    #!/usr/bin/env bash
    BROKER_PID=$(pidof broker)
    if [ -z "$BROKER_PID" ]; then
        echo "No broker running. Start it first with: just run"
        exit 1
    fi
    echo "Attaching to broker PID $BROKER_PID for {{seconds}}s..."
    perf record -F 9999 -g -p $BROKER_PID -o {{bindir}}/perf.data -- sleep {{seconds}}
    echo ""
    echo "=== Top Functions ==="
    perf report -i {{bindir}}/perf.data --stdio --no-children -n --percent-limit 1 | head -60

# View perf results interactively
perf-report:
    perf report -i {{bindir}}/perf.data

# === Utilities ===

# Show binary info
info: build
    @echo "=== File ==="
    file {{out}}
    @echo "\n=== Size ==="
    size {{out}}
    @echo "\n=== Symbols ==="
    nm {{out}} | head -20
    @echo "\n=== Dependencies ==="
    ldd {{out}} || echo "Not a dynamic executable (good!)"

# Disassemble hot functions
disasm: build
    objdump -d {{out}} | less

# === Testing ===

# Run all tests (*.test.c files)
test: _bindir
    #!/usr/bin/env bash
    echo "Finding tests..."
    for f in $(find src -name '*.test.c'); do
        name=$(basename "$f" .test.c)
        echo -e "\n=== $name ==="
        {{cc}} {{cflags}} {{clang_flags}} -Wno-unused-function -o {{bindir}}/test-$name "$f" {{srcdir}}/mem/string.c || exit 1
        ./{{bindir}}/test-$name || exit 1
    done
    echo -e "\n=== All tests passed ==="

# Clean build artifacts
clean:
    rm -rf {{bindir}}
    rm -f compile_commands.json

# Create bin directory
_bindir:
    @mkdir -p {{bindir}}
