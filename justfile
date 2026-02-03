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
sources := srcdir / "main.c " + srcdir / "sys/io_uring.c"

# Base compiler flags
cflags := "-std=c11 -Wall -Wextra -Wpedantic -Wno-unused-function -ffreestanding -nostdlib -fno-stack-protector -fno-pic -fno-builtin -I" + srcdir

# Clang-specific flags
clang_flags := "--target=" + target + " -fuse-ld=" + ld + " -Wl,--no-pie -static"

# === Build Recipes ===

# Build release binary (default)
build: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -O3 -flto -DNDEBUG -o {{out}} {{sources}}
    strip {{out}}
    @echo "Built: {{out}} ($(stat -c%s {{out}}) bytes)"

# Build debug binary
debug: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -g -O1 -DDEBUG -o {{out}} {{sources}}
    @echo "Built: {{out}} (debug)"

# Build with sanitizers (requires libc, for testing only)
sanitize: _bindir
    clang -std=c11 -Wall -Wextra -g -O1 \
        -fsanitize=address,undefined \
        -fno-omit-frame-pointer \
        -I{{srcdir}} \
        -o {{out}}-san {{srcdir}}/main.c
    @echo "Built: {{out}}-san (sanitizers enabled)"

# === Lint & Analysis ===

# Run clang-tidy on all source files
lint:
    find {{srcdir}} -name '*.c' -o -name '*.h' | xargs clang-tidy --config-file=.clang-tidy

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

# Generate compile_commands.json for LSP
compile-commands: _bindir
    {{cc}} {{cflags}} {{clang_flags}} -MJ {{bindir}}/compile_commands.json -o /dev/null {{srcdir}}/main.c 2>/dev/null || true
    echo "[" > compile_commands.json
    cat {{bindir}}/compile_commands.json >> compile_commands.json
    echo "]" >> compile_commands.json
    @echo "Generated compile_commands.json"

# === Testing ===

# Run the broker (debug build)
run: debug
    {{out}}

# Run with strace to see syscalls
strace: debug
    strace -f -e trace=io_uring_setup,io_uring_enter,io_uring_register {{out}}

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

# Clean build artifacts
clean:
    rm -rf {{bindir}}
    rm -f compile_commands.json

# Create bin directory
_bindir:
    @mkdir -p {{bindir}}
