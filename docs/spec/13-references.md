# Section 13: References

## 13.1 Normative References

These documents are essential for implementing this specification:

### 13.1.1 MQTT Protocol

**[MQTT-3.1.1]** OASIS Standard. "MQTT Version 3.1.1". 29 October 2014.
https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html

**[MQTT-5.0]** OASIS Standard. "MQTT Version 5.0". 07 March 2019.
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

### 13.1.2 Linux Kernel

**[io_uring]** Linux Kernel Documentation. "io_uring".
https://kernel.dk/io_uring.pdf

**[io_uring-man]** Linux Manual Pages. "io_uring_setup(2), io_uring_enter(2), io_uring_register(2)".
https://man7.org/linux/man-pages/man2/io_uring_setup.2.html

### 13.1.3 RFC Standards

**[RFC2119]** Bradner, S. "Key words for use in RFCs to Indicate Requirement Levels". BCP 14, RFC 2119, March 1997.
https://www.rfc-editor.org/rfc/rfc2119

**[RFC3629]** Yergeau, F. "UTF-8, a transformation format of ISO 10646". STD 63, RFC 3629, November 2003.
https://www.rfc-editor.org/rfc/rfc3629

## 13.2 Informative References

These documents provide additional context and background:

### 13.2.1 io_uring Resources

**[liburing]** Axboe, J. "liburing - io_uring library".
https://github.com/axboe/liburing

**[io_uring-intro]** Axboe, J. "Efficient IO with io_uring". 2019.
https://kernel.dk/io_uring.pdf

**[io_uring-lwn]** Corbet, J. "The rapid growth of io_uring". LWN.net, 2020.
https://lwn.net/Articles/810414/

**[io_uring-zc]** Linux Kernel Mailing List. "io_uring zero copy send".
https://lore.kernel.org/io-uring/

### 13.2.2 Performance References

**[c10k]** Kegel, D. "The C10K problem". 2006.
http://www.kegel.com/c10k.html

**[c10m]** Waldo, J. "The C10M problem". 2013.
https://c10m.robertgraham.com/

**[seastar]** ScyllaDB. "Seastar - High performance server-side application framework".
https://seastar.io/

### 13.2.3 MQTT Implementations

**[mosquitto]** Eclipse Foundation. "Eclipse Mosquitto".
https://mosquitto.org/

**[emqx]** EMQ Technologies. "EMQX - The most scalable MQTT broker".
https://www.emqx.io/

**[nanomq]** EMQ Technologies. "NanoMQ - Ultra-lightweight MQTT broker".
https://nanomq.io/

### 13.2.4 Systems Programming

**[linux-syscall]** Linux Kernel Documentation. "Linux System Call Table".
https://github.com/torvalds/linux/blob/master/arch/x86/entry/syscalls/syscall_64.tbl

**[atomic-weapons]** Williams, A. "C++ Concurrency in Action". Manning, 2019.

**[memory-ordering]** Preshing, J. "Memory Ordering at Compile Time".
https://preshing.com/20120625/memory-ordering-at-compile-time/

## 13.3 Test Suites

**[mqtt-conformance]** Eclipse Paho. "MQTT Conformance Test".
https://github.com/eclipse/paho.mqtt.testing

**[mqtt-test-tools]** HiveMQ. "MQTT-CLI - A command line interface for MQTT".
https://hivemq.github.io/mqtt-cli/

## 13.4 Glossary

| Term | Definition |
|------|------------|
| CQE | Completion Queue Entry (io_uring) |
| DMA | Direct Memory Access |
| fd | File Descriptor |
| FNV | Fowler-Noll-Vo (hash function) |
| IPC | Inter-Process Communication |
| mmap | Memory-mapped file/region |
| MQTT | Message Queuing Telemetry Transport |
| NIC | Network Interface Card |
| NUMA | Non-Uniform Memory Access |
| QoS | Quality of Service |
| SQE | Submission Queue Entry (io_uring) |
| TLS | Transport Layer Security |
| UTF-8 | Unicode Transformation Format - 8-bit |
| VBI | Variable Byte Integer (MQTT encoding) |
| ZC | Zero-Copy |
