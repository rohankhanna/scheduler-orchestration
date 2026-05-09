# Host resource facts (captured 2026-05-08)

Purpose
- Record host hardware + kernel/cgroup capabilities so we can choose safe defaults for:
  - Reserved CPUs for interactivity
  - Workload MemoryHigh/MemoryMax
  - Workload IOWeight / possible io.max caps
  - systemd/cgroups v2 enforcement approach

Raw command outputs

## free -h
```
              total        used        free      shared  buff/cache   available
Mem:           121Gi        18Gi        34Gi       252Mi        68Gi       103Gi
Swap:           15Gi          0B        15Gi
```

## swapon --show
```
NAME      TYPE SIZE USED PRIO
/swap.img file  16G   0B   -2
```

## nproc
```
20
```

## lscpu
```
Architecture:                            aarch64
CPU op-mode(s):                          64-bit
Byte Order:                              Little Endian
CPU(s):                                  20
On-line CPU(s) list:                     0-19
Vendor ID:                               ARM
Model name:                              Cortex-X925
Model:                                   1
Thread(s) per core:                      1
Core(s) per socket:                      10
Socket(s):                               1
Thread(s) per core:                      1
NUMA node(s):                            1
NUMA node0 CPU(s):                       0-19
...
(system reports both Cortex-X925 and Cortex-A725 groups)
```

## cgroups v2 controllers
```
cpuset cpu io memory hugetlb pids rdma misc dmem
```

## systemd --version
```
systemd 255 (255.4-1ubuntu8.15)
```

## lsblk
```
NAME        TYPE   SIZE MODEL                      ROTA MOUNTPOINT
nvme0n1     disk   3.7T SAMSUNG MZALC4T0HBL1-00B07    0
├─nvme0n1p1 part   512M                               0 /boot/efi
└─nvme0n1p2 part   3.6T                               0 /
```

Notes
- cgroups v2 is available (systemd default-hierarchy=unified).
- Single NUMA node.
- Memory is large (121 GiB), swap exists (16 GiB). We should still cap workloads to prevent swap storms.
