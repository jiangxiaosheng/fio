#!/bin/bash

gcc -Wall -O0 -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic -fPIC -o rdma_rw.o rdma_rw.c -lrdmacm -libverbs

# rdma-ioring must be compiled with O3 turned on, or it may suffer RNR retry error
gcc -Wall -O3 -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic -fPIC -o rdma_io_uring.o rdma_io_uring.c -lrdmacm -libverbs -luring