#!/bin/bash

gcc -Wall -O0 -g -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic -fPIC -o rdma_rw.o rdma_rw.c -lrdmacm -libverbs
