#!/bin/bash

gcc -Wall -O2 -g -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic -fPIC -o net_rw.o net_rw.c
