#!/bin/sh

aclocal &&
    autoreconf --install &&
    glibtoolize &&
    autoconf &&
    automake --add-missing &&
    automake
