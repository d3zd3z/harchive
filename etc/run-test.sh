#! /bin/sh

set -e

# Invoked with
#  $1 - active directory
#  $2 - the name of the test.
#  pwd is the directory for the source.

active="$1"
tname="$2"
libdir=`pwd -P`

tdir=/tmp/aetest-$$
mkdir $tdir
trap "rm -rf $tdir" 0

mkdir $tdir/obj
mkdir $tdir/bin
ghc -package-conf "$libdir"/dist/package.conf \
	-i"$active/test" \
	-odir "$tdir/obj" \
	-hidir "$tdir/obj" \
	-stubdir "$tdir/obj" \
	-o "$tdir/bin/$tname" \
	--make "$active/test/${tname}.hs"
echo test: "$tdir/bin/$tname"
"$tdir/bin/$tname"

exit 0
