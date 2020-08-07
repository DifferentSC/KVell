CC=clang  #If you use GCC, add -fno-strict-aliasing to the CFLAGS because the Google BTree does weird stuff
#CFLAGS=-Wall -O0 -ggdb3
CFLAGS=-O2 -ggdb3 -Wall -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/ -I/usr/lib/jvm/java-1.8.0-openjdk-amd64/include/linux

CXX=clang++
CXXFLAGS= ${CFLAGS} -std=c++11 -fPIC

LDLIBS=-lm -lpthread -lstdc++

INDEXES_OBJ=indexes/rbtree.o indexes/rax.o indexes/art.o indexes/btree.o
MAIN_OBJ=main.o slab.o freelist.o ioengine.o pagecache.o stats.o random.o slabworker.o workload-common.o workload-ycsb.o workload-production.o utils.o in-memory-index-rbtree.o in-memory-index-rax.o in-memory-index-art.o in-memory-index-btree.o ${INDEXES_OBJ} kvell_jni.o
MICROBENCH_OBJ=microbench.o random.o stats.o utils.o ${INDEXES_OBJ}
BENCH_OBJ=benchcomponents.o pagecache.o random.o $(INDEXES_OBJ)
SHARED_LIBRARY_TARGET=kvell_jni.so
# LDFLAGS=-shared

.PHONY: all clean

all: makefile.dep main microbench benchcomponents sharedlibrary

makefile.dep: *.[Cch] indexes/*.[ch] indexes/*.cc
	for i in *.[Cc]; do ${CC} -MM "$${i}" ${CFLAGS}; done > $@
	for i in indexes/*.c; do ${CC} -MM "$${i}" -MT $${i%.c}.o ${CFLAGS}; done >> $@
	for i in indexes/*.cc; do ${CXX} -MM "$${i}" -MT $${i%.cc}.o ${CXXFLAGS}; done >> $@
	#find ./ -type f \( -iname \*.c -o -iname \*.cc \) | parallel clang -MM "{}" -MT "{.}".o > makefile.dep #If you find that the lines above take too long...

-include makefile.dep

main: $(MAIN_OBJ)

microbench: $(MICROBENCH_OBJ)

benchcomponents: $(BENCH_OBJ)

sharedlibrary: $(SHARED_LIBRARY_TARGET)

$(SHARED_LIBRARY_TARGET): $(INDEXES_OBJ) $(MAIN_OBJ)
	$(CC) -fPIC $(INDEXES_OBJ) $(MAIN_OBJ) -o $(SHARED_LIBRARY_TARGET) -shared


clean:
	rm -f *.o *.so indexes/*.o main microbench benchcomponents sharedlibrary

