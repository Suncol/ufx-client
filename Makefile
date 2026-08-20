CXX ?= g++
CXXFLAGS ?= -std=c++11 -Wall -Wextra -Iinclude
LDFLAGS ?=

CORE_SRC = src/books.cpp src/coalescer.cpp
TEST_BIN = tests/test_books

.PHONY: test clean session demo

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): tests/test_books.cpp $(CORE_SRC) include/ufx/books.h \
	include/ufx/coalescer.h include/ufx/listener.h include/ufx/session.h \
	include/ufx/types.h src/biz_error.h src/listener_guard.h
	$(CXX) $(CXXFLAGS) -o $@ tests/test_books.cpp $(CORE_SRC)

# Linux x64 with official third-party T2SDK. Copy t2sdk.ini / subscriber.ini / license.dat first.
T2SDK_ROOT ?= ../../T2SDK_第三方版本/c++
T2SDK_INC = $(T2SDK_ROOT)/Include
T2SDK_LIB = $(T2SDK_ROOT)/linux.x64/lib/libt2sdk.so

session:
	$(CXX) $(CXXFLAGS) -Isrc -I$(T2SDK_INC) -c src/session.cpp -o src/session.o
	$(CXX) $(CXXFLAGS) -c src/books.cpp -o src/books.o
	$(CXX) $(CXXFLAGS) -c src/coalescer.cpp -o src/coalescer.o

demo: session
	$(CXX) $(CXXFLAGS) -Idemo -o ufx_demo demo/main.cpp src/session.o src/books.o src/coalescer.o $(T2SDK_LIB) -lpthread

clean:
	rm -f $(TEST_BIN) ufx_demo src/*.o
