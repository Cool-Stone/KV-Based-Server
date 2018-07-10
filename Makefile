.PHONY : all

all : server client press utest

utest : kv.cpp kv.h utest.cpp
	g++ -g -std=c++11 kv.cpp utest.cpp -o utest -lpthread

CC=g++ -std=c++11

kv.o : kv.h kv.cpp
	${CC} -c kv.cpp

tcp.o : tcp.h tcp.cpp
	${CC} -c tcp.cpp

epl.o : epl.h epl.cpp
	${CC} -c epl.cpp

protocol.o : protocol.h protocol.cpp
	${CC} -c protocol.cpp

server.o : server.cpp tcp.h
	${CC} -c server.cpp

client.o : client.cpp tcp.h
	${CC} -c client.cpp

press.o : press.cpp tcp.h
	${CC} -c press.cpp

server : server.o tcp.o epl.o kv.o protocol.o
	${CC} -g tcp.o epl.o server.o kv.o protocol.o -o server -lpthread 

client : client.o tcp.o epl.o kv.o protocol.o
	${CC} -g tcp.o epl.o client.o kv.o protocol.o -o client -lpthread

press : press.o tcp.o epl.o kv.o protocol.o
	${CC} -g tcp.o epl.o press.o kv.o protocol.o -o press -lpthread 

clean :
	rm server.o client.o press.o tcp.o epl.o kv.o protocol.o server client press utest
