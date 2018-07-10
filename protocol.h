/**
 * File: protocol.h
 */

#ifndef PROTOCOL_H_
#define PROTOCOL_H_

#include <iostream>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#define TEMPSIZE 1024

using std::string;

class Processor {	// catch the whole request
public:
	Processor() = default;
	Processor(int fd) : connfd(fd), content("") {}
	int read();
	bool ready();
	string request();		// get response from client
	int response(const string& res);	// send response to client
private:
	int connfd;
	string content;
	
	int getRequestSize();
};


#endif
