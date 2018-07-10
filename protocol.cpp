/**
 * File: protocol.cpp
 */

#include "protocol.h"

int Processor::read() {
	int total = 0, nread, read_bytes = 0;
	char buf[TEMPSIZE + 1] = { 0 };

	while ((nread = (int)recv(connfd, buf + total, (size_t)(TEMPSIZE - total), 0)) > 0) {
		total += nread;
		read_bytes += nread;
		if (total >= TEMPSIZE) {
			content.append(buf, total);
			memset(buf, 0, sizeof(buf));
			total = 0;
		}
	}
	if (nread < 0 && errno != EAGAIN) {
		perror("read");
		close(connfd);
		return -1;
	}
	if (nread == 0) {
		close(connfd);
		return 0;
	} else {
		content.append(buf, total);
		return read_bytes;
	}
}

int Processor::getRequestSize() {
	if (content.size() < 4) {
		return -1;
	} else {
		return *(int*)&content[0];
	}
}

bool Processor::ready() {
	int req_size = getRequestSize();
	if (req_size == -1) {
		return false;
	} else {
		return (content.size() >= sizeof(int) + req_size);
	}
}

string Processor::request() {
	int req_size = getRequestSize();
	string req = content.substr(4, req_size);
	content.erase(content.begin(), content.begin() + 4 + req_size);
	return req;
}

int Processor::response(const string& res){
	const char *ptr = res.c_str();
	int left = (int)res.length(), nwrite;

	send(connfd, (char*)&left, 4, 0);
	while (left > 0) {
		nwrite = (int)send(connfd, ptr, (size_t)left, 0);
		if (nwrite <= 0) {
			perror("write");
			close(connfd);
			return -1;
		}
		left -= nwrite;
		ptr += nwrite;
	}
	return 0;
}
