/**
 * File: epl.cpp
 * This file implements epl.h
 */

#include "epl.h"

int addfd(int epfd, int fd, int mode, int flag) {
    epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.data.fd = fd;
    ev.events = flag;
	return epoll_ctl(epfd, mode, fd, &ev);
}


int setnonblock(int fd) {
    int flag = fcntl(fd, F_GETFL);
    return fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}
