#ifndef EPL_H
#define EPL_H

/**
 * Epoll operations are wrapped here
 */

#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <signal.h>
#include <cstring>

int addfd(int epfd, int fd, int mode = EPOLL_CTL_ADD, int flag = EPOLLIN | EPOLLET);  // with a assigned mode

int setnonblock(int fd);    // nonblock I/O




#endif
