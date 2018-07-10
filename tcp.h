/**
 * File: tcp.h
 *
 * This file wraps functions of socket for server and client
 */
#ifndef TCP_H
#define TCP_H

#include <sys/socket.h> 
#include <sys/types.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <iostream>
#include <cstring>

#define LISTENQ 10

using SA = struct sockaddr;

int open_clientfd(char *name, int port);

int open_listenfd(int port);

#endif
