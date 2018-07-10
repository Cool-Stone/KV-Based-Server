#include "tcp.h"

int open_clientfd(char *name, int port) {
    int clientfd;
    hostent *hp;
    in_addr addr;
    sockaddr_in servaddr;

    if ((clientfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Creating client socket error" << std::endl;
        return -1;
    }

    if (inet_aton(name, &addr) != 0) {  // get host entry
        hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);
    } else {
        hp = gethostbyname(name);
    }

    bzero((char*)&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    if (hp) {
        bcopy((char*)hp->h_addr_list[0], (char*)&servaddr.sin_addr.s_addr, hp->h_length);
    }
    servaddr.sin_port = htons((unsigned short)port);
    if (connect(clientfd, (SA*)&servaddr, sizeof(servaddr)) < 0) {
        std::cerr << "Client connecting error" << std::endl;
        return -1;
    }
    return clientfd;
}


int open_listenfd(int port) {
    int listenfd, optval = 1;
    sockaddr_in servaddr;

    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cerr << "Server listening error" << std::endl;
        return -1;
    }

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(int)) < 0) {
        std::cerr << "Setsockopt error" << std::endl;
        return -1;
    }

    bzero((char*)&servaddr,sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons((unsigned short)port);
    if (bind(listenfd, (SA*)&servaddr, sizeof(servaddr)) < 0) {
        std::cerr << "Server binding error" << std::endl;
        return -1;
    }

    if (listen(listenfd, LISTENQ) < 0) {
        std::cerr << "Server listening error" << std::endl;
        return -1;
    }
    return listenfd;
}
