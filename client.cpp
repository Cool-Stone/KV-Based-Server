/**
 * File: client.cpp
 */
#include <unistd.h>
#include <sys/file.h>
#include <string>
#include <sys/epoll.h>
#include <pthread.h>
#include <signal.h>
#include <unordered_map>
#include "tcp.h"
#include "epl.h"
#include "protocol.h"
#include "kv.h"


#define MAXSIZE 1024
#define EVENTSIZE 100

using std::unordered_map;

void initialize(int &clientfd, int &epfd, int argc, char* argv[]);



int main(int argc, char* argv[]) {
    int clientfd, epfd;
    epoll_event events[EVENTSIZE];
	Processor *proc = nullptr;

    initialize(clientfd, epfd, argc, argv);
	proc = new Processor(clientfd);

	int acc = 0;
	Debugger debugger;
	for (int i = 0; i < 100; ++i) {
		proc->response("set " + debugger.genString() + " " + debugger.genString());
	}

    // main loop
    while (true) {
        int nfds = epoll_wait(epfd, events, EVENTSIZE, -1);
        for (int i = 0; i < nfds; ++i) {
            if (events[i].events & EPOLLIN) {
                int sockfd = events[i].data.fd;
                if (sockfd == 0) {      // if user inputs
                    char buf[MAXSIZE + 1] = { 0 };
					// request structure: size | content
					string input;
					getline(std::cin, input);
					proc->response(input);
                } else if (sockfd == clientfd) {    // receive data from server
					int read_bytes;
					if ((read_bytes = proc->read()) <= 0) {
						delete proc;
						std::cout << "Server closed" << std::endl;
						exit(0);
					}
					while (proc->ready()) {
						std::cout << proc->request() << std::endl;
						if (++acc >= 100) {
							delete proc;
							exit(0);
						}
					}
                }
            }
        }
    }
    /* End */

    close(clientfd);    
    close(epfd);

    return 0;
}



void initialize(int &clientfd, int &epfd, int argc, char* argv[]) {
    if (argc != 3) {
        std::cout << "Usage: " << argv[0] << " <address> <port>" << std::endl;
        exit(1);
    }

    if ((clientfd = open_clientfd(argv[1], atoi(argv[2]))) < 0) {
        std::cerr << "open client fd failed" << std::endl;
        exit(1);
    }

    // signals 
    signal(SIGPIPE, SIG_IGN);
    
    epfd = epoll_create1(0);
    setnonblock(clientfd);
    setnonblock(0);
    addfd(epfd, clientfd, EPOLL_CTL_ADD, EPOLLIN | EPOLLET);
    addfd(epfd, 0, EPOLL_CTL_ADD, EPOLLIN | EPOLLET);     // user's input 
}


