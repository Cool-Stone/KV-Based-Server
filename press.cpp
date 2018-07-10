/**
 * File press.cpp
 *
 * This file test the performance of server 
 */


#include <unistd.h>
#include <sys/file.h>
#include <string>
#include <sys/epoll.h>
#include <pthread.h>
#include <signal.h>
#include <cstring>
#include <sys/time.h>
#include <getopt.h>
#include <unordered_map>
#include "tcp.h"
#include "epl.h"
#include "kv.h"
#include "protocol.h"

using std::unordered_map;

unordered_map<int, timeval> start_times, end_times;
int op = 0;

#define DEFAULT_USER_NUM 10000        // concurrency number
#define DEFAULT_REQ_NUM 100          // request number 
#define BUFSIZE 2048                // max message length 
#define THREADNUM 9


struct Arg {
    int epfd, nfds;
    int *cur;
    unordered_map<int, Processor*>* table;
	unordered_map<int, int>* record;
    pthread_mutex_t *_lock, *g_lock;
    epoll_event* events;
};

/** request **/
void* request(void* arg) {
    Arg *para = (Arg*)arg;
	Debugger debugger;

    while (*para->cur < para->nfds) {
        if (pthread_mutex_trylock(para->_lock) == 0) {
            if (*para->cur < para->nfds) {
                epoll_event ev = para->events[(*para->cur)++];
                pthread_mutex_unlock(para->_lock);
                int sockfd = ev.data.fd;
               
				pthread_mutex_lock(para->g_lock);
                if (sockfd > 0 && (ev.events & EPOLLIN)) {  // read 
					int read_bytes;
					Processor* proc;

					if (para->table->count(sockfd)) {
						proc = (*para->table)[sockfd];
					} else {
						pthread_mutex_unlock(para->g_lock);
						continue;
					}
					if ((read_bytes = proc->read()) <= 0) {
						exit(1);
					}
					while (proc->ready()) {
						proc->request();
						if (--((*para->record)[sockfd]) == 0) {
							delete proc;
							para->table->erase(sockfd);
							para->record->erase(sockfd);
							gettimeofday(&end_times[sockfd], nullptr);
							break;
						}
					}
                } else if (sockfd > 0 && (ev.events & EPOLLOUT)) {  // write 
					gettimeofday(&start_times[sockfd], nullptr);
                	for (int i = 0; i < DEFAULT_REQ_NUM; ++i) {
						string k = debugger.genString(), v = debugger.genString();
						switch (op) {
							case 0:
								(*para->table)[sockfd]->response("set " + k + " " + v);
								break;
							case 1:
								(*para->table)[sockfd]->response("get " + k);
								break;
							case 2:
								(*para->table)[sockfd]->response("del " + k);
								break;
							default:
								std::cout << op << std::endl;
								break;
						}
					}
					addfd(para->epfd, sockfd, EPOLL_CTL_MOD, EPOLLIN | EPOLLET);
				}
				pthread_mutex_unlock(para->g_lock);
            } else {
                pthread_mutex_unlock(para->_lock);
            }
        }
    }

    return nullptr;
}


int main(int argc, char* argv[]) {
    int nfds, epfd;
    long double total_time = 0.0;
    unordered_map<int, Processor*> table;
	unordered_map<int, int> record;
    pthread_t pids[THREADNUM];
    pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
    epoll_event events[DEFAULT_USER_NUM];


    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " <address> <port> <op>" << std::endl;
        exit(1);
    }

	if (!strcmp(argv[3], "set")) {
		op = 0;
	} else if (!strcmp(argv[3], "get")) {
		op = 1;
	} else if (!strcmp(argv[3], "del")) {
		op = 2;
	} else {
		std::cerr << "invalid op" << std::endl;
		exit(1);
	}

    epfd = epoll_create1(0);    // create epoll 
    for (int i = 0; i < DEFAULT_USER_NUM; ++i) {
		int fd;
        if ((fd = open_clientfd(argv[1], atoi(argv[2]))) < 0) {
            std::cerr << "Connection to server failed" << std::endl;
            exit(1);
        }
        setnonblock(fd);
		addfd(epfd, fd, EPOLL_CTL_ADD, EPOLLOUT | EPOLLET);
		table[fd] = new Processor(fd);
		record[fd] = DEFAULT_REQ_NUM;
		start_times[fd] = timeval();
		end_times[fd] = timeval();
    }

    // signals 
    signal(SIGPIPE, SIG_IGN);
    

    // main loop
	struct timeval start, end;
	gettimeofday(&start, nullptr);
    while (true) {
        nfds = epoll_wait(epfd, events, DEFAULT_USER_NUM, -1);
        int id = 0;
        pthread_mutex_t _lock = PTHREAD_MUTEX_INITIALIZER;
        Arg arg = { epfd, nfds, &id, &table, &record, &_lock, &g_lock, events };

        for (int i = 0; i < THREADNUM; ++i) {
            pthread_create(&pids[i], nullptr, request, (void*)&arg);
        }

        for (int i = 0; i < THREADNUM; ++i) {
            pthread_join(pids[i], nullptr);
        }

        if (table.empty()) break;
    }
	gettimeofday(&end, nullptr);

	for (auto& p : start_times) {
		int fd = p.first;
		timeval st = p.second, et = end_times[fd];
		total_time += ((et.tv_sec - st.tv_sec) * 1000 + (et.tv_usec - st.tv_usec) / 1000);
	}
	std::cout << "Time: " << (end.tv_sec - start.tv_sec + (double)(end.tv_usec - start.tv_usec) / 1000000) << "s" << std::endl;
	std::cout << "Avg: " << ((end.tv_sec - start.tv_sec) * 1000 + (double)(end.tv_usec - start.tv_usec) / 1000) / (DEFAULT_USER_NUM * DEFAULT_REQ_NUM) << "ms" << std::endl;
	std::cout << "Avg time per request: " << (total_time / (DEFAULT_USER_NUM * DEFAULT_REQ_NUM)) << "ms" << std::endl;

    return 0;
}
