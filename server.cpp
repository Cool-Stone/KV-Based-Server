/**
 * File: server.cpp
 *
 * This file is used to set up a server
 */
#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <signal.h>
#include <pthread.h>
#include <unordered_map>
#include <atomic>
#include "tcp.h"
#include "epl.h"
#include "protocol.h"
#include "kv.h"

#define DEBUG false         // for debug 
#define SPEEDUP true        // cancel sync with stdio, be careful with this 
#define DETAILED false      // print out detailed content 

using std::unordered_map;

/* Macro definitions */
#define DEFAULT_PORT 9000
#define THREADSIZE 9
#define BUFSIZE 2048
#define EVENTSIZE 20000

#define log(msg) (std::cout << (msg) << std::flush)
#define err_log(msg) (std::cerr << (msg) << std::flush)


/** This struct transfers parameters to threads **/

struct Arg {
    int epfd;
    int listenfd;
    int nfds;
    epoll_event* events;
    volatile int* cur;
    pthread_mutex_t *_lock;
	unordered_map<int, Processor*>* table;
	DB* db;
};


void initialize(int &port, int &listenfd, int &epfd, DB& db, int argc, char* argv[]);
int parse(int& port, int argc, char* argv[]);
void* serve(void* arg);    // create threads to deal with tasks 



int main(int argc, char* argv[]) {
    // viariables of socket and epoll 
    int port, listenfd, epfd, nfds;
	DB db;
	unordered_map<int, Processor*> table;
    pthread_t pids[THREADSIZE];
    epoll_event *events = (epoll_event*)malloc(EVENTSIZE * sizeof(epoll_event));
    

#if DEBUG 
    int logfd = open("server-log.txt", O_RDWR | O_CREAT, 0666),
        elogfd = open("server-err_log.txt", O_RDWR | O_CREAT, 0666);
    if (logfd < 0 || elogfd < 0) {
        err_log("Open log failed\n");
        exit(1);
    }
    dup2(logfd, 1);
    dup2(elogfd, 2);
#endif


#if SPEEDUP
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);
#endif

    // initialize 
    initialize(port, listenfd, epfd, db, argc, argv); 

    // main loop
    while (true) {
        nfds = epoll_wait(epfd, events, EVENTSIZE, -1);
        if (nfds < 0) {
            perror("epoll_wait");
            exit(1);
        }
        
        // arguments 
        volatile int i = 0;
        pthread_mutex_t _lock = PTHREAD_MUTEX_INITIALIZER;
        Arg arg = { epfd, listenfd, nfds, events, &i, &_lock, &table , &db };

        for (int n = 0; n < THREADSIZE; ++n) {
            pthread_create(&pids[n], nullptr, serve, (void*)&arg);
        }
        for (int n = 0; n < THREADSIZE; ++n) {
            pthread_join(pids[n], nullptr);
        }

		//db.merge();	// TODO: judge whether to merge or not
    }

    free(events);
    close(epfd);
	for (auto &_p : table) {
		delete _p.second;
	}
	db.close();
}



/*************** Definitions ***************/


int parse(int& port, int argc, char* argv[]) {
    if (argc == 1) {
        port = DEFAULT_PORT;
        return 0;
    } else if (argc == 2) {
        port = atoi(argv[1]);
        return 0;
    } else {
        std::cerr << "Usage: " << argv[0] << " <port>" << std::endl;
        return 1;
    }
}

void initialize(int &port, int &listenfd, int &epfd, DB& db, int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);
    if (parse(port, argc, argv)) {
        err_log("Error occurs when parsing command line arguments\n");
        exit(1);
    }

    log("Initializing...\n");
    if ((listenfd = open_listenfd(port)) < 0) {
        err_log("Open listen fd failed\n");
        exit(1);
    }
    setnonblock(listenfd);
    log("Success, server installed.\n");
    log("Creating epoll...\n");
    if ((epfd = epoll_create1(0)) < 0) {    // create epoll
        perror("epoll");
        exit(1);
    }
    log("Success, epoll installed.\n");
    addfd(epfd, listenfd);
    log("Success, listenfd added to epoll.\n");

	Status s;
	s = db.open("db");
	if (!s.ok()) {
		std::cout << s.toString() << std::endl;
		exit(1);
	}
	log("Success, database opened.\n");
}


/**
 * WARING: edge-triggered requests us to read out all the data each time
 */


void* serve(void *arg) {
    Arg *para = (Arg*)arg;
    while (*para->cur < para->nfds) {
        if (pthread_mutex_trylock(para->_lock) == 0) {
            if (*para->cur < para->nfds) {  // check again, since it may have been changed by other thread 
                epoll_event event = para->events[(*para->cur)++];
                pthread_mutex_unlock(para->_lock);
                int sockfd = event.data.fd;
                if (sockfd == para->listenfd) {     // if client connects 
                    sockaddr_in clientaddr;
                    socklen_t clientlen = sizeof(clientaddr);
                    int connfd;
                    // read ALL clients, otherwise error may occur 
                    while ((connfd = accept(para->listenfd, (SA*)&clientaddr, &clientlen)) > 0) {
                        setnonblock(connfd);
                        addfd(para->epfd, connfd, EPOLL_CTL_ADD, EPOLLIN | EPOLLET);
                        //std::cout << "Server connected to " << inet_ntoa(clientaddr.sin_addr) << std::endl;
						(*para->table)[connfd] = new Processor(connfd);
                    }
                } else if (sockfd > 0 && (event.events & EPOLLIN)) {
					Processor* proc = (*para->table)[sockfd];
					int read_bytes;
					string res;

					if ((read_bytes = proc->read()) < 0) {
						//delete proc;
						//para->table->erase(sockfd);
					} else if (read_bytes == 0) {
						//delete proc;
						//para->table->erase(sockfd);
						//log("Client quited\n");
						//continue;
					} else {
						//std::cout << "Received " << read_bytes << " bytes" << std::endl;
					}
					while (proc->ready()) {
						res = para->db->exec(proc->request());
						if (proc->response(res) < 0) {
							delete proc;
							para->table->erase(sockfd);
						}
					}
				}
            } else {
                pthread_mutex_unlock(para->_lock);
            }
        }
    }
    return nullptr;
}

