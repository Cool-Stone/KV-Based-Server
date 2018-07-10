#ifndef KV_H_
#define KV_H_

/**
 * KV store -- bitcask
 */

#include <iostream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sstream>
#include <pthread.h>
#include <errno.h>

using namespace std;

/** global variables **/
const string IndexDirectory = "/index";
const string DataDirectory = "/data";
const string HintFileName = "hint";
const string DataFileName = "data";
const string LockFileName = "/LOCK";
const uint32_t MaxDataFileSize = 1 << 26;	// 64M
const uint32_t MaxHintFileSize = 1 << 25;
const uint32_t BucketSize = 107;


struct Data {
	time_t time_stamp;
	uint32_t key_size;
	uint32_t val_size;
	string key;
	string value;
	uint32_t crc;
	uint32_t magic;
};

struct Index {
	time_t time_stamp;
	uint32_t key_size;
	string key;
	uint32_t id;		// data file number
	uint64_t offset;
	bool valid;
};

struct FileLock {
	int fd;
	string name;
};


class Debugger;
class Status;
class Env;
class Cache;
class Map;

/**
 * Map
 *
 * based on hash map, equiped with lock
 */

class Map {
public:
	Map();
	Status set(const string& key, const Index& index);
	Status get(const string& key, Index& index);
	Status del(const string& key);
	
	bool has(const string& key);
	bool empty();
	uint64_t size();
	void clear();
	void copyTo(unordered_map<string, Index>& _whole_index);
private:
	vector<unordered_map<string, Index>> maps;
	vector<pthread_rwlock_t> lockset;

	Status rdlock_key(const string& key);
	Status wrlock_key(const string& key);
	Status unlock_key(const string& key);
	uint32_t hash(const string& key);
};

/**
 * Cache
 *
 * cache key-value pair, using LRU strategy
 */

struct Node {
	string key, val;
	Node *next, *prev;
	Node(const string& k, const string& v) : key(k), val(v), next(nullptr), prev(nullptr) {}
};

class Cache {
	friend class Debugger;
public:
	Cache(uint32_t c);
	~Cache();
	Status set(const string& key, const string& value);
	Status get(const string& key, string& value);
	Status del(const string& key);
private:	
	pthread_mutex_t _lock;
	uint32_t _capacity;
	uint32_t _size;
	Node *head, *tail;
	unordered_map<string, Node*> table;

	void lock() { pthread_mutex_lock(&_lock); }
	void unlock() { pthread_mutex_unlock(&_lock); }
	void pop(Node* n);
	void put_front(Node* n);
};


/**
 * DB
 *
 * database engine
 */

class DB {
	friend class Debugger;
public:
	DB();
	~DB();
	Status open(const string& dbname);
	Status set(const string& key, const string& value);
	Status get(const string& key, string& value);
	Status del(const string& key);
	Status merge();
	string exec(const string& cmd);
	Status close();
private:
	FileLock* lock;		// so that another process is denied from read/write this database
	pthread_rwlock_t _disk_lock;		// protect disk

	string dbname;
	Map _index;	// index
	Cache cache;							// cache

	// active data file
	uint32_t active_id;
	uint64_t active_size;
	ofstream active_ofs;

	// hint file
	uint32_t hint_id;
	uint64_t hint_size;
	ofstream hint_ofs;

	Env* env;

	// lock disk
	Status disk_rdlock();
	Status disk_wrlock();
	Status disk_unlock();

	Status init();
	Status newFileStream(ofstream& fs, uint32_t& id, uint64_t& size, const string& dir, const string& filename);
	uint64_t syncData(const Data& data);
	Status syncIndex(const Index& index);
	Status retrieve(const string& key, const uint32_t id, const uint64_t offset, time_t& time_stamp, string& value);
	Status loadIndex(const string& filename);
};


/**
 * Status
 *
 * show message if error occurs
 */

class Status {
public:
	Status() : code(cOk) {}
	bool ok() { return code == cOk; }
	bool IsNotFound() { return code == cNotFound; }
	bool IsIOError() { return code == cIOError; }
	string toString() { return msg; }
	Status Ok() { return Status(); }
	Status NotFound(const string& msg) { return Status(cNotFound, msg); }
	Status IOError(const string& msg) { return Status(cIOError, msg); }
private:
	enum Code { cOk = 0, cNotFound = 1, cIOError = 2 };
	Code code;
	string msg;
	Status(Code c, const string& m) : code(c), msg(m) {}
};


/**
 * Env
 *
 * provide some file and directory operations
 */

class Env {
public:
	Status createDir(const string& name);
	time_t timeStamp() {
		time_t ts;
		time(&ts);
		return ts;
	}
	Status lock(const string& name, FileLock** l);
	Status unlock(FileLock* l);
	Status getChildren(const string& name, vector<string>& files);
	uint32_t getMaxId(const vector<string>& files, const string& base);
	bool existFile(const string& name);
	int lockUnlock(int fd, bool lock);
};




/**
 * Debugger
 *
 * debug
 */

struct Job {
	enum type_t { SET = 0, GET = 1, DEL = 2 };
	DB *db;
	unordered_map<string, string> *kv;
	pthread_mutex_t *_lock;
	type_t op;
};

class Debugger {
public:
	Debugger();
	void ui();
	void test_db();
	void test_concurrency();
	string genString();
private:
	DB db;
};

#endif
