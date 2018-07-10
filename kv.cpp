/**
 * KV.cpp
 *
 * Referred to Bitcask technique
 */

#include "kv.h"

/** Map **/

Map::Map() {
	maps.resize(BucketSize);
	lockset.resize(BucketSize);

	for (auto& lock : lockset) {
		lock = PTHREAD_RWLOCK_INITIALIZER;
	}
}

uint32_t Map::hash(const string& key) {
	uint32_t bucketno = 0;

	for (auto& ch : key) {
		bucketno += (unsigned)ch * (unsigned)ch;
	}

	return bucketno % BucketSize;
}

Status Map::rdlock_key(const string& key) {
	Status s;
	uint32_t bucketno = hash(key);

	if (pthread_rwlock_rdlock(&lockset[bucketno]) != 0) {
		return s.IOError("Rdlock failed");
	}
	return s;
}

Status Map::wrlock_key(const string& key) {
	Status s;
	uint32_t bucketno = hash(key);

	if (pthread_rwlock_wrlock(&lockset[bucketno]) != 0) {
		return s.IOError("Wrlock failed");
	}
	return s;
}

Status Map::unlock_key(const string& key) {
	Status s;
	uint32_t bucketno = hash(key);

	if (pthread_rwlock_unlock(&lockset[bucketno]) != 0) {
		return s.IOError("Unlock failed");
	}
	return s;
}

bool Map::has(const string& key) {
	uint32_t bucketno = hash(key);
	return (maps[bucketno].count(key) > 0);
}

uint64_t Map::size() {
	uint64_t total = 0;

	for (auto& _map : maps) {
		total += _map.size();
	}

	return total;
}

bool Map::empty() {
	return (size() == 0);
}

void Map::clear() {
	for (auto& _map : maps) {
		_map.clear();
	}
}

void Map::copyTo(unordered_map<string, Index>& _whole_index) {
	_whole_index.clear();
	for (auto& _map : maps) {
		for (auto& p : _map) {
			_whole_index.insert(p);
		}
	}
}

Status Map::set(const string& key, const Index& index) {
	Status s;
	uint32_t bucketno = hash(key);

	wrlock_key(key);
	maps[bucketno][key] = index;
	unlock_key(key);
	return s;
}

Status Map::get(const string& key, Index& index) {
	Status s;
	uint32_t bucketno = hash(key);

	rdlock_key(key);
	if (!has(key)) {
		unlock_key(key);
		return s.IOError("Key " + key + " not found.");
	}
	index = maps[bucketno][key];
	unlock_key(key);
	return s;
}

Status Map::del(const string& key) {
	Status s;
	uint32_t bucketno = hash(key);

	wrlock_key(key);
	if (!has(key)) {
		unlock_key(key);
		return s.IOError("Key " + key + " not found.");
	}
	maps[bucketno].erase(key);
	unlock_key(key);
	return s;
}


/** Cache **/

Cache::Cache(uint32_t c) {
	_capacity = c;
	_size = 0;
	head = new Node("head", "");
	tail = new Node("tail", "");
	head->next = tail;
	tail->prev = head;
	_lock = PTHREAD_MUTEX_INITIALIZER;
}

Cache::~Cache() {
	Node *node = head, *next = nullptr;
	while (node) {
		next = node->next;
		delete node;
		node = next;
	}
}

void Cache::pop(Node *node) {
	node->next->prev = node->prev;
	node->prev->next = node->next;
}

void Cache::put_front(Node *node) {
	head->next->prev = node;
	node->next = head->next;
	head->next = node;
	node->prev = head;
}

Status Cache::set(const string& key, const string& value) {
	Status s;

	lock();
	if (table.count(key)) {
		Node *node = table[key];
		node->val = value;
		pop(node);
		put_front(node);
	} else {
		if (_size < _capacity) {	// if not full, add to front
			Node *node = new Node(key, value);
			++_size;
			put_front(node);
			table[key] = node;
		} else {		// else kick out the tail
			Node *node = tail->prev;
			table.erase(node->key);
			pop(node);
			node->key = key;
			node->val = value;
			put_front(node);
			table[key] = node;
		}
	}
	unlock();

	return s;
}

Status Cache::get(const string& key, string& value) {
	Status s;

	lock();
	if (table.count(key)) {
		value = table[key]->val;
		Node *node = table[key];
		pop(node);
		put_front(node);
		unlock();
		return s;
	} else {
		unlock();
		return s.NotFound("key " + key + " not found in cache.");
	}
}

Status Cache::del(const string& key) {
	Status s;

	lock();
	if (table.count(key)) {
		Node *node = table[key];
		pop(node);
		delete node;
		table.erase(key);
		--_size;
		unlock();
		return s;
	} else {
		unlock();
		return s.NotFound("key " + key + " not found in cache.");
	}
}


/** Env **/

Status Env::createDir(const string& name) {	// create directory
	Status s;
	if (mkdir(name.c_str(), 0755) != 0) {
		return s.IOError("Create directory " + name + " failed.");
	}
	return s;
}

Status Env::lock(const string& name, FileLock** l) {
	Status s;
	*l = nullptr;
	int fd;

	if ((fd = ::open(name.c_str(), O_RDWR | O_CREAT, 0644)) < 0) {
		return s.IOError("Lock file " + name + " failed, error: " + strerror(errno));
	} else if (lockUnlock(fd, true) == -1) {
		::close(fd);
		return s.IOError("Lock file " + name + " failed, error: " + strerror(errno));
	} else {
		FileLock* m_lock = new FileLock;
		m_lock->fd = fd;
		m_lock->name = name;
		*l = m_lock;
		return s;
	}
}

Status Env::unlock(FileLock* l) {
	Status s;
	if (lockUnlock(l->fd, false) == -1) {
		return s.IOError("Unlock file " + l->name + " failed.");
	}
	::close(l->fd);
	return s;
}

int Env::lockUnlock(int fd, bool lock) {
	errno = 0;
	struct flock f;
	memset(&f, 0, sizeof(f));
	f.l_type = (lock ? F_WRLCK : F_UNLCK);
	f.l_whence = SEEK_SET;
	f.l_start = 0;
	f.l_len = 0;
	return fcntl(fd, F_SETLK, &f);
}

Status Env::getChildren(const string& name, vector<string>& files) {
	Status s;
	DIR* dir;
	struct dirent* ent;

	files.clear();
	if ((dir = opendir(name.c_str())) == nullptr) {
		return s.IOError("Open directory " + name + " failed.");
	}
	while ((ent = readdir(dir)) != nullptr) {
		if (strcmp(ent->d_name, "..") == 0 || strcmp(ent->d_name, ".") == 0) {
			continue;
		}
		files.push_back(ent->d_name);
	}
	closedir(dir);
	return s;
}

uint32_t Env::getMaxId(const vector<string>& files, const string& base) {
	uint32_t id = 0, tmp = 0;
	for (auto& file : files) {
		sscanf(file.c_str(), (base + "%d").c_str(), &tmp);
		id = std::max(id, tmp);
	}
	return id;
}

bool Env::existFile(const string& name) {
	return access(name.c_str(), F_OK) == 0;
}


/**
 * DB
 */

DB::DB() : active_id(0), hint_id(0), cache(100), lock(nullptr), env(nullptr) {
	_disk_lock = PTHREAD_RWLOCK_INITIALIZER;
}

DB::~DB() {
	close();
	delete env;
	delete lock;
}

Status DB::open(const string& name) {
	dbname = name;
	return init();
}

Status DB::close() {
	Status s;

	if (active_ofs.is_open()) {
		active_ofs.close();
	}
	if (hint_ofs.is_open()) {
		hint_ofs.close();
	}
	//s = env->unlock(lock);
	return s;
}

Status DB::init() {
	Status s;
	vector<string> index_files, data_files;

	if (!env->existFile(dbname)) {
		env->createDir(dbname);
	}

	s = env->lock(dbname + LockFileName, &lock);
	if (!s.ok()) {
		return s;
	}

	if (env->existFile(dbname + IndexDirectory)) {
		s = env->getChildren(dbname + IndexDirectory, index_files);
		if (!s.ok()) {
			return s;
		}
	} else {
		env->createDir(dbname + IndexDirectory);
	}

	hint_id = env->getMaxId(index_files, HintFileName);

	// load index
	for (auto& file : index_files) {
		s = loadIndex(file);
		if (!s.ok()) {
			std::cout << s.toString() << std::endl;
		}
	}

	if (env->existFile(dbname + DataDirectory)) {
		s = env->getChildren(dbname + DataDirectory, data_files);
		if (!s.ok()) {
			return s;
		} else {
			active_id = env->getMaxId(data_files, DataFileName);
		}
	} else {
		env->createDir(dbname + DataDirectory);
	}

	s = newFileStream(active_ofs, active_id, active_size, DataDirectory, DataFileName);
	if (!s.ok()) {
		return s;
	}
	s = newFileStream(hint_ofs, hint_id, hint_size, IndexDirectory, HintFileName);
	if (!s.ok()) {
		return s;
	}

	return s;
}

Status DB::disk_rdlock() {
	Status s;

	if (pthread_rwlock_rdlock(&_disk_lock) != 0) {
		return s.IOError("Disk rdlock failed.");
	}
	return s;
}

Status DB::disk_wrlock() {
	Status s;

	if (pthread_rwlock_wrlock(&_disk_lock) != 0) {
		return s.IOError("Disk wrlock failed.");
	}
	return s;
}

Status DB::disk_unlock() {
	Status s;

	if (pthread_rwlock_unlock(&_disk_lock) != 0) {
		return s.IOError("Disk unlock failed.");
	}
	return s;
}

Status DB::newFileStream(ofstream& ofs, uint32_t& id, uint64_t& size, const string& dir, const string& filename) {
	Status s;

	if (ofs.is_open()) {
		ofs.close();
	}
	ofs.open(dbname + dir + "/" + filename + std::to_string(id), std::ios::in | std::ios::out | std::ios::app | std::ios::ate | std::ios::binary);
	size = ofs.tellp();

	if (ofs.is_open()) {
		return s;
	} else {
		return s.IOError("Open fstream failed.");
	}
}

Status DB::set(const string& key, const string& value) {
	Status s;
	Data data;
	Index index;

	data.time_stamp = env->timeStamp();
	data.key_size = static_cast<uint32_t>(key.size());
	data.val_size = static_cast<uint32_t>(value.size());
	data.key = key;
	data.value = value;
	data.crc = 0;	// NOT implement crc & magic here
	data.magic = 0;

	index.time_stamp = env->timeStamp();
	index.key_size = static_cast<uint32_t>(key.size());
	index.key = key;

	// write to disk
	disk_wrlock();
	uint64_t off = syncData(data);
	if (off < 0) {
		disk_unlock();
		return s.IOError("Write data failed.");
	}

	index.id = active_id;
	index.offset = off;
	index.valid = true;

	s = syncIndex(index);
	if (!s.ok()) {
		disk_unlock();
		return s;
	}
	disk_unlock();

	// update index
	_index.set(key, index);

	// update cache
	cache.set(key, value);

	return s;
}

uint64_t DB::syncData(const Data& data) {
	uint64_t off = active_size;
	if (active_size < MaxDataFileSize) {	// if not full
		active_ofs.write((char*)&data.time_stamp, sizeof(data.time_stamp));
		active_ofs.write((char*)&data.key_size, sizeof(data.key_size));
		active_ofs.write((char*)&data.val_size, sizeof(data.val_size));
		active_ofs.write(data.key.c_str(), data.key_size);
		active_ofs.write(data.value.c_str(), data.val_size);
		active_ofs.write((char*)&data.crc, sizeof(data.crc));
		active_ofs.write((char*)&data.magic, sizeof(data.magic));

		active_size += sizeof(time_t) + sizeof(uint32_t) * 4 + data.key_size + data.val_size;
		active_ofs.flush();
		return off;
	} else {
		active_ofs.close();

		Status s;
		s = newFileStream(active_ofs, ++active_id, active_size, DataDirectory, DataFileName);
		if (!s.ok()) {
			return -1;
		}
		return syncData(data);
	}
}

Status DB::syncIndex(const Index& index) {
	Status s;
	if (hint_size < MaxHintFileSize) {		// if not full
		hint_ofs.write((char*)&index.time_stamp, sizeof(index.time_stamp));
		hint_ofs.write((char*)&index.key_size, sizeof(index.key_size));
		hint_ofs.write(index.key.c_str(), index.key_size);
		hint_ofs.write((char*)&index.id, sizeof(index.id));
		hint_ofs.write((char*)&index.offset, sizeof(index.offset));
		hint_ofs.write((char*)&index.valid, sizeof(index.valid));
		
		hint_size += sizeof(time_t) + sizeof(uint32_t) * 2 + sizeof(uint64_t) + index.key_size + sizeof(bool);
		hint_ofs.flush();
		return s;
	} else {
		hint_ofs.close();

		s = newFileStream(hint_ofs, ++hint_id, hint_size, IndexDirectory, HintFileName);
		if (!s.ok()) {
			return s;
		}

		return syncIndex(index);
	}
}

Status DB::get(const string& key, string& value) {
	Status s;
	Index index;

	// firstly search in cache
	s = cache.get(key, value);
	if (s.ok()) {
		return s;
	}

	// if not found, search in index
	if (_index.has(key)) {
		_index.get(key, index);
		uint32_t id = index.id;
		uint64_t offset = index.offset;
		time_t ts;
		disk_rdlock();
		s = retrieve(key, id, offset, ts, value);
		disk_unlock();
		return s;
	} else {
		return s.NotFound("Key " + key + " not found.");
	}
}

Status DB::del(const string& key) {
	Status s;
	Index index;

	// delete in cache
	cache.del(key);

	// delete in disk
	if (_index.has(key)) {
		_index.get(key, index);
		index.time_stamp = env->timeStamp();
		index.valid = false;
		disk_wrlock();
		s = syncIndex(index);
		disk_unlock();

		_index.del(key);
		return s;
	} else {
		return s.NotFound("Key " + key + " not found.");
	}
}

Status DB::retrieve(const string& key, const uint32_t id, const uint64_t offset, time_t& ts, string& value) {
	Status s;
	ifstream ifs;
	ifs.open(dbname + DataDirectory + "/" + DataFileName + std::to_string(id), std::ios::out | std::ios::binary);
	ifs.seekg(offset, std::ios::beg);

	uint32_t key_size = 0, val_size = 0;
	ifs.read((char*)&ts, sizeof(ts));
	ifs.read((char*)&key_size, sizeof(key_size));
	ifs.read((char*)&val_size, sizeof(val_size));

	char *read_key = new char[key_size + 1], *read_val = new char[val_size + 1];
	ifs.read(read_key, key_size);
	ifs.read(read_val, val_size);
	read_key[key_size] = '\0';
	read_val[val_size] = '\0';
	value = string(read_val);

	delete[] read_key;
	delete[] read_val;
	ifs.close();
	return s;
}

Status DB::loadIndex(const string& file) {
	Status s;
	ifstream ifs;
	ifs.open(dbname + IndexDirectory + "/" + file, std::ios::out | std::ios::binary);
	ifs.seekg(0, std::ios::beg);
	if (!ifs.is_open()) {
		return s.IOError("open " + dbname + DataDirectory + "/" + file + " failed.");
	}

	while (ifs) {
		Index index;
		ifs.read((char*)&index.time_stamp, sizeof(time_t));
		if (ifs.eof()) break;
		ifs.read((char*)&index.key_size, sizeof(index.key_size));
		char *read_key = new char[index.key_size + 1];
		ifs.read(read_key, index.key_size);
		read_key[index.key_size] = '\0';
		index.key = string(read_key);
		delete[] read_key;
		ifs.read((char*)&index.id, sizeof(index.id));
		ifs.read((char*)&index.offset, sizeof(index.offset));
		ifs.read((char*)&index.valid, sizeof(bool));
		
		if (index.valid) {
			_index.set(index.key, index);
		} else {
			if (_index.has(index.key)) {
				_index.del(index.key);
			}
		}
	}

	ifs.close();

	return s;
}

string DB::exec(const string& cmd) {
	stringstream ss(cmd);
	string op, k, v;
	Status s;

	ss >> op;
	if (op == "set") {
		ss >> k >> v;
		s = set(k, v);
		if (!s.ok()) {
			return "set failed";
		} else {
			return "set success";
		}
	} else if (op == "get") {
		ss >> k;
		s = get(k, v);
		if (!s.ok()) {
			return s.toString();
		} else {
			return v;
		}
	} else if (op == "del") {
		ss >> k;
		s = del(k);
		if (!s.ok()) {
			return s.toString();
		} else {
			return "del success";
		}
	} else {
		return "invalid command";
	}
}

Status DB::merge() {
	Status s;
	string val;
	unordered_map<string, Index> _whole_index;
	unordered_map<string, string> kv;	
	vector<string> index_files, data_files;

	_index.copyTo(_whole_index);
	for (auto& p : _whole_index) {
		s = get(p.first, val);
		if (!s.ok()) {
			return s;
		}
		kv[p.first] = val;
	}
	
	s = env->getChildren(dbname + IndexDirectory, index_files);
	if (!s.ok()) {
		return s.IOError("Get children of " + dbname + IndexDirectory + " failed.");
	}
	for (auto& file : index_files) {
		if (remove((dbname + IndexDirectory + "/" + file).c_str()) != 0) {
			return s.IOError("Remove file " + dbname + IndexDirectory + "/" + file + " failed.");
		}
	}

	s = env->getChildren(dbname + DataDirectory, data_files);
	if (!s.ok()) {
		return s.IOError("Get children of " + dbname + DataDirectory + " failed.");
	}
	for (auto& file : data_files) {
		if (remove((dbname + DataDirectory + "/" + file).c_str()) != 0) {
			return s.IOError("Remove file " + dbname + DataDirectory + "/" + file + " failed.");
		}
	}


	s = newFileStream(active_ofs, active_id = 0, active_size, DataDirectory, DataFileName);
	if (!s.ok()) {
		return s;
	}

	s = newFileStream(hint_ofs, hint_id = 0, hint_size, IndexDirectory, HintFileName);
	if (!s.ok()) {
		return s;
	}


	_index.clear();
	for (auto& p : kv) {
		set(p.first, p.second);
	}

	std::cout << "Database size: " << _index.size() << std::endl;

	return s;
}



/** Debugger **/
Debugger::Debugger() {
	srand((unsigned)time(nullptr));
}


string Debugger::genString() {
	int len = 10 + rand() % 20;
	string res(len, 'a');
	for (auto& ch : res) {
		ch += rand() % 26;
	}
	return res;
}

void Debugger::test_db() {
	Status s;

	s = db.open("tmp___");
	if (!s.ok()) {
		cout << s.toString() << endl;
		system("rm -rf tmp___");
		return; 
	}

	cout << "====== Test set ======" << endl;

	// generate random kv pair
	unordered_map<string, string> kv;
	for (int i = 0; i < 5000; ++i) {
		string k = genString(), v = genString();
		kv[k] = v;
		s = db.set(k, v);
		if (!s.ok()) {
			cout << s.toString() << endl;
			cout << "Set failed" << endl;
			system("rm -rf tmp___");
			return;
		}
		cout << "Case " << i << " ok" << endl;
	}

	cout << "====== Set ok ======" << endl;

	cout << "====== Test merge & get ======" << endl;
	db.merge();
	s = db.merge();
	if (!s.ok()) {
		cout << s.toString() << endl;
		system("rm -rf tmp___");
		return;
	}

	for (auto& p : kv) {
		string k = p.first, v;
		s = db.get(k, v);
		if (!s.ok()) {
			cout << "Get failed" << endl;
			system("rm -rf tmp___");
			return;
		}
	}

	cout << "====== Merge & Get ok ======" << endl;
	
	cout << "====== Test random get ======" << endl;

	for (int i = 0; i < 1000; ++i) {
		string k = genString(), v;
		s = db.get(k, v);
		if (kv.count(k) && s.IsNotFound() || !kv.count(k) && s.ok()) {
			cout << "Random get failed" << endl;
			system("rm -rf tmp___");
			return;
		}
		cout << "Case " << i << " ok" << endl;
	}

	cout << "====== Random get ok ======" << endl;

	cout << "====== Test del ======" << endl;

	for (auto& p : kv) {
		string k = p.first;
		s = db.del(k);
		if (!s.ok()) {
			cout << s.toString() << endl;
			cout << "Del failed" << endl;
			system("rm -rf tmp___");
			return;
		}
	}

	cout << "====== Del ok ======" << endl;

	cout << "====== Check empty ======" << endl;

	if (!db._index.empty()) {
		cout << "Not empty! size: " << db._index.size() << endl;
		system("rm -rf tmp___");
		return;
	}

	cout << "====== All test pass ======" << endl;
	system("rm -rf tmp___");
}

void Debugger::ui() {
	string cmd;
	Status s;
	s = db.open("tmp___");

	if (!s.ok()) {
		cout << s.toString() << endl;
		system("rm -rf tmp___");
		return;
	}
	
	while (getline(cin, cmd)) {
		cout << db.exec(cmd) << endl;
	}
	system("rm -rf tmp___");
}

void* work(void* arg) {
	Job *job = (Job*)arg;
	string k, v;
	Status s;

	while (!job->kv->empty()) {
		// get next job
		pthread_mutex_lock(job->_lock);
		if (job->kv->empty()) break;
		auto it = job->kv->begin();
		k = it->first;
		v = it->second;
		job->kv->erase(k);
		pthread_mutex_unlock(job->_lock);
	
		switch (job->op) {
			case Job::SET:
				s = job->db->set(k, v);
				break;
			case Job::GET:
				s = job->db->get(k, v);
				break;
			case Job::DEL:
				s = job->db->del(k);
				break;
			default:
				break;
		}
	
		if (!s.ok()) {
			cout << s.toString() << endl;
		}
	}

	return nullptr;
}

void Debugger::test_concurrency() {
	int thread_num = 20, request_num = 50;
	pthread_mutex_t _lock = PTHREAD_MUTEX_INITIALIZER;
	pthread_t *pids = new pthread_t[thread_num];
	unordered_map<string, string> kv, cmp;
	Status s;

	auto clean = [&pids]() {	// cleaner
		delete[] pids;
		system("rm -rf tmp___");
	};

	s = db.open("tmp___");
	if (!s.ok()) {
		cout << s.ok() << endl;
		clean();
		return;
	}

	cout << "====== Concurrency test ======" << endl;
	// randomly generate kv
	cout << "Generating random kv pair" << endl;
	for (int i = 0; i < 2000; ++i) {
		string k = genString(), v = genString();
		kv[k] = v;
		cmp[k] = v;
	}

	Job job = { &db, &kv, &_lock, Job::SET };
	for (int i = 0; i < thread_num; ++i) {
		pthread_create(&pids[i], nullptr, work, (void*)&job);
	}
	for (int i = 0; i < thread_num; ++i) {
		pthread_join(pids[i], nullptr);
	}

	cout << "Checking results" << endl;

	// check results
	unordered_map<string, string> result;
	unordered_map<string, Index> _whole_index;
	
	db._index.copyTo(_whole_index);
	for (auto& p : _whole_index) {
		string value;
		s = db.get(p.first, value);
		if (!s.ok()) {
			cout << s.toString() << endl;
			break;
		}
		result[p.first] = value;
	}

	if (result == cmp) {
		cout << "====== Concurrency test success ======" << endl;
	} else {
		cout << "<!> Concurrency test failed" << endl;
	}

	clean();
}
