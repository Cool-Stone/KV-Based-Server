#include <string>
#include <sstream>
#include <ctime>
#include <getopt.h>

#include "kv.h"

using namespace std;

int main(int argc, char* argv[]) {
	if (argc != 2) {
		cout << "Usage: " << argv[0] << " < debug / ui / con >" << std::endl;
		exit(1);
	}
	
	Debugger debugger;
	
	if (!strcmp(argv[1], "debug")) {
		debugger.test_db();
	} else if (!strcmp(argv[1], "ui")) {
		debugger.ui();
	} else if (!strcmp(argv[1], "con")) { 
		debugger.test_concurrency();
	} else {
		cout << "invalid option: " << argv[1] << endl;
	}

	return 0;

}
