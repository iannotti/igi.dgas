#include <iostream>
#include <sstream>
#include <string>
#include <openssl/md5.h>  // for md5sum method
#include <openssl/sha.h>  // for sha1sum method

using namespace std;

string md5sum(string &);
string sha1sum(string &);

