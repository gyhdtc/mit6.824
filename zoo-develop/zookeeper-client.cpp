#include <zookeeper.h>
#include <zookeeper_log.h>
#include <iostream>
#include <unistd.h>
using namespace std;

const char* host = "10.7.18.31:2181,10.7.18.199:2181";
const int timeout = 2000;
zhandle_t* zkhandle = NULL;

void InitWatch(zhandle_t* zh, int type, int state, const char* path, void* watcher) {
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      cout << "build connection ok" << endl;
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      cout << "connection disconnect" << endl;
      zkhandle = zookeeper_init(host, InitWatch, timeout, 0, const_cast<char*>("TODO"), 0);
    }
  }
}


void ChildWatch(zhandle_t* zh, int type, int state, const char* path, void* watcher) {
  if (type == ZOO_CHILD_EVENT) {
    struct String_vector str_vec;
    int ret = zoo_wget_children(zh, "/cluster", ChildWatch, 0, &str_vec);
    if (ret) {
      cout << "zoo_wget_children error :" << ret << endl;
    }
    cout << "endpoint change: " << endl;
    for (int i = 0; i < str_vec.count; ++i) {
      string endpoint = str_vec.data[i]; 
      cout << endpoint << endl;
    }
  }
}

int main(int argc, char* argv[]) {
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
  if (zkhandle) {
    zookeeper_close(zkhandle);
  }
  zkhandle = zookeeper_init(host, InitWatch, timeout, 0, const_cast<char*>("TODO"), 0);
  if (NULL == zkhandle) {
    cout << "zookeeper init error" << endl;
    return 0;
  }
  while (1) {
    struct String_vector str_vec;
    int ret = zoo_wget_children(zkhandle, "/cluster", ChildWatch, const_cast<char*>(""), &str_vec);
    if (ret) {
      cout << "zoo_wget_children error :" << ret << endl;
    }
    for (int i = 0; i < str_vec.count; ++i) {
      string endpoint = str_vec.data[i]; 
      cout << "init endpoint :" << endpoint << endl;
    }
    sleep(120);
  }

  zookeeper_close(zkhandle);
}