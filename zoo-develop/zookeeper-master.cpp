#include <zookeeper.h>
#include <zookeeper_log.h>
#include <iostream>
#include <unistd.h>
using namespace std;

 zhandle_t* zkhandle = NULL;
 const char* host = "10.7.18.31:2181,10.7.18.199:2181";
 int timeout = 20000;
 
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

void CreateWatch(int rc, const char *name, const void *data) {
  if (rc == ZNODEEXISTS || rc == ZOK) {
    if (rc == ZOK) {
      cout << "registry ok" << endl;
    } else {
      cout << "node exist" << endl;
    }
  } else {
    cout << "registry error" << endl;
  }
}

int main(int argc, char* argv[]) {
  zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
  zhandle_t* zkhandle = NULL;
  zkhandle = zookeeper_init(host, InitWatch, timeout, 0, const_cast<char*>("TODO"), 0);
  if (NULL == zkhandle) {
    cout << "zookeeper init error" << endl;
    return 0;
  }
 while (1) {
  int ret = zoo_acreate(zkhandle, "/cluster/index+endpoint", "", 8, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, CreateWatch, "create");
  if (ret) {
    cout << "zoo_acreate error :" << ret << endl;
  }
  sleep(5);
 }

 zookeeper_close(zkhandle);
 
 return 0;
}