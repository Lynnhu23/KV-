#include "app/kv_server.h"

int main(int argc, char *argv[])
{
    KVServer server;
    if (!server.init(argc, argv))
    {
        return 1;
    }

    return server.start();
}
