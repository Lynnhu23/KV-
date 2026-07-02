#ifndef CLUSTER_NODE_H
#define CLUSTER_NODE_H

#include <string>

struct ClusterNode
{
    std::string id;
    std::string host;
    int port = 0;
};

#endif
