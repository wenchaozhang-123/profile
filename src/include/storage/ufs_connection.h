#ifndef UFS_CONNECTION_H
#define UFS_CONNECTION_H

#include <gopher/gopher.h>

#define DFS_MAX_PATH_SIZE    256

extern gopherFS UfsGetConnection(const char *serverName, const char *path);

#endif  /* UFS_CONNECTION_H */
