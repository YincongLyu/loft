//
// Created by Coonger on 2024/11/21.
//

#include <pthread.h>
#include <stdio.h>

namespace common {

int thread_set_name(const char *name)
{
  const int namelen = 16;
  char      buf[namelen];
  snprintf(buf, namelen, "%s", name);

#ifdef __APPLE__
  return pthread_setname_np(buf);
#elif __linux__
  return pthread_setname_np(pthread_self(), buf);
#endif
}

}  // namespace common