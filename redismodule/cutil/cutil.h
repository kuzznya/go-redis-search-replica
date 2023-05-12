#ifndef CGO_CUTIL_H
#define CGO_CUTIL_H

#include <string.h>
#include <stdlib.h>
#include <stdint.h>

// 避免在 Go 进行指针运算
char *StrArrAt(char **strArr, int n) {
  return strArr[n];
}

uintptr_t PtrArrAt(void*arr, int n) {
  return (uintptr_t)((void**)arr)[n];
}

uintptr_t PtrToIntptr(void*ptr){
    return (uintptr_t)(ptr);
}
#endif

