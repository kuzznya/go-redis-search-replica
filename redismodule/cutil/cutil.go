package cutil

//#include "./cutil.h"
import "C"
import "unsafe"
import "reflect"
import "fmt"

//import "github.com/davecgh/go-spew/spew"

// (**char,n) -> [][]byte
func StrArrToByteSlice(arr unsafe.Pointer, c int) [][]byte {
	slices := make([][]byte, c)
	for i := 0; i < c; i++ {
		s := C.StrArrAt((**C.char)(arr), C.int(i))
		l := C.int(C.strlen(s))
		//func C.GoBytes(cArray unsafe.Pointer, length C.int) []byte
		slices[i] = C.GoBytes(unsafe.Pointer(s), l)
	}
	return slices
}

// (**char,n) -> []string
func StrArrToStringSlice(strArr unsafe.Pointer, c int) []string {
	slices := make([]string, c)
	for i := 0; i < c; i++ {
		slices[i] = string(C.GoString(C.StrArrAt((**C.char)(strArr), C.int(i))))
	}
	return slices
}
func PtrToUintptr(ptr unsafe.Pointer) uintptr {
	return reflect.ValueOf(ptr).Pointer()
}

func PtrArrAt(arr unsafe.Pointer, i int) uintptr {
	return uintptr(C.PtrArrAt(arr, C.int(i)))
}

// Free this after use
// defer C.free(ptr)
func VarArgsPtr(args ...interface{}) (unsafe.Pointer, error) {
	size := int(unsafe.Sizeof(C.uintptr_t(0)))
	list := C.malloc(C.size_t(size * len(args)))

	//values := make([]uintptr, len(args))
	for i, arg := range args {
		ptr := unsafe.Pointer(uintptr(list) + uintptr(size*i))
		var val C.uintptr_t
		ref := reflect.ValueOf(arg)
		switch ref.Kind() {
		case reflect.Chan, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Func, reflect.Slice:
			val = C.uintptr_t(ref.Pointer())
		case reflect.Uintptr:
			val = C.uintptr_t(ref.Uint())
		default:
			return nil, fmt.Errorf("Can not cast %T %#v to uintptr", arg, arg)
		}
		*(*C.uintptr_t)(ptr) = val
		//values[i] = uintptr(val)
	}
	//spew.Dump(values,list)
	return list, nil
}
