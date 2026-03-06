#include "type_name_as_string.h"

std::string current_exception_name(){
    std::unique_ptr<char, void(*)(void*)> own
           (
#ifndef _MSC_VER
                abi::__cxa_demangle(abi::__cxa_current_exception_type()->name(), nullptr,
                                           nullptr, nullptr),
#else
                nullptr,
#endif
                std::free
           );
    std::string r = own != nullptr ? own.get() : abi::__cxa_current_exception_type()->name();
    return r;
}
