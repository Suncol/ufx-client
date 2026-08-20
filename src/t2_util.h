#ifndef UFX_T2_UTIL_H
#define UFX_T2_UTIL_H

#include "t2sdk_interface.h"
#include "ufx/types.h"

#include <cerrno>
#include <climits>
#include <cstdlib>
#include <string>
#include <vector>

namespace ufx {
namespace t2 {

inline std::string CStr(const char* s) { return s ? std::string(s) : std::string(); }

inline bool HasField(IF2UnPacker* unpack, const char* name) {
    return unpack != NULL && name != NULL && unpack->FindColIndex(name) >= 0;
}

inline int FieldIndex(IF2UnPacker* unpack, const char* name) {
    return unpack != NULL && name != NULL ? unpack->FindColIndex(name) : -1;
}

inline bool FieldStrByIndex(IF2UnPacker* unpack, int index, std::string* value) {
    if (unpack == NULL || index < 0 || value == NULL) {
        return false;
    }
    const char* text = unpack->GetStrByIndex(index);
    if (text == NULL) {
        return false;
    }
    *value = text;
    return true;
}

inline bool FieldStr(IF2UnPacker* unpack, const char* name, std::string* value) {
    return FieldStrByIndex(unpack, FieldIndex(unpack, name), value);
}

inline bool ParseInt64(const char* text, int64_t* value) {
    if (value == NULL || text == NULL || *text == '\0') {
        return false;
    }
    errno = 0;
    char* end = NULL;
    const long long parsed = std::strtoll(text, &end, 10);
    if (errno == ERANGE || end == text || *end != '\0') {
        return false;
    }
    *value = static_cast<int64_t>(parsed);
    return true;
}

inline bool ParseInt64(const std::string& text, int64_t* value) {
    return ParseInt64(text.c_str(), value);
}

inline bool FieldInt64ByIndex(IF2UnPacker* unpack, int index, int64_t* value) {
    if (unpack == NULL || index < 0 || value == NULL) {
        return false;
    }
    return ParseInt64(unpack->GetStrByIndex(index), value);
}

inline bool FieldInt64(IF2UnPacker* unpack, const char* name, int64_t* value) {
    return FieldInt64ByIndex(unpack, FieldIndex(unpack, name), value);
}

inline bool FieldInt(IF2UnPacker* unpack, const char* name, int* value) {
    int64_t parsed = 0;
    if (value == NULL || !FieldInt64(unpack, name, &parsed) || parsed < INT_MIN ||
        parsed > INT_MAX) {
        return false;
    }
    *value = static_cast<int>(parsed);
    return true;
}

template <int Scale>
inline bool FieldDecimalByIndex(IF2UnPacker* unpack, int index,
                                FixedDecimal<Scale>* value) {
    if (unpack == NULL || index < 0 || value == NULL) {
        return false;
    }
    return FixedDecimal<Scale>::Parse(unpack->GetStrByIndex(index), value);
}

template <int Scale>
inline bool FieldDecimal(IF2UnPacker* unpack, const char* name,
                         FixedDecimal<Scale>* value) {
    return FieldDecimalByIndex(unpack, FieldIndex(unpack, name), value);
}

inline std::vector<char> CopyBuffer(const void* data, int len) {
    std::vector<char> out;
    if (data != NULL && len > 0) {
        out.assign(static_cast<const char*>(data), static_cast<const char*>(data) + len);
    }
    return out;
}

struct ErrorHead {
    bool valid;
    int error_code;
    int data_count;
    std::string error_msg;
    std::string msg_detail;

    ErrorHead() : valid(false), error_code(-1), data_count(0) {}
};

inline ErrorHead ReadHead(IF2UnPacker* unpack) {
    ErrorHead head;
    if (unpack == NULL || unpack->GetDatasetCount() <= 0) {
        head.error_msg = "empty response header";
        return head;
    }
    unpack->SetCurrentDatasetByIndex(0);
    unpack->First();
    if (!FieldInt(unpack, "ErrorCode", &head.error_code) ||
        !FieldInt(unpack, "DataCount", &head.data_count) ||
        !FieldStr(unpack, "ErrorMsg", &head.error_msg) ||
        !FieldStr(unpack, "MsgDetail", &head.msg_detail)) {
        head.error_code = -1;
        head.error_msg = "invalid response header";
        return head;
    }
    head.valid = true;
    return head;
}

inline void ReleasePacker(IF2Packer* packer) {
    if (packer == NULL) {
        return;
    }
    packer->FreeMem(packer->GetPackBuf());
    packer->Release();
}

}  // namespace t2
}  // namespace ufx

#endif
