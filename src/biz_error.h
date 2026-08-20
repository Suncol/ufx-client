#ifndef UFX_BIZ_ERROR_H
#define UFX_BIZ_ERROR_H

#include <string>

namespace ufx {
namespace detail {

inline bool IsT2SdkErrorReturnCode(int return_code) {
    return return_code == 1 || return_code == -1;
}

inline int ResolveBizErrorCode(bool head_valid, int head_error_code,
                               int sdk_error_no, int return_code) {
    if (head_valid && head_error_code != 0) {
        return head_error_code;
    }
    if (sdk_error_no != 0) {
        return sdk_error_no;
    }
    if (return_code != 0) {
        return return_code;
    }
    return -1;
}

inline std::string JoinUfxErrorText(const std::string& error_msg,
                                    const std::string& msg_detail) {
    if (error_msg.empty()) {
        return msg_detail;
    }
    if (msg_detail.empty() || msg_detail == error_msg) {
        return error_msg;
    }
    return error_msg + "; MsgDetail: " + msg_detail;
}

inline std::string ResolveBizErrorText(bool head_valid,
                                       const std::string& error_msg,
                                       const std::string& msg_detail,
                                       const std::string& sdk_error_info,
                                       const std::string& fallback) {
    if (head_valid) {
        const std::string ufx_text = JoinUfxErrorText(error_msg, msg_detail);
        if (!ufx_text.empty()) {
            return ufx_text;
        }
    }
    return sdk_error_info.empty() ? fallback : sdk_error_info;
}

}  // namespace detail
}  // namespace ufx

#endif
