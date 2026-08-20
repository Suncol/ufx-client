#ifndef UFX_TYPES_H
#define UFX_TYPES_H

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

namespace ufx {

template <int Scale>
class FixedDecimal {
public:
    static_assert(Scale >= 0, "FixedDecimal scale must be non-negative");

    FixedDecimal() : scaled_value_(0) {}

    static FixedDecimal FromScaled(int64_t scaled_value) {
        FixedDecimal value;
        value.scaled_value_ = scaled_value;
        return value;
    }

    static bool Parse(const std::string& text, FixedDecimal* value) {
        return ParseChars(text.data(), text.size(), value);
    }

    static bool Parse(const char* text, FixedDecimal* value) {
        if (text == NULL) {
            return false;
        }
        return ParseChars(text, std::strlen(text), value);
    }

    int64_t ScaledValue() const { return scaled_value_; }

    std::string ToString() const {
        const bool negative = scaled_value_ < 0;
        uint64_t magnitude = 0;
        if (negative) {
            magnitude = static_cast<uint64_t>(-(scaled_value_ + 1));
            ++magnitude;
        } else {
            magnitude = static_cast<uint64_t>(scaled_value_);
        }

        std::string digits;
        do {
            digits.insert(digits.begin(), static_cast<char>('0' + magnitude % 10U));
            magnitude /= 10U;
        } while (magnitude != 0U);

        if (Scale > 0) {
            while (digits.size() <= static_cast<size_t>(Scale)) {
                digits.insert(digits.begin(), '0');
            }
            digits.insert(digits.end() - Scale, '.');
        }
        if (negative) {
            digits.insert(digits.begin(), '-');
        }
        return digits;
    }

    bool operator==(const FixedDecimal& other) const {
        return scaled_value_ == other.scaled_value_;
    }
    bool operator!=(const FixedDecimal& other) const { return !(*this == other); }
    bool operator<(const FixedDecimal& other) const {
        return scaled_value_ < other.scaled_value_;
    }

private:
    static bool ParseChars(const char* text, size_t length, FixedDecimal* value) {
        if (value == NULL || text == NULL || length == 0U) {
            return false;
        }

        size_t pos = 0;
        bool negative = false;
        if (text[pos] == '+' || text[pos] == '-') {
            negative = text[pos] == '-';
            ++pos;
        }
        if (pos == length) {
            return false;
        }

        uint64_t magnitude = 0;
        int fractional_digits = -1;
        bool saw_digit = false;
        for (; pos < length; ++pos) {
            const char ch = text[pos];
            if (ch == '.') {
                if (fractional_digits >= 0) {
                    return false;
                }
                fractional_digits = 0;
                continue;
            }
            if (ch < '0' || ch > '9') {
                return false;
            }
            saw_digit = true;
            if (fractional_digits >= Scale) {
                return false;
            }
            const uint64_t digit = static_cast<uint64_t>(ch - '0');
            if (magnitude > (std::numeric_limits<uint64_t>::max() - digit) / 10U) {
                return false;
            }
            magnitude = magnitude * 10U + digit;
            if (fractional_digits >= 0) {
                ++fractional_digits;
            }
        }
        if (!saw_digit) {
            return false;
        }

        if (fractional_digits < 0) {
            fractional_digits = 0;
        }
        for (int i = fractional_digits; i < Scale; ++i) {
            if (magnitude > std::numeric_limits<uint64_t>::max() / 10U) {
                return false;
            }
            magnitude *= 10U;
        }

        const uint64_t max_positive =
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        const uint64_t max_negative = max_positive + 1U;
        if ((!negative && magnitude > max_positive) ||
            (negative && magnitude > max_negative)) {
            return false;
        }

        if (negative) {
            value->scaled_value_ = magnitude == max_negative
                                       ? std::numeric_limits<int64_t>::min()
                                       : -static_cast<int64_t>(magnitude);
        } else {
            value->scaled_value_ = static_cast<int64_t>(magnitude);
        }
        return true;
    }
    int64_t scaled_value_;
};

typedef FixedDecimal<2> Money;
typedef FixedDecimal<4> Price;

struct QueryScope {
    std::string account_code;
    std::string asset_no;
    std::string combi_no;
};

struct PositionView {
    std::string account_code;
    std::string asset_no;
    std::string combi_no;
    std::string market_no;
    std::string stock_code;
    std::string invest_type;
    std::string stockholder_id;
    std::string hold_seat;
    int64_t current_amount;
    int64_t enable_amount;
    int64_t unsellable_amount;
    int64_t pre_buy_amount;
    int64_t pre_sell_amount;
    int64_t today_buy_amount;
    int64_t today_sell_amount;

    PositionView()
        : current_amount(0),
          enable_amount(0),
          unsellable_amount(0),
          pre_buy_amount(0),
          pre_sell_amount(0),
          today_buy_amount(0),
          today_sell_amount(0) {}
};

struct OrderView {
    int business_date;
    int64_t entrust_no;
    std::string msgtype;
    std::string entrust_state;
    std::string account_code;
    std::string asset_no;
    std::string combi_no;
    std::string market_no;
    std::string stock_code;
    std::string entrust_direction;
    Price entrust_price;
    int64_t entrust_amount;
    int64_t deal_amount;
    int64_t last_fill_amount;
    int64_t cancel_amount;
    int64_t extsystem_id;
    std::string deal_no;
    int64_t realdeal_serial_no;

    OrderView()
        : business_date(0),
          entrust_no(0),
          entrust_amount(0),
          deal_amount(0),
          last_fill_amount(0),
          cancel_amount(0),
          extsystem_id(0),
          realdeal_serial_no(0) {}
};

struct AccountView {
    std::string account_code;
    std::string asset_no;
    Money enable_balance_t0;
    Money enable_balance_t1;
    Money current_balance;
};

inline std::string KeyPart(const std::string& value) {
    return std::to_string(value.size()) + ":" + value;
}

inline std::string PositionKey(const PositionView& p) {
    return KeyPart(p.account_code) + KeyPart(p.combi_no) + KeyPart(p.market_no) +
           KeyPart(p.stock_code) + KeyPart(p.invest_type) +
           KeyPart(p.stockholder_id) + KeyPart(p.hold_seat);
}

inline std::string AccountKey(const AccountView& a) {
    return KeyPart(a.account_code) + KeyPart(a.asset_no);
}

inline bool MatchesScope(const PositionView& row, const QueryScope& scope) {
    return (scope.account_code.empty() || row.account_code == scope.account_code) &&
           (scope.asset_no.empty() || row.asset_no == scope.asset_no) &&
           (scope.combi_no.empty() || row.combi_no == scope.combi_no);
}

inline std::string RefreshKey(const QueryScope& scope) {
    return KeyPart(scope.account_code) + KeyPart(scope.asset_no) +
           KeyPart(scope.combi_no);
}

inline int64_t UnsellableAmount(int64_t current_amount, int64_t enable_amount) {
    return current_amount > enable_amount ? current_amount - enable_amount : 0;
}

}  // namespace ufx

#endif
