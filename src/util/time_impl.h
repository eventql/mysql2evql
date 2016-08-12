constexpr UnixTime::UnixTime(uint64_t utc_time) :
    utc_micros_(utc_time) {}

constexpr bool UnixTime::operator==(const UnixTime& other) const {
  return utc_micros_ == other.utc_micros_;
}

constexpr bool UnixTime::operator!=(const UnixTime& other) const {
  return utc_micros_ != other.utc_micros_;
}

constexpr bool UnixTime::operator<(const UnixTime& other) const {
  return utc_micros_ < other.utc_micros_;
}

constexpr bool UnixTime::operator>(const UnixTime& other) const {
  return utc_micros_ > other.utc_micros_;
}

constexpr bool UnixTime::operator<=(const UnixTime& other) const {
  return utc_micros_ <= other.utc_micros_;
}

constexpr bool UnixTime::operator>=(const UnixTime& other) const {
  return utc_micros_ >= other.utc_micros_;
}

constexpr UnixTime::operator uint64_t() const {
  return utc_micros_;
}

constexpr UnixTime::operator double() const {
  return utc_micros_;
}

constexpr uint64_t UnixTime::unixMicros() const {
  return utc_micros_;
}

UnixTime UnixTime::epoch() {
  return UnixTime(0);
}

constexpr Duration UnixTime::operator-(const UnixTime& other) const {
  return *this > other
      ? Duration(utc_micros_ - other.utc_micros_)
      : Duration(other.utc_micros_ - utc_micros_);
}

constexpr UnixTime UnixTime::operator+(const Duration& duration) const {
  return UnixTime(utc_micros_ + duration.microseconds());
}

constexpr UnixTime UnixTime::operator-(const Duration& duration) const {
  return UnixTime(utc_micros_ - duration.microseconds());
}

inline constexpr CivilTime::CivilTime() :
    year_(0),
    month_(0),
    day_(0),
    hour_(0),
    minute_(0),
    second_(0),
    millisecond_(0),
    offset_(0) {}

inline constexpr CivilTime::CivilTime(std::nullptr_t) :
    CivilTime() {
}

inline constexpr uint16_t CivilTime::year() const {
  return year_;
}

inline constexpr uint8_t CivilTime::month() const {
  return month_;
}

inline constexpr uint8_t CivilTime::day() const {
  return day_;
}

inline constexpr uint8_t CivilTime::hour() const {
  return hour_;
}

inline constexpr uint8_t CivilTime::minute() const {
  return minute_;
}

inline constexpr uint8_t CivilTime::second() const {
  return second_;
}

inline constexpr uint16_t CivilTime::millisecond() const {
  return millisecond_;
}

inline constexpr int32_t CivilTime::offset() const {
  return offset_;
}

inline constexpr Duration::Duration(ZeroType)
    : micros_(0) {}

inline constexpr Duration::Duration(uint64_t microseconds)
    : micros_(microseconds) {}

inline Duration::Duration(const struct ::timeval& value)
    : micros_(value.tv_sec + value.tv_usec * kMicrosPerSecond) {}

inline Duration::Duration(const struct ::timespec& value)
    : micros_(value.tv_sec + value.tv_nsec * kMicrosPerSecond / 1000) {}

constexpr bool Duration::operator==(const Duration& other) const {
  return micros_ == other.micros_;
}

constexpr bool Duration::operator!=(const Duration& other) const {
  return micros_ != other.micros_;
}

constexpr bool Duration::operator<(const Duration& other) const {
  return micros_ < other.micros_;
}

constexpr bool Duration::operator>(const Duration& other) const {
  return micros_ > other.micros_;
}

constexpr bool Duration::operator<=(const Duration& other) const {
  return micros_ <= other.micros_;
}

constexpr bool Duration::operator>=(const Duration& other) const {
  return micros_ >= other.micros_;
}

constexpr bool Duration::operator!() const {
  return micros_ == 0;
}

inline constexpr Duration::operator struct timeval() const {
#if defined(STX_OS_DARWIN)
  // OS/X plays in it's own universe. ;(
  return { static_cast<time_t>(micros_ / kMicrosPerSecond),
           static_cast<__darwin_suseconds_t>(micros_ % kMicrosPerSecond) };
#else
  return { static_cast<time_t>(micros_ / kMicrosPerSecond),
           static_cast<int>(micros_ % kMicrosPerSecond) };
#endif
}

inline constexpr Duration::operator struct timespec() const {
#if defined(STX_OS_DARWIN)
  // OS/X plays in it's own universe. ;(
  return { static_cast<time_t>(micros_ / kMicrosPerSecond),
           (static_cast<__darwin_suseconds_t>(micros_ % kMicrosPerSecond) * 1000) };
#else
  return { static_cast<time_t>(micros_ / kMicrosPerSecond),
           (static_cast<long>(micros_ % kMicrosPerSecond) * 1000) };
#endif
}

inline constexpr uint64_t Duration::microseconds() const noexcept {
  return micros_;
}

inline constexpr uint64_t Duration::milliseconds() const noexcept {
  return micros_ / kMillisPerSecond;
}

inline constexpr uint64_t Duration::seconds() const noexcept {
  return micros_ / kMicrosPerSecond;
}

inline constexpr Duration Duration::operator+(const Duration& other) const {
  return Duration(micros_ + other.micros_);
}

inline constexpr uint64_t Duration::minutes() const noexcept {
  return seconds() / kSecondsPerMinute;
}

inline constexpr uint64_t Duration::hours() const noexcept {
  return minutes() / kMinutesPerHour;
}

inline constexpr uint64_t Duration::days() const noexcept {
  return hours() / kHoursPerDay;
}

Duration Duration::fromDays(uint64_t v) {
  return Duration(v * kMicrosPerSecond * kSecondsPerDay);
}

Duration Duration::fromHours(uint64_t v) {
  return Duration(v * kMicrosPerSecond * kSecondsPerHour);
}

Duration Duration::fromMinutes(uint64_t v) {
  return Duration(v * kMicrosPerSecond * kSecondsPerMinute);
}

Duration Duration::fromSeconds(uint64_t v) {
  return Duration(v * kMicrosPerSecond);
}

Duration Duration::fromMilliseconds(uint64_t v) {
  return Duration(v * 1000);
}

Duration Duration::fromMicroseconds(uint64_t v) {
  return Duration(v);
}

Duration Duration::fromNanoseconds(uint64_t v) {
  return Duration(v / 1000);
}

