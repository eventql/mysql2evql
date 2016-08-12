/**
 * Copyright (c) 2016 DeepCortex GmbH <legal@eventql.io>
 * Authors:
 *   - Laura Schlimmer <laura@eventql.io>
 *   - Christian Parpart <christianparpart@gmail.com>
 *   - Paul Asmuth <paul@eventql.io>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License ("the license") as
 * published by the Free Software Foundation, either version 3 of the License,
 * or any later version.
 *
 * In accordance with Section 7(e) of the license, the licensing of the Program
 * under the license does not imply a trademark license. Therefore any rights,
 * title and interest in our trademarks remain entirely with us.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the license for more details.
 *
 * You can be released from the requirements of the license by purchasing a
 * commercial license. Buying such a license is mandatory as soon as you develop
 * commercial activities involving this program without disclosing the source
 * code of your own applications
 */
#pragma once
#include <ctime>
#include <inttypes.h>
#include <limits>
#include <string>
#include "option.h"

constexpr const uint64_t kMicrosPerMilli = 1000;
constexpr const uint64_t kMicrosPerSecond = 1000000;
constexpr const uint64_t kMillisPerSecond = 1000;
constexpr const uint64_t kSecondsPerMinute = 60;
constexpr const uint64_t kMiillisPerMinute = kSecondsPerMinute * kMillisPerSecond;
constexpr const uint64_t kMicrosPerMinute = kSecondsPerMinute * kMicrosPerSecond;
constexpr const uint64_t kMinutesPerHour = 60;
constexpr const uint64_t kSecondsPerHour = kSecondsPerMinute * kMinutesPerHour;
constexpr const uint64_t kMillisPerHour = kSecondsPerHour * kMillisPerSecond;
constexpr const uint64_t kMicrosPerHour = kSecondsPerHour * kMicrosPerSecond;
constexpr const uint64_t kHoursPerDay = 24;
constexpr const uint64_t kSecondsPerDay = kSecondsPerHour * kHoursPerDay;
constexpr const uint64_t kMillisPerDay = kSecondsPerDay * kMillisPerSecond;
constexpr const uint64_t kMicrosPerDay = kSecondsPerDay * kMicrosPerSecond;
constexpr const uint64_t kDaysPerWeek = 7;
constexpr const uint64_t kSecondsPerWeek = kSecondsPerDay * kDaysPerWeek;
constexpr const uint64_t kMillisPerWeek = kSecondsPerWeek * kMicrosPerSecond;
constexpr const uint64_t kMicrosPerWeek = kSecondsPerWeek * kMicrosPerSecond;
constexpr const uint64_t kDaysPerYear = 365;
constexpr const uint64_t kSecondsPerYear = kDaysPerYear * kSecondsPerDay;
constexpr const uint64_t kMillisPerYear = kDaysPerYear * kMillisPerDay;
constexpr const uint64_t kMicrosPerYear = kDaysPerYear * kMicrosPerDay;

class UnixTime;
class CivilTime;
class Duration;

class WallClock {
public:
  static UnixTime now();
  static uint64_t unixSeconds();
  static uint64_t getUnixMillis();
  static uint64_t unixMillis();
  static uint64_t getUnixMicros();
  static uint64_t unixMicros();
};

class MonotonicClock {
public:
  static uint64_t now();
};

class UnixTime {
public:

  /**
   * Create a new UTC UnixTime instance with time = now
   */
  UnixTime();

  /**
   * Create a new UTC UnixTime instance
   *
   * @param timestamp the UTC microsecond timestamp
   */
  constexpr UnixTime(uint64_t utc_time);

  /**
   * Create a new UTC UnixTime instance from a civil time reference
   *
   * @param civil the civil time
   */
  UnixTime(const CivilTime& civil);

  /**
   * Return a representation of the date as a string (strftime)
   *
   * @param fmt the strftime format string (optional)
   */
  std::string toString(const char* fmt = "%Y-%m-%d %H:%M:%S") const;

  UnixTime& operator=(const UnixTime& other);

  constexpr bool operator==(const UnixTime& other) const;
  constexpr bool operator!=(const UnixTime& other) const;
  constexpr bool operator<(const UnixTime& other) const;
  constexpr bool operator>(const UnixTime& other) const;
  constexpr bool operator<=(const UnixTime& other) const;
  constexpr bool operator>=(const UnixTime& other) const;

  /**
   * Calculates the duration between this time and the other.
   *
   * @param other the other time to calculate the difference from.
   *
   * @return the difference between this and the other time.
   */
  constexpr Duration operator-(const UnixTime& other) const;

  constexpr UnixTime operator+(const Duration& duration) const;
  constexpr UnixTime operator-(const Duration& duration) const;

  /**
   * Cast the UnixTime object to a UTC unix microsecond timestamp represented as
   * an uint64_t
   */
  constexpr explicit operator uint64_t() const;

  /**
   * Cast the UnixTime object to a UTC unix microsecond timestamp represented as
   * a double
   */
  constexpr explicit operator double() const;

  /**
   * Return the represented date/time as a UTC unix microsecond timestamp
   */
  constexpr uint64_t unixMicros() const;

  /**
   * Return a new UnixTime instance with time 00:00:00 UTC, 1 Jan. 1970
   */
  static inline UnixTime epoch();

  /**
   * Return a new UnixTime instance with time = now
   */
  static inline UnixTime now();

  /**
   * Return a new UnixTime instance with time = now + days
   */
  static inline UnixTime daysFromNow(double days);

protected:

  /**
   * The utc microsecond timestamp of the represented moment in time
   */
  uint64_t utc_micros_;
};

namespace std {
template <> class numeric_limits<UnixTime> {
public:
  static UnixTime max();
  static UnixTime min();
};
}

/**
 * Class representing an instance of time in the gregorian calendar
 */
class CivilTime {
public:

  /**
   * Create a new CivilTime instance with all fields set to zero
   */
  constexpr CivilTime();

  /**
   * Create a new CivilTime instance with all fields set to zero
   */
  constexpr CivilTime(std::nullptr_t);

  /**
   * Parse time from the provided string
   *
   * @param str the string to parse
   * @param fmt the strftime format string (optional)
   */
  static Option<CivilTime> parseString(
      const std::string& str,
      const char* fmt = "%Y-%m-%d %H:%M:%S");

  /**
   * Parse time from the provided string
   *
   * @param str the string to parse
   * @param strlen the size of the string to parse
   * @param fmt the strftime format string (optional)
   */
  static Option<CivilTime> parseString(
      const char* str,
      size_t strlen,
      const char* fmt = "%Y-%m-%d %H:%M:%S");

  /**
   * Year including century / A.D. (eg. 1999)
   */
  constexpr uint16_t year() const;

  /**
   * Month [1-12]
   */
  constexpr uint8_t month() const;

  /**
   * Day of the month [1-31]
   */
  constexpr uint8_t day() const;

  /**
   * Hour [0-23]
   */
  constexpr uint8_t hour() const;

  /**
   * Hour [0-59]
   */
  constexpr uint8_t minute() const;

  /**
   * Second [0-60]
   */
  constexpr uint8_t second() const;

  /**
   * Millisecond [0-999]
   */
  constexpr uint16_t millisecond() const;

  /**
   * Timezone offset to UTC in seconds
   */
  constexpr int32_t offset() const;

  void setYear(uint16_t value);
  void setMonth(uint8_t value);
  void setDay(uint8_t value);
  void setHour(uint8_t value);
  void setMinute(uint8_t value);
  void setSecond(uint8_t value);
  void setMillisecond(uint16_t value);
  void setOffset(int32_t value);

protected:
  uint16_t year_;
  uint8_t month_;
  uint8_t day_;
  uint8_t hour_;
  uint8_t minute_;
  uint8_t second_;
  uint16_t millisecond_;
  int32_t offset_;
};


class Duration {
private:
  enum class ZeroType { Zero };

public:
  constexpr static ZeroType Zero = ZeroType::Zero;

  /**
   * Creates a new Duration of zero microseconds.
   */
  constexpr Duration(ZeroType);

  /**
   * Create a new Duration
   *
   * @param microseconds the duration in microseconds
   */
  constexpr Duration(uint64_t microseconds);

  /**
   * Creates a new Duration out of a @c timeval struct.
   *
   * @param value duration as @c timeval.
   */
  Duration(const struct ::timeval& value);

  /**
   * Creates a new Duration out of a @c timespec struct.
   *
   * @param value duration as @c timespec.
   */
  Duration(const struct ::timespec& value);

  constexpr bool operator==(const Duration& other) const;
  constexpr bool operator!=(const Duration& other) const;
  constexpr bool operator<(const Duration& other) const;
  constexpr bool operator>(const Duration& other) const;
  constexpr bool operator<=(const Duration& other) const;
  constexpr bool operator>=(const Duration& other) const;
  constexpr bool operator!() const;

  constexpr Duration operator+(const Duration& other) const;

  constexpr operator struct timeval() const;
  constexpr operator struct timespec() const;

  /**
   * Return the represented duration in microseconds
   */
  constexpr explicit operator uint64_t() const;

  /**
   * Return the represented duration in microseconds
   */
  constexpr explicit operator double() const;

  /**
   * Return the represented duration in microseconds
   */
  constexpr uint64_t microseconds() const noexcept;

  /**
   * Return the represented duration in milliseconds
   */
  constexpr uint64_t milliseconds() const noexcept;

  /**
   * Return the represented duration in seconds
   */
  constexpr uint64_t seconds() const noexcept;

  constexpr uint64_t minutes() const noexcept;
  constexpr uint64_t hours() const noexcept;
  constexpr uint64_t days() const noexcept;

  static inline Duration fromDays(uint64_t v);
  static inline Duration fromHours(uint64_t v);
  static inline Duration fromMinutes(uint64_t v);
  static inline Duration fromSeconds(uint64_t v);
  static inline Duration fromMilliseconds(uint64_t v);
  static inline Duration fromMicroseconds(uint64_t v);
  static inline Duration fromNanoseconds(uint64_t v);

protected:
  uint64_t micros_;
};

#include "time_impl.h"
