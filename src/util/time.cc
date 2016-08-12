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
#include <string>
#include <ctime>
#include <sys/time.h>
#include <sys/types.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif
#include "time.h"
#include "stringutil.h"
#include "logging.h"

UnixTime WallClock::now() {
  return UnixTime(WallClock::getUnixMicros());
}

uint64_t WallClock::unixSeconds() {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return tv.tv_sec;
}

uint64_t WallClock::getUnixMillis() {
  return unixMillis();
}

uint64_t WallClock::unixMillis() {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000llu + tv.tv_usec / 1000llu;
}

uint64_t WallClock::getUnixMicros() {
  return unixMicros();
}

uint64_t WallClock::unixMicros() {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000llu + tv.tv_usec;
}

uint64_t MonotonicClock::now() {
#ifdef __MACH__
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), SYSTEM_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  return std::uint64_t(mts.tv_sec) * 1000000 + mts.tv_nsec / 1000;
#else
  timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    logFatal("clock_gettime(CLOCK_MONOTONIC) failed");
    abort();
  } else {
    return std::uint64_t(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
  }
#endif
}

UnixTime::UnixTime() :
    utc_micros_(WallClock::unixMicros()) {}

UnixTime& UnixTime::operator=(const UnixTime& other) {
  utc_micros_ = other.utc_micros_;
  return *this;
}

UnixTime UnixTime::now() {
  return UnixTime(WallClock::unixMicros());
}

UnixTime UnixTime::daysFromNow(double days) {
  return UnixTime(WallClock::unixMicros() + (days * kMicrosPerDay));
}

std::string UnixTime::toString(const char* fmt) const {
  struct tm tm;
  time_t tt = utc_micros_ / 1000000;
  gmtime_r(&tt, &tm); // FIXPAUL

  char buf[256]; // FIXPAUL
  buf[0] = 0;
  strftime(buf, sizeof(buf), fmt, &tm);

  return std::string(buf);
}

template <>
std::string StringUtil::toString(UnixTime value) {
  return value.toString();
}

UnixTime std::numeric_limits<UnixTime>::min() {
  return UnixTime::epoch();
}

UnixTime std::numeric_limits<UnixTime>::max() {
  return UnixTime(std::numeric_limits<uint64_t>::max());
}

Option<CivilTime> CivilTime::parseString(
    const std::string& str,
    const char* fmt /* = "%Y-%m-%d %H:%M:%S" */) {
  return CivilTime::parseString(str.data(), str.size(), fmt);
}

Option<CivilTime> CivilTime::parseString(
    const char* str,
    size_t strlen,
    const char* fmt /* = "%Y-%m-%d %H:%M:%S" */) {
  struct tm t;

  // FIXME strptime doesn't handle time zone offsets
  if (strptime(str, fmt, &t) == NULL) {
    return None<CivilTime>();
  } else {
    CivilTime ct;
    ct.setSecond(t.tm_sec);
    ct.setMinute(t.tm_min);
    ct.setHour(t.tm_hour);
    ct.setDay(t.tm_mday);
    ct.setMonth(t.tm_mon + 1);
    ct.setYear(t.tm_year + 1900);
    return Some(ct);
  }
}

void CivilTime::setYear(uint16_t value) {
  year_ = value;
}

void CivilTime::setMonth(uint8_t value) {
  month_ = value;
}

void CivilTime::setDay(uint8_t value) {
  day_ = value;
}

void CivilTime::setHour(uint8_t value) {
  hour_ = value;
}

void CivilTime::setMinute(uint8_t value) {
  minute_ = value;
}

void CivilTime::setSecond(uint8_t value) {
  second_ = value;
}

void CivilTime::setMillisecond(uint16_t value) {
  millisecond_ = value;
}

void CivilTime::setOffset(int32_t value) {
  offset_ = value;
}

template<>
std::string StringUtil::toString<Duration>(Duration duration) {
  return toString(duration);
}

template<>
std::string StringUtil::toString<const Duration&>(const Duration& value) {
  unsigned years = value.days() / kDaysPerYear;
  unsigned days = value.days() % kDaysPerYear;;
  unsigned hours = value.hours() % kHoursPerDay;
  unsigned minutes = value.minutes() % kMinutesPerHour;
  unsigned seconds = value.seconds() % kSecondsPerMinute;
  unsigned msecs = value.milliseconds () % kMillisPerSecond;

  std::stringstream sstr;
  int i = 0;

  if (years)
    sstr << years << " years";

  if (days) {
    if (i++) sstr << ' ';
    sstr << days << " days";
  }

  if (hours) {
    if (i++) sstr << ' ';
    sstr << hours << " hours";
  }

  if (minutes) {
    if (i++) sstr << ' ';
    sstr << minutes << " minutes";
  }

  if (seconds) {
    if (i++) sstr << ' ';
    sstr << seconds << " seconds";
  }

  if (msecs) {
    if (i) sstr << ' ';
    sstr << msecs << "ms";
  }

  return sstr.str();
}

