/**
 * Copyright (c) 2016 DeepCortex GmbH <legal@eventql.io>
 * Authors:
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
#include <stdlib.h>
#include <stdint.h>
#include <string>
#include "time.h"

class SimpleRateLimit {
public:
  SimpleRateLimit(const Duration& period);
  bool check();
protected:
  uint64_t period_micros_;
  uint64_t last_micros_;
};

class SimpleRateLimitedFn {
public:
  SimpleRateLimitedFn(const Duration& period, std::function<void ()> fn);
  void runMaybe();
  void runForce();
protected:
  SimpleRateLimit limit_;
  std::function<void ()> fn_;
};

