/**
 * Copyright (c) 2016 DeepCortex GmbH <legal@eventql.io>
 * Authors:
 *   - Paul Asmuth <paul@eventql.io>
 *   - Laura Schlimmer <laura@eventql.io>
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
#include <unistd.h>
#include <iostream>
#include <thread>
#include <curl/curl.h>
#include "util/return_code.h"
#include "util/flagparser.h"
#include "util/logging.h"
#include "util/mysql.h"
#include "util/queue.h"
#include "util/rate_limit.h"

struct UploadShard {
  std::string data;
  size_t nrows;
};

bool run(const FlagParser& flags) {
  auto source_table = flags.getString("source_table");
  auto destination_table = flags.getString("destination_table");
  auto batch_size = flags.getInt("batch_size");
  auto num_upload_threads = flags.getInt("upload_threads");
  auto mysql_addr = flags.getString("mysql");
  auto host = flags.getString("host");
  auto port = flags.getInt("port");
  auto db = flags.getString("database");
  auto max_retries = flags.getInt("max_retries");

  logInfo("Connecting to MySQL Server...");

  mysqlInit();
  auto mysql_conn = MySQLConnection::openConnection(URI(mysql_addr));

  logInfo(
      "Analyzing the input table. This might take a few minutes...");

  auto column_names = mysql_conn->describeTable(source_table);
  logDebug("Table Columns: $0", StringUtil::join(column_names, ", "));

  /* status line */
  std::atomic<size_t> num_rows_uploaded(0);
  SimpleRateLimitedFn status_line(kMicrosPerSecond, [&] () {
    logInfo(
        "Uploading... $0 rows",
        num_rows_uploaded.load());
  });

  ///* start upload threads */
  //http::HTTPMessage::HeaderList auth_headers;
  //if (flags.isSet("auth_token")) {
  //  auth_headers.emplace_back(
  //      "Authorization",
  //      StringUtil::format("Token $0", flags.getString("auth_token")));
  ////} else if (!cfg_.getPassword().isEmpty()) {
  ////  auth_headers.emplace_back(
  ////      "Authorization",
  ////      StringUtil::format("Basic $0",
  ////          util::Base64::encode(
  ////              cfg_.getUser() + ":" + cfg_.getPassword().get())));
  //}

  auto insert_uri = StringUtil::format(
      "http://$0:$1/api/v1/tables/insert",
      host,
      port);

  std::mutex upload_mutex;
  bool upload_done = false;
  std::atomic<bool> upload_error(false);
  Queue<UploadShard> upload_queue(1);
  std::list<std::thread> upload_threads;
  for (size_t i = 0; i < num_upload_threads; ++i) {
    auto t = std::thread([&] {
      auto curl = curl_easy_init();
      if (!curl) {
        logError("curl_init() failed");
        std::unique_lock<std::mutex> lk(upload_mutex);
        upload_error = true;
        lk.unlock();
        upload_queue.wakeup();
        return;
      }

      for (;;) {
        {
          std::unique_lock<std::mutex> lk(upload_mutex);
          if (upload_done || upload_error) {
            break;
          }
        }

        auto shard = upload_queue.interruptiblePop();
        if (shard.isEmpty()) {
          continue;
        }

        logDebug(
            "Uploading batch; target=$0:$1 size=$2KB",
            host,
            port,
            shard.get().data.size() / double(1000.0));

        bool success = false;
        for (size_t retry = 0; retry < max_retries; ++retry) {
          sleep(std::min(retry, 5lu));

          auto http_url = StringUtil::format(
              "http://$0:$1/api/v1/tables/insert",
              host,
              port);

          logInfo(http_url);
          std::string http_body = "[" + shard.get().data + "]";

          struct curl_slist* req_headers = NULL;
          req_headers = curl_slist_append(
              req_headers,
              "Content-Type: application/json; charset=utf-8");

          if (flags.isSet("auth_token")) {
            auto hdr = "Authorization: Token " + flags.getString("auth_token");
            req_headers = curl_slist_append(req_headers, hdr.c_str());
          }

          //if (!username_.empty() || !password_.empty()) {
          //  std::string hdr = "Authorization: Basic ";
          //  hdr += Base64::encode(username_ + ":" + password_);
          //  req_headers = curl_slist_append(req_headers, hdr.c_str());
          //}

          curl_easy_setopt(curl, CURLOPT_URL, http_url.c_str());
          curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000);
          curl_easy_setopt(curl, CURLOPT_POSTFIELDS, http_body.c_str());
          curl_easy_setopt(curl, CURLOPT_HTTPHEADER, req_headers);
          CURLcode curl_res = curl_easy_perform(curl);
          curl_slist_free_all(req_headers);
          if (curl_res != CURLE_OK) {
            logError("http request failed: $0", curl_easy_strerror(curl_res));
            continue;
          }

          long http_res_code = 0;
          curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_res_code);

          switch (http_res_code) {
            case 201:
              success = true;
              break;
            default:
              logError("http error: $0", http_res_code);
              continue;
          }
        }

        if (!success) {
          std::unique_lock<std::mutex> lk(upload_mutex);
          upload_error = true;
          lk.unlock();
          upload_queue.wakeup();
        }
      }

      curl_easy_cleanup(curl); 
    });

    upload_threads.emplace_back(std::move(t));
  }

  /* fetch rows from mysql */
  UploadShard shard;
  shard.nrows = 0;

  std::string where_expr;
  if (flags.isSet("filter")) {
    where_expr = "WHERE " + flags.getString("filter");
  }

  try {
    auto get_rows_qry = StringUtil::format(
        "SELECT * FROM `$0` $1;",
       source_table,
       where_expr);

    mysql_conn->executeQuery(
        get_rows_qry,
        [&] (const std::vector<std::string>& column_values) -> bool {
      ++shard.nrows;

      if (shard.nrows > 1) {
        shard.data += ",";
      }

      std::vector<std::string> fields;
      for (size_t i = 0; i < column_names.size() && i < column_values.size(); ++i) {
        fields.emplace_back(StringUtil::format(
            R"("$0": "$1")",
            StringUtil::jsonEscape(column_names[i]),
            StringUtil::jsonEscape(column_values[i])));
      }

      shard.data += StringUtil::format(
          R"({"database": "$0", "table": "$1", "data": {$2}})",
          StringUtil::jsonEscape(db),
          StringUtil::jsonEscape(destination_table),
          StringUtil::join(fields, ","));

      if (shard.nrows == batch_size) {
        upload_queue.insert(shard, true);
        num_rows_uploaded += shard.nrows;
        shard.data.clear();
        shard.nrows = 0;
        status_line.runMaybe();
      }

      return !upload_error;
    });
  } catch (const std::exception& e) {
    logError(
        std::string("error while executing mysql query: ") + e.what());

    std::unique_lock<std::mutex> lk(upload_mutex);
    upload_error = true;
    lk.unlock();
    upload_queue.wakeup();
  }

  if (!upload_error) {
    if (shard.nrows > 0) {
      upload_queue.insert(shard, true);
      num_rows_uploaded += shard.nrows;
      status_line.runMaybe();
    }

    upload_queue.waitUntilEmpty();
  }

  {
    std::unique_lock<std::mutex> lk(upload_mutex);
    upload_done = true;
  }

  upload_queue.wakeup();
  for (auto& t : upload_threads) {
    t.join();
  }

  status_line.runForce();

  if (upload_error) {
    logInfo("Upload finished with errors");
    return false;
  } else {
    logInfo("Upload finished successfully :)");
    return true;
  }
}

int main(int argc, const char** argv) {
  FlagParser flags;

  flags.defineFlag(
      "help",
      FlagParser::T_SWITCH,
      false,
      "?",
      NULL);

  flags.defineFlag(
      "version",
      FlagParser::T_SWITCH,
      false,
      "V",
      NULL);

  flags.defineFlag(
      "loglevel",
      FlagParser::T_STRING,
      false,
      NULL,
      "INFO");

  flags.defineFlag(
      "source_table",
      FlagParser::T_STRING,
      false,
      "t",
      NULL);

  flags.defineFlag(
      "destination_table",
      FlagParser::T_STRING,
      false,
      "t",
      NULL);

  flags.defineFlag(
      "host",
      FlagParser::T_STRING,
      false,
      "h",
      "localhost");

  flags.defineFlag(
      "port",
      FlagParser::T_INTEGER,
      false,
      "p",
      "9175");

  flags.defineFlag(
      "database",
      FlagParser::T_STRING,
      false,
      NULL,
      NULL);

  flags.defineFlag(
      "auth_token",
      FlagParser::T_STRING,
      false,
      NULL,
      NULL);

  flags.defineFlag(
      "mysql",
      FlagParser::T_STRING,
      false,
      "x",
      "mysql://localhost:3306/mydb?user=root");

  flags.defineFlag(
      "filter",
      FlagParser::T_STRING,
      false,
      NULL,
      NULL);

  flags.defineFlag(
      "batch_size",
      FlagParser::T_INTEGER,
      false,
      NULL,
      "128");

  flags.defineFlag(
      "upload_threads",
      FlagParser::T_INTEGER,
      false,
      NULL,
      "8");

  flags.defineFlag(
      "max_retries",
      FlagParser::T_INTEGER,
      false,
      NULL,
      "20");

  /* parse flags */
  {
    auto rc = flags.parseArgv(argc, argv);
    if (!rc.isSuccess()) {
      std::cerr << rc.getMessage() << "\n";
      return 1;
    }
  }

  /* setup logging */
  if (!flags.isSet("nolog_to_stderr") && !flags.isSet("daemonize")) {
    Logger::logToStderr("mysql2evql");
  }

  if (flags.isSet("log_to_syslog")) {
    Logger::logToSyslog("mysql2evql");
  }

  Logger::get()->setMinimumLogLevel(
      strToLogLevel(flags.getString("loglevel")));

  /* print help */
  if (flags.isSet("help") || flags.isSet("version")) {
    std::cerr <<
        StringUtil::format(
            "mysql2evql $0\n"
            "Copyright (c) 2016, DeepCortex GmbH. All rights reserved.\n\n",
            MYSQL2EVQL_VERSION);
  }

  if (flags.isSet("version")) {
    return 0;
  }

  if (flags.isSet("help")) {
    std::cerr <<
        "Usage: $ mysql2evql [OPTIONS]\n\n"
        "   --source_table <name>     \n"
        "   --destination_table <name>     \n"
        "   --host <name>     \n"
        "   --port <name>     \n"
        "   --auth_token <name>     \n"
        "   --database <name>     \n"
        "   --mysql <name>     \n"
        "   --filter <name>     \n"
        "   --batch_size <name>     \n"
        "   --upload_threads <name>     \n"
        "   --max_retries <name>     \n"
        "   --loglevel <level>        Minimum log level (default: INFO)\n"
        "   --[no]log_to_syslog       Do[n't] log to syslog\n"
        "   --[no]log_to_stderr       Do[n't] log to stderr\n"
        "   -?, --help                Display this help text and exit\n"
        "   -v, --version             Display the version of this binary and exit\n"
        "                                                        \n"
        "Examples:                                               \n"
        "   $ mysql2evql \\\n"
        "       --source_table src_tbl \\\n"
        "       --destination_table target_tbl \\\n"
        "       --host localhost \\\n"
        "       --port 9175  \\\n"
        "       --database target_db \\\n"
        "       --mysql \"mysql://localhost:3306/mydb?user=root\"\n";

    return 0;
  }

  int rc = 0;
  curl_global_init(CURL_GLOBAL_DEFAULT);

  try {
    if (run(flags)) {
      rc = 0;
    } else {
      rc = 1;
    }
  } catch (const std::exception& e) {
    logFatal("$0", e.what());
    rc = 1;
  }

  curl_global_cleanup();
  return rc;
}

