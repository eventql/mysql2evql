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

  logInfo("mysql2evql", "Connecting to MySQL Server...");

  mysqlInit();
  auto mysql_conn = MySQLConnection::openConnection(URI(mysql_addr));

  logInfo(
      "mysql2evql",
      "Analyzing the input table. This might take a few minutes...");

  //auto schema = mysql_conn->getTableSchema(source_table);
  //logDebug("mysql2evql", "Table Schema:\n$0", schema->toString());
  std::vector<std::string> column_names;
  //for (const auto& field : schema->fields()) {
  //  column_names.emplace_back(field.name);
  //}

  ///* status line */
  std::atomic<size_t> num_rows_uploaded(0);
  SimpleRateLimitedFn status_line(kMicrosPerSecond, [&] () {
    logInfo(
        "mysql2evql",
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

  bool upload_done = false;
  bool upload_error = false;
  Queue<UploadShard> upload_queue(1);
  std::list<std::thread> upload_threads;
  for (size_t i = 0; i < num_upload_threads; ++i) {
    auto t = std::thread([&] {
      while (!upload_done) {
        auto shard = upload_queue.interruptiblePop();
        if (shard.isEmpty()) {
          continue;
        }

        bool success = false;
        for (size_t retry = 0; retry < max_retries; ++retry) {
          sleep(2 * retry);

          logDebug(
              "mysql2evql",
              "Uploading batch; target=$0:$1 size=$2MB",
              host,
              port,
              shard.get().data.size() / 1000000.0);

  //        try {
  //          http::HTTPClient http_client;
  //          auto upload_res = http_client.executeRequest(
  //              http::HTTPRequest::mkPost(
  //                  insert_uri,
  //                  "[" + shard.get().data.toString() + "]",
  //                  auth_headers));

  //          if (upload_res.statusCode() != 201) {
  //            logError(
  //                "mysql2evql", "[FATAL ERROR]: HTTP Status Code $0 $1",
  //                upload_res.statusCode(),
  //                upload_res.body().toString());

  //            if (upload_res.statusCode() == 403) {
  //              break;
  //            } else {
  //              continue;
  //            }
  //          }

  //          num_rows_uploaded += shard.get().nrows;
  //          status_line.runMaybe();
  //          success = true;
  //          break;
  //        } catch (const std::exception& e) {
  //          logError("mysql2evql", e, "error while uploading table data");
  //        }
        }

        if (!success) {
          upload_error = true;
        }
      }
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

      //if (shard.nrows > 1) {
      //  json.addComma();
      //}

      //json.beginObject();
      //json.addObjectEntry("database");
      //json.addString(db);
      //json.addComma();
      //json.addObjectEntry("table");
      //json.addString(destination_table);
      //json.addComma();
      //json.addObjectEntry("data");
      //json.beginObject();

      //for (size_t i = 0; i < column_names.size() && i < column_values.size(); ++i) {
      //  if (i > 0 ){
      //    json.addComma();
      //  }

      //  json.addObjectEntry(column_names[i]);
      //  json.addString(column_values[i]);
      //}

      //json.endObject();
      //json.endObject();

      if (shard.nrows == batch_size) {
        upload_queue.insert(shard, true);
        shard.data.clear();
        shard.nrows = 0;
      }

      status_line.runMaybe();
      return !upload_error;
    });
  } catch (const std::exception& e) {
    logError(
        "mysql2evql",
        std::string("error while executing mysql query: ") + e.what());

    upload_error = true;
  }

  if (!upload_error) {
    if (shard.nrows > 0) {
      upload_queue.insert(shard, true);
    }

    upload_queue.waitUntilEmpty();
  }

  upload_done = true;
  upload_queue.wakeup();
  for (auto& t : upload_threads) {
    t.join();
  }

  status_line.runForce();

  if (upload_error) {
    logInfo("mysql2evql", "Upload finished with errors");
    return false;
  } else {
    logInfo("mysql2evql", "Upload finished successfully :)");
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
      NULL);

  flags.defineFlag(
      "port",
      FlagParser::T_INTEGER,
      false,
      "p",
      NULL);

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

  try {
    if (run(flags)) {
      return 0;
    } else {
      return 1;
    }
  } catch (const std::exception& e) {
    logFatal("mysql2evql", "$0", e.what());
    return 1;
  }
}
