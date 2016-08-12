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
#include <memory>
#include <list>
#include <functional>
#include <mysql.h>
#include "mysql.h"
#include "uri.h"

void mysqlInit();

class MySQLConnection {
public:

  /**
   * Creates a new mysql connection and tries to connect to the provided URI
   * May throw an exception
   *
   * Example URIs:
   *    mysql://localhost/test_database?user=root
   *    mysql://somehost:3306/my_database?user=my_user&password=my_secret
   *
   * @param URI the mysql:// URI
   * @returns a new MySQLConnection
   */
  static std::unique_ptr<MySQLConnection> openConnection(const URI& uri);

  /**
   * Creates a new mysql connection and tries to connect
   * May throw an exception
   *
   * @returns a new MySQLConnection
   */
  static std::unique_ptr<MySQLConnection> openConnection(
      const std::string& host,
      unsigned int port,
      const std::string& database,
      const std::string& username,
      const std::string& password);

  /**
   * Create a new mysql connection
   */
  MySQLConnection();

  /**
   * Destroy a mysql connection. Closes the connection
   */
  ~MySQLConnection();

  /**
   * Connect to a mysql server on the specified URI. May throw an exception
   *
   * Example URIs:
   *    mysql://localhost/test_database?user=root
   *    mysql://somehost:3306/my_database?user=my_user&password=my_secret
   *
   * @param URI the mysql:// URI
   * @returns a new MySQLConnection
   */
  void connect(const URI& uri);

  /**
   * Connect to a mysql server. May throw an exception
   *
   * @param host the mysql server hostname
   * @param port the mysql server port
   * @param database the mysql database to open or empty string
   * @param username the mysql user username or empty string
   * @param password the mysq user password or empty string
   * @returns a new MySQLConnection
   */
  void connect(
      const std::string& host,
      unsigned int port,
      const std::string& database,
      const std::string& username,
      const std::string& password);

  /**
   * Returns a list of all column names for the provided table name. May
   * throw an exception (This does the equivalent to a DESCRIBE TABLE)
   *
   * @param table_name the name of the table do describe
   * @returns a list of all columns names of the table
   */
  std::vector<std::string> describeTable(const std::string& table_name);

  /**
   * Execute a mysql query. The mysql query string must not include a terminal
   * semicolon.
   *
   * The provided row callback will be called for every row in the result set.
   * The row callback must return a boolean value; if it returns true it will
   * be called again for the next row in the result set (if a next row exists),
   * if it returns false it will not be called again and the remainder of the
   * result set will be discarded.
   *
   * This method may throw an exception.
   *
   * @param query the mysql query string without a terminal semicolon
   * @param row_callback the callback that should be called for every result row
   */
  void executeQuery(
      const std::string& query,
      std::function<bool (const std::vector<std::string>&)> row_callback);

  /**
   * Execute a mysql query. The mysql query string must not include a terminal
   * semicolon.
   *
   * The provided row callback will be called for every row in the result set.
   * The row callback must return a boolean value; if it returns true it will
   * be called again for the next row in the result set (if a next row exists),
   * if it returns false it will not be called again and the remainder of the
   * result set will be discarded.
   *
   * This method may throw an exception.
   *
   * @param query the mysql query string without a terminal semicolon
   * @param row_callback the callback that should be called for every result row
   */
  std::list<std::vector<std::string>> executeQuery(const std::string& query);

protected:
  MYSQL* mysql_;
};

