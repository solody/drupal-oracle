<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\IntegrityConstraintViolationException;
use Drupal\Core\Database\Database;
use Drupal\Core\Database\Connection as DatabaseConnection;
use Drupal\Core\Database\Log;
use Drupal\Core\Database\StatementInterface;
use Drupal\Core\Database\StatementWrapper;

/**
 * Used to replace '' character in queries.
 */
define('ORACLE_EMPTY_STRING_REPLACER', '^');

/**
 * Maximum oracle identifier length (e.g. table names cannot exceed the length).
 *
 * @TODO: make dynamic. 30 is a limit for v11. In OD12+ has new limit of 128.
 * Current value can be get by `DESCRIBE all_tab_columns`.
 */
define('ORACLE_IDENTIFIER_MAX_LENGTH', 128);

/**
 * Prefix used for long identifier keys.
 */
define('ORACLE_LONG_IDENTIFIER_PREFIX', 'L#');

/**
 * Prefix used for BLOB values.
 */
define('ORACLE_BLOB_PREFIX', 'B^#');

/**
 * Maximum length (in bytes) for a string value in a table column in oracle.
 *
 * Affects schema.inc table creation.
 */
define('ORACLE_MAX_VARCHAR2_LENGTH', 1332);

/**
 * Maximum length of a string that PDO_OCI can handle.
 *
 * Affects runtime blob creation.
 */
define('ORACLE_MIN_PDO_BIND_LENGTH', 1332);

/**
 * Alias used for queryRange filtering (we have to remove that from resultsets).
 */
define('ORACLE_ROWNUM_ALIAS', 'RWN_TO_REMOVE');

/**
 * @addtogroup database
 * @{
 */

/**
 * Oracle implementation of \Drupal\Core\Database\Connection.
 */
class Connection extends DatabaseConnection {

  /**
   * Error code for "Unknown database" error.
   */
  const DATABASE_NOT_FOUND = 0;

  /**
   * We are being use to connect to an external oracle database.
   *
   * @var bool
   */
  public $external = FALSE;

  /**
   * {@inheritdoc}
   */
  protected $statementClass = NULL;

  /**
   * {@inheritdoc}
   */
  protected $statementWrapperClass = StatementWrapper::class;

  /**
   * @todo proper description.
   */
  private $maxVarchar2Size = ORACLE_MIN_PDO_BIND_LENGTH;

  /**
   * {@inheritdoc}
   */
  public function __construct(\PDO $connection, array $connection_options = array()) {
    parent::__construct($connection, $connection_options);

    // This driver defaults to transaction support, except if explicitly
    // passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle.
    $this->transactionalDDLSupport = FALSE;

    // Setup session attributes.
    try {
      $stmt = $connection->prepare("begin ? := setup_session; end;");
      $stmt->bindParam(1, $this->maxVarchar2Size, \PDO::PARAM_INT | \PDO::PARAM_INPUT_OUTPUT, 32);
      $stmt->execute();
    }
    catch (\Exception $ex) {
      // Ignore at install time or external databases.
      // Fallback to minimum bind size.
      $this->maxVarchar2Size = ORACLE_MIN_PDO_BIND_LENGTH;

      // Connected to an external oracle database (not necessarily a drupal
      // schema).
      $this->external = TRUE;
    }

    // Ensure all used Oracle prefixes (users schemas) exists.
    foreach ($this->prefixes as $table_name => $prefix) {
      if (!empty($prefix)) {
        // This will create the user if not exists.
        // @todo: clean up Simpletest TEST% users.

        // Allow ORA-00904 ("invalid identifier error") and ORA-06575 ("package
        // or function is in an invalid state") and during the installation
        // process (before the 'identifier' package were created).
        $this->prefixes[$table_name] = $this
          ->query('SELECT identifier.check_db_prefix(?) FROM dual', [$prefix], [
            'oracle_exceptions_allowed' => ['06575', '00904'],
            ]);
        if ($this->prefixes[$table_name]) {
          $this->prefixes[$table_name] = $this->prefixes[$table_name]->fetchColumn();
        }
      }
    }
  }

  /**
   * {@inheritdoc}
   */
  public static function open(array &$connection_options = array()) {

    /**
     * Here is full possible TNS description (with master/slave).
     * @todo: implement all options?
     *
     * (DESCRIPTION_LIST=
     *   (LOAD_BALANCE=off)
     *   (FAILOVER=on)
     *   (DESCRIPTION=
     *     (ADDRESS=(PROTOCOL=TCPS)(HOST=<master_host>)(PORT=<master_port>))
     *     (CONNECT_DATA=(SERVICE_NAME=<master_service_name>))
     *     (SECURITY= (SSL_SERVER_CERT_DN="for example cn=sales,cn=OracleContext,dc=us,dc=acme,dc=com"))
     *   )
     *   (DESCRIPTION=
     *     (ADDRESS=(PROTOCOL=TCPS)(HOST=<slave_host>)(PORT=<slave_port>))
     *     (CONNECT_DATA=(SERVICE_NAME=<master_service_name>))
     *     (SECURITY=(SSL_SERVER_CERT_DN="for example cn=sales,cn=OracleContext,dc=us,dc=acme,dc=com"))
     *   )
     * )
     */

    if ($connection_options['host'] === 'USETNS') {
      // Use database as TNSNAME.
      $dsn = 'oci:dbname=' . $connection_options['database'] . ';charset=AL32UTF8';
    }
    else {
      // Use host/port/database.
      $tns = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {$connection_options['host']})(PORT = {$connection_options['port']})) (CONNECT_DATA = (SERVICE_NAME = {$connection_options['database']}) (SID = {$connection_options['database']})))";
      $dsn = "oci:dbname={$tns};charset=AL32UTF8";
    }

    // Allow PDO options to be overridden.
    $connection_options += array(
      'pdo' => array(),
    );

    $connection_options['pdo'] += array(
      \PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      \PDO::ATTR_CASE => \PDO::CASE_LOWER,
      \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    );

    $pdo = new \PDO($dsn, $connection_options['username'], $connection_options['password'], $connection_options['pdo']);

    return $pdo;
  }

  /**
   * {@inheritdoc}
   */
  protected function setPrefix($prefix) {
    if (is_array($prefix)) {
      $this->prefixes = $prefix + ['default' => ''];
    }
    else {
      $this->prefixes = ['default' => strtoupper($prefix)];
    }

    // Set up variables for use in prefixTables(). Replace table-specific
    // prefixes first.
    $this->prefixSearch = [];
    $this->prefixReplace = [];
    foreach ($this->prefixes as $table_name => $prefix) {
      if ($table_name !== 'default') {

        // Set up a map of prefixed => un-prefixed tables.
        $prefixed = $this->schema()->oid($prefix) . '.' . $this->schema()->oid($table_name);
        $this->unprefixedTablesMap[$prefixed] = $table_name;

        // Add replacements.
        $this->prefixSearch[] = '{' . $table_name . '}';
        $this->prefixReplace[] = $prefixed;
      }
    }

    // Ensure we do not have double quoted tables.
    $this->prefixSearch[] = '"{';
    $this->prefixSearch[] = '}"';
    $this->prefixSearch[] = '{';
    $this->prefixSearch[] = '}';

    if ($this->prefixes['default']) {
      $this->prefixReplace[] = '{';
      $this->prefixReplace[] = '}';
      $this->prefixReplace[] = $this->schema()->oid($this->prefixes['default']) . '."';
      $this->prefixReplace[] = '"';
    }
    else {
      $this->prefixReplace[] = '{';
      $this->prefixReplace[] = '}';
      $this->prefixReplace[] = '"';
      $this->prefixReplace[] = '"';
    }
  }

  /**
   * {@inheritdoc}
   */
  public function query($query, array $args = [], $options = []) {
    // Use default values if not already set.
    $options += $this->defaultOptions();
    assert(!isset($options['target']), 'Passing "target" option to query() has no effect. See https://www.drupal.org/node/2993033');

    try {

      // We allow either a pre-bound statement object (deprecated) or a literal
      // string. In either case, we want to end up with an executed statement
      // object, which we pass to StatementInterface::execute.
      if (is_string($query)) {
        $this->expandArguments($query, $args);

        // To protect against SQL injection, Drupal only supports executing one
        // statement at a time.  Thus, the presence of a SQL delimiter (the
        // semicolon) is not allowed unless the option is set.  Allowing
        // semicolons should only be needed for special cases like defining a
        // function or stored procedure in SQL.
        // @see https://www.drupal.org/project/drupal/issues/2489672
        if (empty($options['allow_delimiter_in_query'])) {
          $query = rtrim($query, ";  \t\n\r\0\x0B");
          if (strpos($query, ';') !== FALSE) {
            throw new \InvalidArgumentException('; is not supported in SQL strings. Use only one statement at a time.');
          }
        }

        $stmt = $this->prepareStatement($query, $options);
      }
      elseif ($query instanceof StatementInterface) {
        @trigger_error('Passing a StatementInterface object as a $query argument to ' . __METHOD__ . ' is deprecated in drupal:9.2.0 and is removed in drupal:10.0.0. Call the execute method from the StatementInterface object directly instead. See https://www.drupal.org/node/3154439', E_USER_DEPRECATED);
        $stmt = $query;
      }
      elseif ($query instanceof \PDOStatement) {
        @trigger_error('Passing a \\PDOStatement object as a $query argument to ' . __METHOD__ . ' is deprecated in drupal:9.2.0 and is removed in drupal:10.0.0. Call the execute method from the StatementInterface object directly instead. See https://www.drupal.org/node/3154439', E_USER_DEPRECATED);
        $stmt = $query;
      }

      if (is_string($query)) {
        $stmt->execute($this->cleanupArgs($args), $options);
      }
      elseif ($query instanceof StatementInterface) {
        $stmt->execute(NULL, $options);
      }
      elseif ($query instanceof \PDOStatement) {
        $stmt->execute();
      }

      switch ($options['return']) {
        case Database::RETURN_STATEMENT:
          return $stmt;

        case Database::RETURN_AFFECTED:
          $stmt->allowRowCount = TRUE;
          return $stmt->rowCount();

        case Database::RETURN_INSERT_ID:
          return (isset($options['sequence_name']) ? $this->lastInsertId($options['sequence_name']) : FALSE);

        case Database::RETURN_NULL:
          return NULL;

        default:
          throw new \PDOException('Invalid return directive: ' . $options['return']);
      }
    }
    catch (\InvalidArgumentException $exception) {
      throw $exception;
    }
    catch (\Exception $e) {
      if (isset($options['throw_exception'])) {
        $message = implode([
          ($query instanceof \PDOStatement) ? $stmt->queryString : $query,
          (isset($stmt) && $stmt instanceof Statement ? ' (prepared: ' . $stmt->getQueryString() . ' )' : ''),
          ' e: ' . $e->getMessage(),
          ' args: ' . print_r($args, TRUE)
        ]);
        syslog(LOG_ERR, "error query: " . $message);

        // Prepare the exception to throw.
        $code = (int) $e->errorInfo[1];
        if (strpos($e->getMessage(), 'ORA-00001')) {
          $exception = new IntegrityConstraintViolationException($message, $code, $e);
        }
        else {
          $exception = new DatabaseExceptionWrapper($message, $code, $e);
        }

        // @todo: check whe do we need this?
        $exception->errorInfo = $e->errorInfo;
        if ($code === 1) {
          $exception->errorInfo[0] = '23000';
        }

        // Ignore allowed errors.
        if (isset($options['oracle_exceptions_allowed']) &&
          in_array($code, $options['oracle_exceptions_allowed'], FALSE)) {
          return NULL;
        }

        // Throw an exception otherwise.
        throw $exception;
      }

      return NULL;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function queryRange($query, $from, $count, array $args = array(), array $options = array()) {
    $start = (int) $from + 1;
    $end = (int) $count + (int) $from;

    $query_string = 'SELECT * FROM (SELECT TAB.*, ROWNUM ' . ORACLE_ROWNUM_ALIAS . ' FROM (' . $query . ') TAB) WHERE ' . ORACLE_ROWNUM_ALIAS . ' BETWEEN ';
    if (Connection::isAssoc($args)) {
      $args['oracle_rwn_start'] = $start;
      $args['oracle_rwn_end'] = $end;
      $query_string .= ':oracle_rwn_start AND :oracle_rwn_end';
    }
    else {
      $args[] = $start;
      $args[] = $end;
      $query_string .= '? AND ?';
    }

    return $this->query($query_string, $args, $options);
  }

  /**
   * {@inheritdoc}
   */
  public function queryTemporary($query, array $args = array(), array $options = array()) {
    $tablename = $this->generateTemporaryTableName();
    try {
      $this->query('DROP TABLE {' . $tablename . '}');
    }
    catch (\Exception $ex) {
      /* ignore drop errors */
    }
    $this->query('CREATE GLOBAL TEMPORARY TABLE {' . $tablename . '} ON COMMIT PRESERVE ROWS AS ' . $query, $args, $options);
    return $tablename;
  }

  /**
   * Helper function: allow to ignore some or all ORA errors.
   *
   * @param string $query
   *   The query to execute. In most cases this will be a string containing
   *   an SQL query with placeholders.
   *
   * @param array $args
   *   An array of arguments for the prepared statement. If the prepared
   *   statement uses ? placeholders, this array must be an indexed array.
   *   If it contains named placeholders, it must be an associative array.
   *
   * @param array $allowed
   *   The array of allowed ORA errors.
   *
   * @return bool
   *   FALSE if the error occurs, TRUE if not.
   *
   * @throws \Drupal\Core\Database\DatabaseExceptionWrapper
   *
   * @deprecated use query() with oracle_exceptions_allowed option instead.
   */
  public function querySafeDdl($query, $args = [], $allowed = []) {
    try {
      $this->query($query, $args);
    }
    catch (DatabaseExceptionWrapper $exception) {
      // Ignore all errors.
      if (empty($allowed)) {
        return FALSE;
      }

      // Ignore allowed errors.
      if (in_array($exception->getCode(), $allowed, FALSE)) {
        return FALSE;
      }

      // Throw an exception otherwise.
      throw $exception;
    }
    return TRUE;
  }

  /**
   * Executes an internal driver queries query string against the database.
   *
   * @param string $query
   *   The query to execute. In most cases this will be a string containing
   *   an SQL query with placeholders.
   *
   * @param array $args
   *   An array of arguments for the prepared statement. If the prepared
   *   statement uses ? placeholders, this array must be an indexed array.
   *   If it contains named placeholders, it must be an associative array.
   *
   * @return \Drupal\oracle\Driver\Database\oracle\Statement
   *
   * @throws \Drupal\Core\Database\DatabaseExceptionWrapper
   */
  public function queryOracle(string $query, $args = []) {
    // @todo: refactor to use query() method + additional options like
    // `oracle_disable_log` + `oracle_processable_query`.

    try {
      $logger = $this->pauseLog();
      $stmt = $this->prepareStatement($query, $this->defaultOptions());
      $stmt->execute($args);
      $this->continueLog($logger);
      return $stmt;
    }
    catch (DatabaseExceptionWrapper $exception) {
      syslog(LOG_ERR, "error: {$exception->getMessage()} {$query}");
      throw $exception;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function driver() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function databaseType() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function createDatabase($database) {
    // Database can be created manually only.
  }

  /**
   * {@inheritdoc}
   */
  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  /**
   * {@inheritdoc}
   */
  public function getFullQualifiedTableName($table) {
    $options = $this->getConnectionOptions();
    $prefix = $this->tablePrefix($table);

    // The fully qualified table name in Oracle Database is in the form of:
    // <user_name>.<table_name>@<database>. Where <database> is either
    // a database link created via `CREATE DATABASE LINK` or an alias from
    // Local Naming Parameters configuration (by default is located in the
    // $ORACLE_HOME/network/admin/tnsnames.ora). This Driver DO NOT support
    // auto creation of database links for the connection.
    return $prefix . '.' . $table . '@' . $options['database'];
  }

  /**
   * {@inheritdoc}
   */
  public function escapeTable($table) {
    if (!isset($this->connection->escapedTables[$table])) {
      $this->connection->escapedTables[$table] = preg_replace('/[^A-Za-z0-9_.@]+/', '', $table);
    }
    return $this->connection->escapedTables[$table];
  }

  /**
   * {@inheritdoc}
   */
  public function nextId($existing_id = 0) {
    // Retrive the name of the sequence. This information cannot be cached
    // because the prefix may change, for example, like it does in simpletests.
    $sequence_name = str_replace('"', '', $this->makeSequenceName('sequences', 'value'));
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    // Reset the sequence to a higher value than the existing id.
    $this->query("DROP SEQUENCE " . $sequence_name);
    $this->query("CREATE SEQUENCE " . $sequence_name . " START WITH " . ($existing_id + 1));

    // Retrive the next id. We know this will be as high as we want it.
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();

    return $id;
  }

  /**
   * Help method to check if array is associative.
   */
  public static function isAssoc($array) {
    return (is_array($array) && 0 !== count(array_diff_key($array, array_keys(array_keys($array)))));
  }

  /**
   * Oracle connection helper.
   */
  public function makePrimary() {
    // We are installing a primary database.
    $this->external = FALSE;
  }

  /**
   * Pause the database logging if any available.
   *
   * @see \Drupal\Core\Database::startLog()
   *
   * @return \Drupal\Core\Database\Log|null
   *  An instance of the database logger, or NULL if logging was not started.
   */
  public function pauseLog()  {
    if (!empty($this->logger)) {
      $logger = $this->logger;
      $this->logger = NULL;
      return $logger;
    }
    return NULL;
  }

  /**
   * Continue the previously paused database logger.
   *
   * @param $logger \Drupal\Core\Database\Log|null
   *  An instance of the database logger, or NULL if logging was not started.
   *
   * @see \Drupal\Driver\Database\oracle\Connection::pauseLog()
   */
  public function continueLog($logger) {
    if ($logger instanceof Log) {
      $this->logger = $logger;
    }
  }

  /**
   * Oracle connection helper.
   */
  private function exceptionQuery(&$unformattedQuery) {
    global $_oracle_exception_queries;

    if (!is_array($_oracle_exception_queries)) {
      return FALSE;
    }

    $count = 0;
    $oracle_unformatted_query = preg_replace(
      array_keys($_oracle_exception_queries),
      array_values($_oracle_exception_queries),
      $unformattedQuery,
      -1,
      $count
    );

    return $count;
  }

  /**
   * Oracle connection helper.
   */
  public function lastInsertId($name = NULL) {
    if (!$name) {
      throw new Exception('The name of the sequence is mandatory for Oracle');
    }

    try {
      return $this->queryOracle($this->prefixTables('SELECT ' . $name . '.currval from dual'))->fetchColumn();
    }
    catch (\Exception $e) {
      // Ignore if CURRVAL not set. May be an insert that specified the serial
      // field.
      syslog(LOG_ERR, " currval: " . print_r(debug_backtrace(FALSE), TRUE));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function generateTemporaryTableName() {
    // @todo: create a cleanup job.
    $session_id = $this->queryOracle("SELECT userenv('sessionid') FROM dual")->fetchColumn();
    return 'TMP_' . $session_id . '_' . $this->temporaryNameIndex++;
  }

  /**
   * {@inheritdoc}
   */
  public function quote($string, $parameter_type = \PDO::PARAM_STR) {
    return "'" . str_replace("'", "''", $string) . "'";
  }

  /**
   * {@inheritdoc}
   */
  public function version() {
    return NULL;
  }

  /**
   * {@inheritdoc}
   */
  public function prefixTables($sql) {
    $sql = parent::prefixTables($sql);
    return $this->escapeAnsi($sql);
  }

  /**
   * Oracle connection helper.
   */
  public function prepareQuery($query, $quote_identifiers = true) {
    $query = $this->escapeEmptyLiterals($query);
    $query = $this->escapeAnsi($query);
    if (!$this->external) {
      $query = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($query);
    }
    $query = $this->escapeReserved($query);
    $query = $this->escapeCompatibility($query);
    $query = $this->prefixTables($query);
    $query = $this->escapeIfFunction($query);
    return $this->connection->prepare($query);
  }

  public function prepareStatement(string $query, array $options): StatementInterface {
    $query = $this->escapeEmptyLiterals($query);
    $query = $this->escapeAnsi($query);
    if (!$this->external) {
      $query = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($query);
    }
    $query = $this->escapeReserved($query);
    $query = $this->escapeCompatibility($query);
    $query = $this->prefixTables($query);
    $query = $this->escapeIfFunction($query);
    return new $this->statementWrapperClass($this, $this->connection, $query, $options['pdo'] ?? []);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeAnsi($query) {
    if (preg_match('/^select /i', $query) &&
      !preg_match('/^select(.*)from/ims', $query)) {
      $query .= ' FROM DUAL';
    }

    $search = [
      "/([^\s\(]+) & ([^\s]+) = ([^\s\)]+)/",
      "/([^\s\(]+) & ([^\s]+) <> ([^\s\)]+)/",
      '/^RELEASE SAVEPOINT (.*)$/',
      '/([^\s\(]*) NOT REGEXP ([^\s\)]*)/',
      '/([^\s\(]*) REGEXP ([^\s\)]*)/',
    ];
    $replace = [
      "BITAND(\\1,\\2) = \\3",
      "BITAND(\\1,\\2) <> \\3",
      'begin null; end;',
      "NOT REGEXP_LIKE(\\1, \\2)",
      "REGEXP_LIKE(\\1, \\2)",
    ];
    $query = preg_replace($search, $replace, $query);

    // Find \w quoted with " but not quoted with '. Examples:
    // COMMENT ON TABLE "match" IS 'description may contain "not_matched" text.'
    // @TODO: this match wrongly (space before after '):
    // COMMENT ON TABLE "not_match_but_should" IS ' <space'
    $query = preg_replace_callback('/(?!\B\'[^\']*)("\w+?")(?![^\']*\'\B)/',
      static function ($matches) {
        return strtoupper($matches[1]);
      },
      $query);

    return str_replace('\\"', '"', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiteral($match) {
    if ($match[0] == "''") {
      return "'" . ORACLE_EMPTY_STRING_REPLACER . "'";
    }
    else {
      return $match[0];
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiterals($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace_callback("/'.*?'/", array($this, 'escapeEmptyLiteral'), $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeIfFunction($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace("/IF\s*\((.*?),(.*?),(.*?)\)/", 'case when \1 then \2 else \3 end', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeReserved($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $ddl = !((boolean) preg_match('/^(select|insert|update|delete)/i', $query));

    // Escapes all table names.
    $query = preg_replace_callback(
      '/({)(\w+)(})/',
      function ($matches) {
        return '"{' . strtoupper($matches[2]) . '}"';
      },
      $query);

    // Escapes long id.
    $query = preg_replace_callback(
      '/({L#)([\d]+)(})/',
      function ($matches) {
        return '"{L#' . strtoupper($matches[2]) . '}"';
      },
      $query);

    // Escapes reserved names.
    $query = preg_replace_callback(
      '/(\:)(uid|session|file|access|mode|comment|desc|size|start|end|increment)/',
      function ($matches) {
        return $matches[1] . 'db_' . $matches[2];
      },
      $query);

    $query = preg_replace_callback(
      '/(<uid>|<session>|<file>|<access>|<mode>|<comment>|<desc>|<size>' . ($ddl ? '' : '|<date>') . ')/',
      function ($matches) {
        return '"' . strtoupper($matches[1]) . '"';
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,\=])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')([,\s\=)])/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"' . $matches[3];
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')$/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"';
      },
      $query);

    return $query;
  }

  /**
   * Oracle connection helper.
   */
  public function removeFromCachedStatements($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $iquery = md5($this->prefixTables($query));
    if (isset($this->preparedStatements[$iquery])) {
      unset($this->preparedStatements[$iquery]);
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeCompatibility($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $search = array(
      // Remove empty concatenations leaved by concatenate_bind_variables.
      "''||",
      "||''",

      // Translate 'IN ()' to '= NULL' they do not match anything anyway.
      "IN ()",
      "IN  ()",

      '(FALSE)',
      'POW(',
      ") AS count_alias",
      '"{URL_ALIAS}" GROUP BY path',
      "ESCAPE '\\\\'",
      'SELECT CONNECTION_ID() FROM DUAL',
      'SHOW PROCESSLIST',
      'SHOW TABLES',
    );

    $replace = array(
      "",
      "",
      "= NULL",
      "= NULL",
      "(1=0)",
      "POWER(",
      ") count_alias",
      '"{URL_ALIAS}" GROUP BY SUBSTRING_INDEX(source, \'/\', 1)',
      "ESCAPE '\\'",
      'SELECT DISTINCT sid FROM v$mystat',
      'SELECT DISTINCT stat.sid, sess.process, sess.status, sess.username, sess.schemaname, sql.sql_text FROM v$mystat stat, v$session sess, v$sql sql WHERE sql.sql_id(+) = sess.sql_id AND sess.status = \'ACTIVE\' AND sess.type = \'USER\'',
      'SELECT * FROM user_tables',
    );

    return str_replace($search, $replace, $query);
  }

  /**
   * {@inheritdoc}
   */
  public function makeSequenceName($table, $field) {
    $sequence_name = $this->schema()->oid('SEQ_' . $table . '_' . $field, FALSE, FALSE);
    return '"{' . $sequence_name . '}"';
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgValue($value) {
    if ($value === '') {
      return ORACLE_EMPTY_STRING_REPLACER;
    }
    if (is_string($value) && strlen($value) > $this->maxVarchar2Size) {
      return $this->writeBlob($value);
    }
    return $value;
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgs($args) {
    if ($this->external) {
      return $args;
    }

    $ret = array();
    if (Connection::isAssoc($args)) {
      foreach ($args as $key => $value) {
        $key = Connection::escapeReserved($key);

        // Bind variables cannot have reserved names.
        $key = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($key);
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }
    else {
      // Indexed array.
      foreach ($args as $key => $value) {
        $ret[$key] = $this->cleanupArgValue($value);
      }
    }

    return $ret;
  }

  /**
   * Oracle connection helper.
   */
  public function writeBlob($value) {
    $hash = md5($value);
    $stmt = $this->connection->prepare("select blobid from blobs where hash = :hash");
    $stmt->bindParam(':hash', $hash, \PDO::PARAM_STR);
    $stmt->execute();
    $handle = $stmt->fetchColumn();

    if (empty($handle)) {
      $stream = Connection::stringToStream($value);
      $transaction = $this->startTransaction();
      $stmt = $this->prepareQuery("insert into blobs (blobid, content, hash) VALUES (seq_blobs.nextval, EMPTY_BLOB(), :hash) RETURNING content INTO :content");
      $stmt->bindParam(':hash', $hash, \PDO::PARAM_STR);
      $stmt->bindParam(':content', $stream, \PDO::PARAM_LOB);
      $stmt->execute();
      unset($transaction);
      $handle = $this->lastInsertId("seq_blobs");
    }

    $handle = ORACLE_BLOB_PREFIX . $handle;
    return $handle;
  }

  /**
   * Oracle connection helper.
   */
  public function readBlob($handle) {
    $handle = (int) substr($handle, strlen(ORACLE_BLOB_PREFIX));
    $stmt = $this->connection->prepare("select content from blobs where blobid= ?");
    $stmt->bindParam(1, $handle, \PDO::PARAM_INT);
    $stmt->execute();
    $return = $stmt->fetchColumn();

    if (!empty($return)) {
      return $return;
    }
    return '';
  }

  /**
   * Cleaned query string.
   *
   * 1) Long identifiers placeholders.
   *  May occur in queries like:
   *               select 1 as myverylongidentifier from mytable
   *  this is translated on query submission as e.g.:
   *               select 1 as L#321 from mytable
   *  so when we fetch this object (or array) we will have
   *     stdClass ( "L#321" => 1 ) or Array ( "L#321" => 1 ).
   *  but the code is expecting to access the field as myverylongidentifier,
   *  so we need to translate the "L#321" back to "myverylongidentifier".
   *
   * 2) BLOB placeholders.
   *   We can find values like B^#2354, and we have to translate those values
   *   back to their original long value so we read blob id 2354 of table blobs.
   *
   * 3) Removes the rwn column from queryRange queries.
   *
   * 4) Translate empty string replacement back to empty string.
   *
   * @return string
   *   Cleaned string to be executed.
   */
  public function cleanupFetched($f) {
    if ($this->external) {
      return $f;
    }

    if (is_array($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f[$key]);
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f[$this->getLongIdentifiersHandler()->longIdentifierKey($key)] = $this->cleanupFetched($value);
          unset($f[$key]);
        }
        else {
          $f[$key] = $this->cleanupFetched($value);
        }
      }
    }
    elseif (is_object($f)) {
      foreach ($f as $key => $value) {
        if ((string) $key == strtolower(ORACLE_ROWNUM_ALIAS)) {
          unset($f->{$key});
        }
        // Long identifier.
        elseif (Connection::isLongIdentifier($key)) {
          $f->{$this->getLongIdentifiersHandler()->longIdentifierKey($key)} = $this->cleanupFetched($value);
          unset($f->{$key});
        }
        else {
          $f->{$key} = $this->cleanupFetched($value);
        }
      }
    }
    else {
      $f = $this->cleanupFetchedValue($f);
    }

    return $f;
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupFetchedValue($value) {
    if (is_string($value)) {
      if ($value == ORACLE_EMPTY_STRING_REPLACER) {
        return '';
      }
      elseif ($this->isBlob($value)) {
        return $this->readBlob($value);
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  /**
   * Oracle connection helper.
   */
  public function resetLongIdentifiers() {
    if (!$this->external) {
      $this->getLongIdentifiersHandler()->resetLongIdentifiers();
    }
  }

  /**
   * Oracle connection helper.
   */
  public static function isLongIdentifier($key) {
    return (substr(strtoupper($key), 0, strlen(ORACLE_LONG_IDENTIFIER_PREFIX)) == ORACLE_LONG_IDENTIFIER_PREFIX);
  }

  /**
   * Oracle connection helper.
   */
  public static function isBlob($value) {
    return (substr($value, 0, strlen(ORACLE_BLOB_PREFIX)) == ORACLE_BLOB_PREFIX);
  }

  /**
   * Oracle connection helper.
   */
  private static function stringToStream($value) {
    $stream = fopen('php://memory', 'a');
    fwrite($stream, $value);
    rewind($stream);
    return $stream;
  }

  /**
   * Long identifier support.
   */
  public function getLongIdentifiersHandler() {
    static $long_identifier = NULL;

    if ($this->external) {
      return NULL;
    }

    // Initialize the long identifier handler.
    if (empty($long_identifier)) {
      $long_identifier = new LongIdentifierHandler($this);
    }
    return $long_identifier;
  }

}


/**
 * @} End of "addtogroup database".
 */
