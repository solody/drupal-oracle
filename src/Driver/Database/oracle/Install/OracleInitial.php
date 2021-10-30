<?php

namespace Drupal\oracle\Driver\Database\oracle\Install;

/**
 * Generate oracle database macros.
 */
class OracleInitial {

  private $pdoBindLengthLimits = array(4000, 1332, 665);

  protected $connection;

  public function __construct($connection) {
    $this->connection = $connection;
  }

  public function initial() {
    $dir = __DIR__ . '/../resources';
    $this->determineSupportedBindSize();
    $this->createFailsafeObjects("{$dir}/table");
    $this->createFailsafeObjects("{$dir}/index");
    $this->createFailsafeObjects("{$dir}/sequence");
    $this->createObjects("{$dir}/function");
    $this->createObjects("{$dir}/procedure");
    $this->createSpObjects("{$dir}/type");
    $this->createSpObjects("{$dir}/package");
    $this->oracleQuery("begin dbms_utility.compile_schema(user); end;");
  }

  /**
   * Oracle helper for install tasks.
   */
  private function oracleQuery($sql, $args = NULL) {
    return $this->connection->queryOracle($sql, $args);
  }

  /**
   * Oracle helper for install tasks.
   */
  private function determineSupportedBindSize() {
    $this->failsafeDdl('create table bind_test (val varchar2(4000 char))');
    $ok = FALSE;

    foreach ($this->pdoBindLengthLimits as $length) {
      try {
        syslog(LOG_ERR, "trying to bind $length bytes...");
        $determined_size = $length;
        $this->oracleQuery('insert into bind_test values (?)', array(
          str_pad('a', $length, 'a'),
        ));
        syslog(LOG_ERR, "bind succeeded.");
        $ok = TRUE;
        break;
      }
      catch (\Exception $e) {
      }
    }

    if (!$ok) {
      throw new \Exception('unable to determine PDO maximum bind size');
    }

    $this->failsafeDdl("drop table oracle_bind_size");
    $this->failsafeDdl("create table oracle_bind_size as select $determined_size val from dual");
  }

  /**
   * Oracle helper for install tasks.
   */
  private function createSpObjects($dir_path) {
    $dir = opendir($dir_path);

    while ($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      if (is_dir($dir_path . "/" . $name)) {
        $this->createSpObject($dir_path . "/" . $name);
      }
    }
  }

  /**
   * Oracle helper for install tasks.
   */
  private function createSpObject($dir_path) {
    $dir = opendir($dir_path);
    $spec = $body = "";

    while ($name = readdir($dir)) {
      if (substr($name, -4) == '.pls') {
        $spec = $name;
      }
      elseif (substr($name, -4) == '.plb') {
        $body = $name;
      }
    }

    $this->createObject($dir_path . "/" . $spec);
    if ($body) {
      $this->createObject($dir_path . "/" . $body);
    }
  }

  /**
   * Oracle helper for install tasks.
   */
  private function createObjects($dir_path) {
    $dir = opendir($dir_path);
    while ($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      $this->createObject($dir_path . "/" . $name);
    }
  }

  /**
   * Oracle helper for install tasks.
   */
  private function createObject($file_path) {
    syslog(LOG_ERR, "creating object: $file_path");

    try {
      $this->oracleQuery($this->getPhpContents($file_path));
    }
    catch (\Exception $e) {
      syslog(LOG_ERR, "object $file_path created with errors");
    }
  }

  /**
   * Oracle helper for install tasks.
   */
  private function createFailsafeObjects($dir_path) {
    $dir = opendir($dir_path);

    while ($name = readdir($dir)) {
      if (in_array($name, array('.', '..', '.DS_Store', 'CVS'))) {
        continue;
      }
      syslog(LOG_ERR, "creating object: $dir_path/$name");
      $this->failsafeDdl($this->getPhpContents($dir_path . "/" . $name));
    }
  }

  /**
   * Oracle helper for install tasks.
   */
  private function failsafeDdl($ddl) {
    $this->oracleQuery("begin execute immediate '" . str_replace("'", "''", $ddl) . "'; exception when others then null; end;");
  }

  /**
   * Oracle helper for install tasks.
   */
  private function getPhpContents($filename) {
    if (is_file($filename)) {
      ob_start();
      require_once $filename;
      $contents = ob_get_contents();
      ob_end_clean();
      return $contents;
    }
    else {
      syslog(LOG_ERR, "error: file " . $filename . " does not exists");
    }
    return FALSE;
  }
}
