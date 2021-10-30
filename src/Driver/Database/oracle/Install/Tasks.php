<?php

namespace Drupal\oracle\Driver\Database\oracle\Install;

use Drupal\Core\Database\Install\Tasks as InstallTasks;
use Drupal\Core\Database\Database;
use Drupal\oracle\Driver\Database\oracle\Connection;

/**
 * Specifies installation tasks for Oracle and equivalent databases.
 */
class Tasks extends InstallTasks {

  const ORACLE_MINIMUM_VERSION = '19c';

  /**
   * {@inheritdoc}
   */
  protected $pdoDriver = 'oci';

  /**
   * {@inheritdoc}
   */
  public function name() {
    return t('Oracle');
  }

  /**
   * {@inheritdoc}
   */
  public function minimumVersion() {
    return static::ORACLE_MINIMUM_VERSION;
  }

  /**
   * {@inheritdoc}
   */
  public function getFormOptions(array $database) {
    $form = parent::getFormOptions($database);
    if (empty($form['advanced_options']['port']['#default_value'])) {
      $form['advanced_options']['port']['#default_value'] = '1521';
    }
    return $form;
  }

  /**
   * {@inheritdoc}
   */
  protected function connect() {
    try {
      // This doesn't actually test the connection.
      Database::setActiveConnection();
      $initial = new OracleInitial(Database::getConnection());
      $initial->initial();
      $this->pass('Oracle has initialized itself.');
      Database::getConnection('default')->makePrimary();
    }
    catch (\Exception $e) {
      if ($e->getCode() == Connection::DATABASE_NOT_FOUND) {

        // Remove the database string from connection info.
        $connection_info = Database::getConnectionInfo();
        $database = $connection_info['default']['database'];
        unset($connection_info['default']['database']);
        $this->fail(t('Database %database not found. The server reports the following message when attempting to create the database: %error.', array('%database' => $database, '%error' => $e->getMessage())));
      }
      return FALSE;
    }
    return TRUE;
  }

}
