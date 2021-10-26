<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Query\Upsert as QueryUpsert;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Upsert.
 */
class Upsert extends QueryUpsert {

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    // @TODO: handle this?
    return '';
  }

  /**
   * {@inheritdoc}
   */
  public function execute() {
    if (!$this->preExecute()) {
      return NULL;
    }

    foreach ($this->insertValues as $insert_values) {
      $this->executeOne($insert_values);
    }

    // @todo Should be last insert id.
    return NULL;
  }

  /**
   * @todo description
   */
  public function executeOne($insert_values) {
    $combined = array_combine($this->insertFields, $insert_values);
    $keys = [$this->key => $combined[$this->key]];

    $merge = $this->connection
      ->merge($this->table)
      ->fields($combined)
      ->keys($keys);

    if ($this->defaultFields) {
      $merge->useDefaults($this->defaultFields);
    }

    return $merge->execute();
  }

}
