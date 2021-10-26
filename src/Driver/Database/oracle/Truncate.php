<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Query\Truncate as QueryTruncate;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Truncate.
 */
class Truncate extends QueryTruncate {

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    $query = parent::__toString();
    return str_replace('TRUNCATE', 'TRUNCATE TABLE', $query);
  }

}
