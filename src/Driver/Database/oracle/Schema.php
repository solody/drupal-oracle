<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Component\Utility\SafeMarkup;
use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\SchemaObjectExistsException;
use Drupal\Core\Database\SchemaObjectDoesNotExistException;
use Drupal\Core\Database\Schema as DatabaseSchema;

/**
 * Oracle implementation of \Drupal\Core\Database\Schema.
 */
class Schema extends DatabaseSchema {

  /**
   * The database connection.
   *
   * @var \Drupal\oracle\Driver\Database\oracle\Connection
   */
  protected $connection;

  /**
   * An array of driver internal tables names.
   */
  protected $driverTables = [
    'BIND_TEST',
    'BLOBS',
    'BLOB_COLUMN',
    'USED_BLOBS',
    'LONG_IDENTIFIERS',
    'ORACLE_BIND_SIZE',
  ];

  /**
   * A cache of information about blob columns and sequences of tables.
   *
   * @var array
   */
  protected $driverTableInformation = [];

  /**
   * @todo description
   */
  private $foundLongIdentifier = FALSE;

  /**
   * Oracle schema helper.
   */
  public function oid($name, $prefix = FALSE, $quote = TRUE) {
    $return = $name;

    if (strlen($return) > ORACLE_IDENTIFIER_MAX_LENGTH) {
      $this->foundLongIdentifier = TRUE;
      $return = $this->connection
        ->queryOracle('SELECT identifier.get_for(?) FROM dual', [strtoupper($return)])
        ->fetchColumn();
    }

    $return = $prefix ? '{' . $return . '}' : strtoupper($return);

    if (!$prefix && $quote) {
      $return = '"' . $return . '"';
    }

    return $return;
  }

  /**
   * Oracle schema helper.
   */
  private function resetLongIdentifiers() {
    if ($this->foundLongIdentifier) {
      $this->connection->resetLongIdentifiers();
      $this->foundLongIdentifier = FALSE;
    }
  }

  /**
   * Oracle schema helper.
   */
  public function getTableInfo($table) {
    $table_prefixed = $this->connection->prefixTables('{' . $table . '}');
    $schema = $this->tableSchema($table_prefixed);

    if (!isset($this->driverTableInformation[$table_prefixed])) {
      $this->driverTableInformation[$table_prefixed] = (object) [];

      try {
        $this->driverTableInformation[$table_prefixed] = $this->connection
          ->queryOracle('SELECT identifier.sequence_for_table(?,?) sequence_name FROM dual', [$table, $schema])
          ->fetchObject();
      }
      catch (\PDOException $exception) {
        if ($exception->errorInfo[1] != '00904') {
          // Ignore (may be a connection to a non drupal schema not having the
          // identifier pkg). See http://drupal.org/node/1121044.
          throw $exception;
        }
      }
    }

    return $this->driverTableInformation[$table_prefixed];
  }

  /**
   * Oracle schema helper.
   */
  public function removeTableInfoCache($table) {
    $table_prefixed = $this->connection->prefixTables('{' . $table . '}');
    unset($this->driverTableInformation[$table_prefixed]);
  }

  /**
   * Emulates mysql default column behaviour.
   *
   * Eg. insert into table (col1) values (null).
   * If col1 has default in mysql you have the default inserted instead of null.
   * On oracle you have null inserted. So we need a trigger to intercept this
   * condition and substitute null with default. This happens on MySQL only
   * inserting not updating.
   */
  public function rebuildDefaultsTrigger($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));
    $oname = $this->oid($table, FALSE, FALSE);

    $trigger = 'create or replace trigger ' . $this->oid('TRG_' . $table . '_DEFS', TRUE) .
      ' before insert on ' . $this->oid($table, TRUE) .
      ' for each row begin /* defs trigger */ if inserting then ';

    $serial_oname = $this->connection->queryOracle("select field_name from table(identifier.get_serial(?,?))", array($table, $schema))->fetchColumn();
    $serial_oname = $serial_oname ? $serial_oname : "^NNC^";

    $stmt = $this->connection->queryOracle(
      "select /*+ALL_ROWS*/ column_name,
       data_default
       from all_tab_columns
       where column_name != ?
       and owner= nvl(user,?)
       and table_name= ?
       and data_default is not null
      ",
      array($serial_oname, $schema, $oname)
    );

    $def = FALSE;

    while ($row = $stmt->fetchObject()) {
      $def = TRUE;
      $trigger .=
        'if :NEW."' . $row->column_name . '" is null or to_char(:NEW."' . $row->column_name . '") = \'' . ORACLE_EMPTY_STRING_REPLACER . '\'
          then :NEW."' . $row->column_name . '":= ' . $row->data_default . ';
          end if;
        ';
    }

    if (!$def) {
      $trigger .= ' null; ';
    }

    $trigger .= 'end if; end;';
    $this->connection->query($trigger, [], ['allow_delimiter_in_query' => TRUE]);
  }

  /**
   * {@inheritdoc}
   */
  public function createTable($name, $table) {
    if ($this->tableExists($name)) {
      throw new SchemaObjectExistsException(t('Table @name already exists.', ['@name' => $name]));
    }
    $statements = $this->createTableSql($name, $table);
    foreach ($statements as $statement) {
      $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
    }
    $this->rebuildDefaultsTrigger($name);
    $this->resetLongIdentifiers();
  }

  /**
   * Oracle schema helper.
   */
  public function createSerialSql($table, $field_name, $start_with = 1) {
    $oname = $this->oid($table, TRUE);

    $trgname = $this->oid('TRG_' . $table . '_' . $field_name, TRUE);
    $seqname = $this->oid('SEQ_' . $table . '_' . $field_name, TRUE);

    $ofield_name = $this->oid($field_name);

    $tblname_ser = $this->oid($table, FALSE, FALSE);
    $trgname_ser = $this->oid('TRG_' . $table . '_' . $field_name, FALSE, FALSE);
    $seqname_ser = $this->oid('SEQ_' . $table . '_' . $field_name, FALSE, FALSE);
    $fldname_ser = $this->oid($field_name, FALSE, FALSE);

    $statements[] = 'CREATE SEQUENCE ' . $seqname . ((int) $start_with > 1 ? ' START WITH ' . (int) $start_with : '');
    $statements[] = 'CREATE OR REPLACE TRIGGER ' . $trgname . ' before insert on ' .
      $oname . ' for each row declare v_id number:= 0; begin /* serial(' . $tblname_ser .
      ',' . $trgname_ser . ',' . $seqname_ser . ',' . $fldname_ser . ') */ if inserting then if :NEW.' .
      $ofield_name . ' is null or :NEW.' . $ofield_name . ' = 0 then select ' .
      $seqname . '.nextval into :NEW.' . $ofield_name . ' from dual; else while v_id < :NEW.' . $ofield_name .
      ' loop select ' . $seqname . '.nextval into v_id from dual; end loop; end if; end if; end;';
    $statements[] = 'ALTER TRIGGER ' . $trgname . ' ENABLE';

    return $statements;
  }

  /**
   * Generate SQL to create a new table from a Drupal schema definition.
   *
   * @param string $name
   *   The name of the table to create.
   * @param array $table
   *   A Schema API table definition array.
   *
   * @return string[]
   *   An array of SQL statements to create the table.
   */
  protected function createTableSql($name, array $table) {
    $oname = $this->oid($name, TRUE);

    $sql_fields = array();
    foreach ($table['fields'] as $field_name => $field) {
      $sql_fields[] = $this->createFieldSql($field_name, $this->processField($field));
    }

    $sql_keys = array();

    if (!empty($table['primary key']) && is_array($table['primary key'])) {
      $this->ensureNotNullPrimaryKey($table['primary key'], $table['fields']);
      $sql_keys[] = 'CONSTRAINT ' . $this->oid('PK_' . $name) . ' PRIMARY KEY (' . $this->createColsSql($table['primary key']) . ')';
    }

    if (isset($table['unique keys']) && is_array($table['unique keys'])) {
      foreach ($table['unique keys'] as $key_name => $key) {
        $sql_keys[] = 'CONSTRAINT ' . $this->oid('UK_' . $name . '_' . $key_name) . ' UNIQUE (' . $this->createColsSql($key) . ')';
      }
    }

    $sql = "CREATE TABLE " . $oname . " (\n\t" . implode(",\n\t", $sql_fields);
    if (count($sql_keys) > 0) {
      $sql .= ",\n\t";
    }
    $sql .= implode(",\n\t", $sql_keys) . "\n)";
    $statements[] = $sql;

    if (isset($table['indexes']) && is_array($table['indexes'])) {
      foreach ($table['indexes'] as $key_name => $key) {
        $statements = array_merge($statements, $this->createIndexSql($name, $key_name, $key));
      }
    }

    // Add table comment.
    if (isset($table['description'])) {
      $statements[] = 'COMMENT ON TABLE ' . $oname . ' IS ' . $this->prepareComment($table['description']);
    }

    // Add column comments.
    foreach ($table['fields'] as $field_name => $field) {
      if (isset($field['description'])) {
        $statements[] = 'COMMENT ON COLUMN ' . $oname . '.' . $this->oid($field_name) . ' IS ' . $this->prepareComment($field['description']);
      }
    }

    foreach ($table['fields'] as $field_name => $field) {
      if (!isset($field['type'])) {
        continue;
      }
      if ($field['type'] == 'serial') {
        $statements = array_merge($statements, $this->createSerialSql($name, $field_name));
      }
      elseif ($field['type'] == 'blob') {
        $statements[] = "INSERT INTO BLOB_COLUMN VALUES ('" . strtoupper($name) . "','" . strtoupper($field_name) . "')";
      }
    }

    return $statements;
  }

  /**
   * Create an SQL string for a field.
   *
   * To be used in table creation or alteration. Before passing a field out of
   * a schema definition into this function it has to be processed by
   * Schema:processField().
   *
   * @param string $name
   *   Name of the field.
   * @param array $spec
   *   The field specification, as per the schema data structure format.
   *
   * @return string
   *   An array of SQL statements to create the field.
   */
  protected function createFieldSql($name, array $spec) {
    $oname = $this->oid($name);
    $sql = $oname . ' ' . $spec['oracle_type'];

    if (isset($spec['type']) && $spec['type'] == 'serial') {
      unset($spec['not null']);
    }

    if ($spec['oracle_type'] == 'varchar2') {
      $sql .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (!empty($spec['length'])) {
      $sql .= '(' . $spec['length'] . ')';
    }
    elseif (isset($spec['precision']) && isset($spec['scale'])) {
      $sql .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    if (isset($spec['default'])) {
      $default = is_string($spec['default']) ? $this->connection->quote($this->connection->cleanupArgValue($spec['default'])) : $spec['default'];
      $sql .= " default {$default}";
    }

    if (!empty($spec['not null'])) {
      $sql .= ' NOT NULL';
    }

    if (!empty($spec['unsigned'])) {
      $sql .= " CHECK ({$oname} >= 0)";
    }

    return $sql;
  }

  /**
   * {@inheritdoc}
   */
  public function getFieldTypeMap() {
    // Put :normal last so it gets preserved by array_flip. This makes
    // it much easier for modules (such as schema.module) to map
    // database types back into schema types.
    $map = array(
      'varchar_ascii:normal' => 'varchar2',

      'varchar:normal' => 'varchar2',
      'char:normal' => 'char',

      'text:tiny' => 'varchar2',
      'text:small' => 'varchar2',
      'text:medium' => 'varchar2',
      'text:big' => 'varchar2',
      'text:normal' => 'varchar2',

      'int:tiny' => 'number',
      'int:small' => 'number',
      'int:medium' => 'number',
      'int:big' => 'number',
      'int:normal' => 'number',

      'float:tiny' => 'number',
      'float:small' => 'number',
      'float:medium' => 'number',
      'float:big' => 'number',
      'float:normal' => 'number',

      'numeric:normal' => 'number',

      'blob:big' => 'varchar2',
      'blob:normal' => 'varchar2',

      'date:normal' => 'date',

      'datetime:normal' => 'timestamp with local time zone',
      'timestamp:normal' => 'timestamp',
      'time:normal'     => 'timestamp',

      'serial:tiny' => 'number',
      'serial:small' => 'number',
      'serial:medium' => 'number',
      'serial:big' => 'number',
      'serial:normal' => 'number',
    );

    return $map;
  }

  /**
   * {@inheritdoc}
   */
  public function renameTable($table, $new_name) {
    $info = $this->getTableSerialInfo($table);
    $oname = $this->oid($new_name, TRUE);

    if (!empty($info->sequence_name)) {
      $this->connection->querySafeDdl('DROP TRIGGER {' . $info->trigger_name . '}');
      $this->connection->querySafeDdl('DROP SEQUENCE {' . $info->sequence_name . '}');
    }

    // Drop defaults trigger.
    $this->connection->querySafeDdl('DROP TRIGGER ' . $this->oid('TRG_' . $table . '_DEFS', TRUE));

    // Should not use prefix because schema is not needed on rename.
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' RENAME TO ' . $this->oid($new_name, FALSE));

    if (!empty($info->sequence_name)) {
      $statements = $this->createSerialSql($table, $info->field_name, $info->sequence_restart);
      foreach ($statements as $statement) {
        $this->connection->query($statement);
      }
    }

    // Rename indexes.
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));
    if ($schema) {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM all_indexes WHERE table_name= ? and owner= ?", array(
        $this->oid($new_name, FALSE, FALSE),
        $schema,
      ));
    }
    else {
      $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM user_indexes WHERE table_name= ?", array(
        $this->oid($new_name, FALSE, FALSE),
      ));
    }
    while ($row = $stmt->fetchObject()) {
      $this->connection->query('ALTER INDEX ' . $this->oid($row->index_name, TRUE) . ' RENAME TO ' . $this->oid(str_replace(strtoupper($table), strtoupper($new_name), $row->index_name), FALSE));
    }

    $this->cleanUpSchema($table, $new_name);
  }

  /**
   * {@inheritdoc}
   */
  public function dropTable($table) {

//    // Workaround to fix deleting of simpletest data.
//    if (preg_match('/^test\d+.*/', $table) === 1) {
//
//      // Always convert to uppercase, because of conversion to lowercase in
//      // findTables() method.
//      return $this->connection->query('DROP USER '. strtoupper($table) .' CASCADE');
//    }

    $info = $this->getTableInfo($table);

    if ($info->sequence_name) {
      $this->connection->query('DROP SEQUENCE ' . $info->sequence_name);
    }

    if (!$this->tableExists($table)) {
      return FALSE;
    }

    $this->connection->query('DROP TABLE ' . $this->oid($table, TRUE) . ' CASCADE CONSTRAINTS PURGE');
    $this->removeTableInfoCache($table);
  }

  /**
   * {@inheritdoc}
   */
  public function addField($table, $field, $spec, $new_keys = array()) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add field @table.@field: table doesn't exist.", ['@field' => $field, '@table' => $table]));
    }
    if ($this->fieldExists($table, $field)) {
      throw new SchemaObjectExistsException(t('Cannot add field @table.@field: field already exists.', ['@field' => $field, '@table' => $table]));
    }

    // Fields that are part of a PRIMARY KEY must be added as NOT NULL.
    $is_primary_key = isset($new_keys['primary key']) && in_array($field, $new_keys['primary key'], TRUE);
    if ($is_primary_key) {
      $this->ensureNotNullPrimaryKey($new_keys['primary key'], [$field => $spec]);
    }

    $fixnull = FALSE;
    if (!empty($spec['not null']) && !isset($spec['default']) && !$is_primary_key) {
      $fixnull = TRUE;
      $spec['not null'] = FALSE;
    }

    // Actually add this field to the table.
    $query = 'ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD (';
    $query .= $this->createFieldSql($field, $this->processField($spec)) . ')';
    $this->connection->query($query);

    // Once the field is created, update to the needed initial values.
    if (isset($spec['initial_from_field'])) {
      if (isset($spec['initial'])) {
        $expression = 'COALESCE(' . $this->oid($spec['initial_from_field']) . ', :default_initial_value)';
        $arguments = [':default_initial_value' => $spec['initial']];

        // @todo: wrong bind of number values (COALESCE return CHAR type).
        if (is_int($spec['initial'])) {
          $expression = 'COALESCE(' . $this->oid($spec['initial_from_field']) . ', ' . $spec['initial'] . ')';
          $arguments = [];
        }
      }
      else {
        $expression = $spec['initial_from_field'];
        $arguments = [];
      }
      $this->connection->update($table)
        ->expression($field, $expression, $arguments)
        ->execute();
    }
    elseif (isset($spec['initial'])) {
      $this->connection->update($table)
        ->fields([$field => $spec['initial']])
        ->execute();
    }

    // Not null.
    if ($fixnull) {
      $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . " MODIFY (" . $this->oid($field) . " NOT NULL)");
    }

    // Make sure to drop the existing primary key before adding a new one.
    // This is only needed when adding a field because this method, unlike
    // changeField(), is supposed to handle primary keys automatically.
    if (isset($new_keys)) {
      if (isset($new_keys['primary key']) && $this->constraintExists($table, 'PK')) {
        $this->dropPrimaryKey($table);
      }
      $this->createKeys($table, $new_keys);
    }

    // Add column comment.
    if (!empty($spec['description'])) {
      $this->connection->query('COMMENT ON COLUMN ' . $this->oid($table, TRUE) . '.' . $this->oid($field) . ' IS ' . $this->prepareComment($spec['description']));
    }

    // Create sequences.
    if ($spec['type'] === 'serial') {
      $statements = $this->createSerialSql($table, $field);
      foreach ($statements as $statement) {
        $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
      }
    }

    $this->cleanUpSchema($table);
  }

  /**
   * {@inheritdoc}
   */
  public function dropField($table, $field) {
    if (!$this->fieldExists($table, $field)) {
      return FALSE;
    }

    // Drop the sequence if exists.
    $info = $this->getTableSerialInfo($table);
    if (!empty($info->sequence_name) && $this->oid($field, FALSE, FALSE) == $info->field_name) {
      $this->connection->querySafeDdl('DROP TRIGGER {' . $info->trigger_name . '}');
      $this->connection->querySafeDdl('DROP SEQUENCE {' . $info->sequence_name . '}');
    }

    // Handle "ORA-12991: column is referenced in a multi-column constraint".
    if (!$this->connection->querySafeDdl('ALTER TABLE {' . $table . '} DROP COLUMN ' . $this->oid($field), [], ['12991'])) {

      // Drop the primary key if column in it.
      if (in_array($field, $this->findPrimaryKeyColumns($table), TRUE)) {
        $this->dropPrimaryKey($table);
      }

      // Re-try the deletion.
      $this->connection->query('ALTER TABLE {' . $table . '} DROP COLUMN ' . $this->oid($field));
    }
    $this->cleanUpSchema($table);
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function changeField($table, $field, $field_new, $spec, $keys_new = array()) {
    if (!$this->fieldExists($table, $field)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot change the definition of field @table.@name: field doesn't exist.", ['@table' => $table, '@name' => $field]));
    }
    if (($field != $field_new) && $this->fieldExists($table, $field_new)) {
      throw new SchemaObjectExistsException(t('Cannot rename field @table.@name to @name_new: target field already exists.', ['@table' => $table, '@name' => $field, '@name_new' => $field_new]));
    }
    if (isset($keys_new['primary key']) && in_array($field_new, $keys_new['primary key'], TRUE)) {
      $this->ensureNotNullPrimaryKey($keys_new['primary key'], [$field_new => $spec]);
    }

    // Proceed with table and constraints info.
    $spec = $this->processField($spec);
    $index_schema = $this->introspectIndexSchema($table);
    $table_serial = $this->getTableSerialInfo($table);

    // Prepare new field definition.
    $field_def = $spec['oracle_type'];
    if ($spec['oracle_type'] === 'varchar2') {
      $field_def .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (isset($spec['precision'], $spec['scale'])) {
      $field_def .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    // Remove old constraints.
    if (!empty($table_serial->sequence_name) && $this->oid($field, FALSE, FALSE) == $table_serial->field_name) {
      $this->connection->querySafeDdl('DROP TRIGGER {' . $table_serial->trigger_name . '}');
      $this->connection->querySafeDdl('DROP SEQUENCE {' . $table_serial->sequence_name . '}');
    }

    // Convert the field type and check for the error:
    // "ORA-01439: column to be modified must be empty to change datatype".
    if (!$this->connection->querySafeDdl('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . ' ' . $field_def, [], ['01439'])) {
      $this->connection->query('ALTER TABLE {' . $table . '} RENAME COLUMN ' . $this->oid($field) . ' TO ' . $this->oid($field . '_old'));
      $not_null = isset($spec['not null']) ? $spec['not null'] : FALSE;
      unset($spec['not null']);
      $this->addField($table, $field, $spec);
      $this->connection->query('UPDATE {' . $table . '} SET ' . $this->oid($field) . ' = ' . $this->oid($field . '_old'));
      if ($not_null) {
        $this->connection->query('ALTER TABLE {' . $table . '} MODIFY (' . $this->oid($field) . ' NOT NULL)');
      }
      $this->dropField($table, $field . '_old');
    }

    // Remove old default.
    $this->connection->query('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . ' DEFAULT NULL');

    // Handle not null specification.
    if (isset($spec['not null'])) {
      if ($spec['not null']) {
        $nullaction = ' NOT NULL';
      }
      else {
        $nullaction = ' NULL';
      }

      // We do not have current field NULL specification, so try to avoid:
      // "ORA-01442: column to be modified to NOT NULL is already NOT NULL"
      // "ORA-01451: column to be modified to NULL cannot be modified to NULL"
      $this->connection->querySafeDdl('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . $nullaction, [], [
        '01442',
        '01451',
      ]);
    }

    // Create sequences.
    if ($spec['type'] === 'serial') {
      $nextval = $this->connection->query('SELECT MAX("' . $field . '") FROM {' . $table . '}')->fetchField();
      $statements = $this->createSerialSql($table, $field, $nextval);
      foreach ($statements as $statement) {
        $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
      }
    }

    // Rename the column if necessary.
    if ($field !== $field_new) {
      $this->connection->query('ALTER TABLE {' . $table . '} RENAME COLUMN ' . $this->oid($field) . ' TO ' . $this->oid($field_new));
    }

    // Add unsigned check if necessary.
    if (!empty($spec['unsigned'])) {
      $this->connection->query('ALTER TABLE {' . $table . '} ADD CHECK (' . $this->oid($field_new) . ' >= 0)');
    }

    // Add default if necessary.
    if (isset($spec['default'])) {
      $default = is_string($spec['default']) ? $this->connection->quote($this->connection->cleanupArgValue($spec['default'])) : $spec['default'];
      $this->connection->query('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field_new) . ' DEFAULT ' . $default);
    }

    // Change description if necessary.
    if (!empty($spec['description'])) {
      $this->connection->query('COMMENT ON COLUMN {' . $table . '}.' . $this->oid($field_new) . ' IS ' . $this->prepareComment($spec['description']));
    }

    // Update primary index because if needed.
    if (in_array($field, $index_schema['primary key'], TRUE)) {
      $index_schema['primary key'][array_search($field, $index_schema['primary key'], TRUE)] = $field_new;
      $this->dropPrimaryKey($table);
      $this->addPrimaryKey($table, $index_schema['primary key']);
    }

    // Set new keys.
    if (isset($keys_new)) {
      $this->createKeys($table, $keys_new);
    }

    $this->cleanUpSchema($table);
  }

  /**
   * Set database-engine specific properties for a field.
   *
   * @param array $field
   *   A field description array, as specified in the schema documentation.
   *
   * @return array
   *   A field description array after changes.
   */
  protected function processField($field) {
    if (!isset($field['size'])) {
      $field['size'] = 'normal';
    }

    // Set the correct database-engine specific datatype.
    $map = $this->getFieldTypeMap();
    if (isset($field['oracle_type'])) {
      $field['oracle_type'] = strtoupper($field['oracle_type']);
    }
    elseif (!isset($field['type']) && isset($field['pgsql_type'])) {
      $field['oracle_type'] = $map[$field['pgsql_type'] . ':' . $field['size']];
    }
    elseif ($field['type']) {
      $field['oracle_type'] = $map[$field['type'] . ':' . $field['size']];
    }

    return $field;
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetDefault($table, $field, $default) {
    if (is_null($default)) {
      $default = 'NULL';
    }
    else {
      $default = is_string($default) ? $this->connection->quote($this->connection->cleanupArgValue($default)) : $default;
    }

    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field) . ' DEFAULT ' . $default . ' )');
    $this->rebuildDefaultsTrigger($table);
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetNoDefault($table, $field) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' MODIFY (' . $this->oid($field) . ' DEFAULT NULL)');
    $this->rebuildDefaultsTrigger($table);
  }

  /**
   * Helper function: check if a constraint exists.
   *
   * @param string $table
   *   The name of the table.
   * @param string $name
   *   The name of the constraint (typically 'pkey' or '[constraint]__key').
   *
   * @return bool
   *   TRUE if the constraint exists, FALSE otherwise.
   */
  public function constraintExists($table, $name) {
    $table_name = $this->oid($table, FALSE, FALSE);
    $constraint_name = $this->oid($name . '_' . $table, FALSE, FALSE);
    $constraint_schema = $this->connection->tablePrefix($table);
    return (bool) $this->connection->query("
     SELECT constraint_name
       FROM all_constraints
      WHERE constraint_type = 'P'
        AND constraint_name = :constraint_name
        AND table_name = :table_name
        AND owner = :constraint_schema", [
      ':table_name' => $table_name,
      ':constraint_name' => $constraint_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchField();
  }

  /**
   * {@inheritdoc}
   */
  protected function findPrimaryKeyColumns($table) {
    if (!$this->tableExists($table)) {
      return FALSE;
    }

    if (!$this->constraintExists($table, 'PK')) {
      return [];
    }

    $table_name = $this->oid($table, FALSE, FALSE);
    $constraint_name = $this->oid('PK_' . $table, FALSE, FALSE);
    $constraint_schema = $this->connection->tablePrefix($table);
    $constraint_columns = $this->connection->query('
     SELECT column_name
       FROM all_cons_columns
      WHERE constraint_name = :constraint_name
        AND table_name = :table_name
        AND owner = :constraint_schema
   ORDER BY position ASC', [
      ':table_name' => $table_name,
      ':constraint_name' => $constraint_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchCol();

    return array_map('strtolower', $constraint_columns);
  }

  /**
   * {@inheritdoc}
   */
  protected function introspectIndexSchema($table) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException("The table {$table} doesn't exist.");
    }

    $index_schema = [
      'primary key' => [],
      'unique keys' => [],
      'indexes' => [],
    ];

    $table_name = $this->oid($table, FALSE, FALSE);
    $constraint_schema = $this->connection->tablePrefix($table);
    $constraint_columns = $this->connection->query('
     SELECT constraint_name, LOWER(column_name) AS column_name
       FROM all_cons_columns
      WHERE table_name = :table_name
        AND owner = :constraint_schema
   ORDER BY position ASC', [
      ':table_name' => $table_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchAll();
    foreach ($constraint_columns as $constraint) {
      if (0 === strpos($constraint->constraint_name, 'PK_')) {
        $index_schema['primary key'][] = $constraint->column_name;
      }
      elseif (0 === strpos($constraint->constraint_name, 'UK_')) {
        // Format `UK_TABLE_NAME_KEY_NAME` into `key_name`.
        $constraint_name = strtolower(substr($constraint->constraint_name, 4 + strlen($table)));
        $index_schema['unique keys'][$constraint_name][] = $constraint->column_name;
      }
    }

    $indexes = $this->connection->query('
     SELECT index_name, LOWER(column_name) AS column_name
       FROM all_ind_columns
      WHERE table_name = :table_name
        AND index_owner = :constraint_schema', [
      ':table_name' => $table_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchAll();
    foreach ($indexes as $index) {
      if (0 === strpos($index->index_name, 'IDX_')) {
        // Format `IDX_TABLE_NAME_INDEX_NAME` into `key_name`.
        $index_name = strtolower(substr($index->index_name, 5 + strlen($table)));
        $index_schema['indexes'][$index_name][] = $index->column_name;
      }
    }
    return $index_schema;
  }

  /**
   * {@inheritdoc}
   */
  public function addPrimaryKey($table, $fields) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add primary key to table @table: table doesn't exist.", ['@table' => $table]));
    }
    if ($this->constraintExists($table, 'PK')) {
      throw new SchemaObjectExistsException(t('Cannot add primary key to table @table: primary key already exists.', ['@table' => $table]));
    }

    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('PK_' . $table) . ' PRIMARY KEY (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropPrimaryKey($table) {
    if (!$this->constraintExists($table, 'PK')) {
      return FALSE;
    }

    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('PK_' . $table));
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function addUniqueKey($table, $name, $fields) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name) . ' UNIQUE (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropUniqueKey($table, $name) {
    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' DROP CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name));
  }

  /**
   * {@inheritdoc}
   */
  public function addIndex($table, $name, $fields, array $specs) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add index @name to table @table: table doesn't exist.", ['@table' => $table, '@name' => $name]));
    }
    if ($this->indexExists($table, $name)) {
      throw new SchemaObjectExistsException(t('Cannot add index @name to table @table: index already exists.', ['@table' => $table, '@name' => $name]));
    }

    $statements = $this->createIndexSql($table, $name, $fields);
    foreach ($statements as $statement) {
      $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function dropIndex($table, $name) {
    if (!$this->indexExists($table, $name)) {
      return FALSE;
    }

    $this->connection->query('DROP INDEX ' . $this->oid('IDX_' . $table . '_' . $name, TRUE));
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function indexExists($table, $name) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));

    $oname = $this->oid('IDX_' . $table . '_' . $name, FALSE, FALSE);

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_indexes WHERE index_name = ? and table_name= ? and owner= ?", array(
        $oname, $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_indexes WHERE index_name = ? and table_name= ?", array(
        $oname,
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * Retrieve a table or column comment.
   */
  public function getComment($table, $column = NULL) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));

    if ($schema) {
      if (isset($column)) {
        return $this->connection->query('select comments from all_col_comments where column_name = ? and table_name = ? and owner = ?', array(
          $this->oid($column, FALSE, FALSE),
          $this->oid($table, FALSE, FALSE),
          $schema,
        ))->fetchField();
      }
      return $this->connection->query('select comments from all_tab_comments where table_name = ? and owner = ?', array(
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      if (isset($column)) {
        return $this->connection->query('select comments from user_col_comments where column_name = ? and table_name = ?', array(
          $this->oid($column, FALSE, FALSE),
          $this->oid($table, FALSE, FALSE),
        ))->fetchField();
      }
      return $this->connection->query('select comments from user_tab_comments where table_name = ?', array(
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }
  }

  /**
   * {@inheritdoc}
   */
  public function tableExists($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tables WHERE temporary= 'N' and table_name = ? and owner= ?", array(
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query("SELECT 1 FROM user_tables WHERE temporary= 'N' and table_name = ?", array(
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function fieldExists($table, $column) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));

    if ($schema) {
      $retval = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ?", array(
        $this->oid($column, FALSE, FALSE),
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }
    else {
      $retval = $this->connection->query('SELECT 1 FROM user_tab_columns WHERE column_name= ? and table_name = ?', array(
        $this->oid($column, FALSE, FALSE),
        $this->oid($table, FALSE, FALSE),
      ))->fetchField();
    }

    if ($retval) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function findTables($table_expression) {
    $individually_prefixed_tables = $this->connection->getUnprefixedTablesMap();
    $default_prefix = $this->connection->tablePrefix();
    $expression_prefix = $this->tableSchema($table_expression);
    $tables = [];

    // Load all the tables up front in order to take into account per-table
    // prefixes. The actual matching is done at the bottom of the method.
    // Don't use {} around `all_tables` table.
    $all_tables = $this->connection->query('SELECT owner, LOWER(table_name) as table_name FROM all_tables')->fetchAll();
    foreach ($all_tables as $table) {
      $table->table_name_prefixed = $this->oid($table->owner) . '.' . $this->oid($table->table_name);

      // Ignore all tables according to the prefix in the search expression.
      if ($expression_prefix && $table->owner !== $expression_prefix) {
        continue;
      }

      // Take into account tables that have an individual prefix.
      if (isset($individually_prefixed_tables[$table->table_name_prefixed]) &&
        $table->table_name === $individually_prefixed_tables[$table->table_name_prefixed]) {
        $tables[$table->table_name] = $table->table_name;
      }

      // This table name does not start the default prefix, which means that
      // it is not managed by Drupal so it should be excluded from the result.
      elseif ($default_prefix && $table->owner !== $default_prefix) {
        continue;
      }

      // Ignore all internal tables needed to operate of this driver.
      elseif (in_array($table->table_name, $this->driverTables, TRUE)) {
        continue;
      }

      $tables[$table->table_name] = $table->table_name;
    }

    // @todo: simpletest data truncating - add "only prefix" for user deletion.
    // @see EnvironmentCleaner::doCleanDatabase().
    // if ($table_expression === 'TEST%') {
    //   $all_tables = $this->connection->query("SELECT t.username FROM DBA_USERS t WHERE t.username LIKE 'TEST%'");
    // }

    // Convert the table expression from its SQL LIKE syntax to a regular
    // expression and escape the delimiter that will be used for matching.
    $table_expression = str_replace(['%', '_'], ['.*?', '.'], preg_quote($table_expression, '/'));
    $tables = preg_grep('/^' . $table_expression . '$/i', $tables);
    return array_map('strtolower', $tables);
  }

  /**
   * {@inheritdoc}
   */
  public function copyTable($source, $destination) {
    if (!$this->tableExists($source)) {
      throw new SchemaObjectDoesNotExistException(SafeMarkup::format("Cannot copy @source to @destination: table @source doesn't exist.", array('@source' => $source, '@destination' => $destination)));
    }
    if ($this->tableExists($destination)) {
      throw new SchemaObjectExistsException(SafeMarkup::format("Cannot copy @source to @destination: table @destination already exists.", array('@source' => $source, '@destination' => $destination)));
    }

    throw new DatabaseExceptionWrapper('Not implemented, see https://drupal.org/node/2056133.');
  }

  /**
   * Oracle schema helper.
   */
  protected function createColsSql($cols) {
    $return = array();
    foreach ($cols as $col) {
      if (is_array($col)) {
        $return[] = $this->oid($col[0]);
      }
      else {
        $return[] = $this->oid($col);
      }
    }
    return implode(', ', $return);
  }

  /**
   * Oracle schema helper.
   */
  private function getTableSerialInfo($table) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));
    return $this->connection->query("select * from table(identifier.get_serial(?, ?))", array(strtoupper($table), $schema))->fetchObject();
  }

  /**
   * Oracle schema helper.
   */
  protected function createIndexSql($table, $name, $fields) {
    $oname = $this->oid('IDX_' . $table . '_' . $name, TRUE);

    $sql = array();
    // Oracle doesn't like multiple indexes on the same column list.
    $ret = $this->dropIndexByColsSql($table, $fields);

    if ($ret) {
      $sql[] = $ret;
    }

    // Suppose we try to create two indexes in the same create table command we
    // will silently fail the second.
    $query = "begin execute immediate 'CREATE INDEX " . $oname . " ON " . $this->oid($table, TRUE) . " (";
    $query .= $this->createKeySql($fields) . ")'; exception when others then if sqlcode = -1408 then null; else raise; end if; end;";
    $sql[] = $query;

    return $sql;
  }

  /**
   * Oracle schema helper.
   */
  public function dropIndexByColsSql($table, $fields) {
    $schema = $this->tableSchema($this->connection->prefixTables('{' . $table . '}'));
    $stmt = $this->connection->queryOracle(
      "select i.index_name,
       e.column_expression exp,
       i.column_name col
       from all_ind_columns i,
       all_ind_expressions e
       where i.column_position= e.column_position (+)
       and i.index_owner = e.index_owner (+)
       and i.table_name = e.table_name (+)
       and i.index_name = e.index_name (+)
       and (i.index_name like 'IDX%' or i.index_name like '" . ORACLE_LONG_IDENTIFIER_PREFIX . "%')
       and i.table_name = ?
       and i.index_owner = ?
      ",
      array(strtoupper($table), $schema)
    );

    $idx = array();
    while ($row = $stmt->fetchObject()) {
      if (!isset($idx[$row->index_name])) {
        $idx[$row->index_name] = array();
      }
      $idx[$row->index_name][] = $row->exp ? $row->exp : $row->col;
    }

    $col = array();

    foreach ($fields as $field) {
      if (is_array($field)) {
        $col[] = 'SUBSTR(' . $this->oid($field[0]) . ',1,' . $field[1] . ')';
      }
      else {
        $col[] = $this->oid($field, FALSE, FALSE);
      }
    }

    foreach ($idx as $name => $value) {
      if (!count(array_diff($value, $col))) {
        return 'DROP INDEX "' . strtoupper($schema) . '"."' . strtoupper($name) . '"';
      }
    }

    return FALSE;
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeySql($fields) {
    $ret = array();
    foreach ($fields as $field) {
      if (is_array($field)) {
        $ret[] = 'substr(' . $this->oid($field[0]) . ', 1, ' . $field[1] . ')';
      }
      else {
        $ret[] = $this->oid($field);
      }
    }
    return implode(', ', $ret);
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeys($table, $new_keys) {
    if (isset($new_keys['primary key'])) {
      $this->addPrimaryKey($table, $new_keys['primary key']);
    }

    if (isset($new_keys['unique keys'])) {
      foreach ($new_keys['unique keys'] as $name => $fields) {
        $this->addUniqueKey($table, $name, $fields);
      }
    }

    if (isset($new_keys['indexes'])) {
      foreach ($new_keys['indexes'] as $name => $fields) {
        $this->addIndex($table, $name, $fields);
      }
    }
  }

  /**
   * Oracle schema helper.
   */
  public static function tableSchema($table) {
    $exp = explode('.', $table);

    if (count($exp) > 1) {
      return strtoupper(str_replace('"', '', $exp[0]));
    }
    return FALSE;
  }

  /**
   * Oracle schema helper.
   */
  private function cleanUpSchema($cache_table, $trigger_table = '') {
    if (!$trigger_table) {
      $trigger_table = $cache_table;
    }

    $this->resetLongIdentifiers();
    $this->removeTableInfoCache($cache_table);
    $this->rebuildDefaultsTrigger($trigger_table);
  }

}
