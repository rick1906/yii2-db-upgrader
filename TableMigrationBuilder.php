<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

/**
 *
 * @author Rick
 */
class TableMigrationBuilder extends MigrationBuilder
{

    /**
     * 
     * @var string
     */
    protected $table;

    /**
     * 
     * @var TableStructure
     */
    protected $tableStructure;

    /**
     * 
     * @param mixed $version
     * @param array $migrations
     * @param mixed $featureSince
     * @param string|TableStructure $table
     */
    public function __construct($version, &$migrations, $featureSince, $table)
    {
        if ($table instanceof TableStructure) {
            $this->table = $table->getTable();
            $this->tableStructure = $table;
        } else {
            $this->table = $table;
            $this->tableStructure = null;
        }
        parent::__construct($version, $migrations, $featureSince);
        $this->params['table'] = $this->table;
    }

    /**
     * 
     * @param string $name
     * @param array $arguments
     * @return $this
     */
    protected function addPredefinedTableMigration($name, $arguments)
    {
        array_unshift($arguments, $this->table);
        return $this->addPredefinedMigration($name, $arguments);
    }

    /**
     * Creates and executes an INSERT SQL statement.
     * The method will properly escape the column names, and bind the values to be inserted.
     * @param array $columns the column data (name => value) to be inserted into the table.
     */
    public function insert($columns)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Creates and executes a batch INSERT SQL statement.
     * The method will properly escape the column names, and bind the values to be inserted.
     * @param array $columns the column names.
     * @param array $rows the rows to be batch inserted into the table
     */
    public function batchInsert($columns, $rows)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Creates and executes a command to insert rows into a database table if
     * they do not already exist (matching unique constraints),
     * or update them if they do.
     *
     * The method will properly escape the column names, and bind the values to be inserted.
     *
     * @param array|Query $insertColumns the column data (name => value) to be inserted into the table or instance
     * of [[Query]] to perform `INSERT INTO ... SELECT` SQL statement.
     * @param array|bool $updateColumns the column data (name => value) to be updated if they already exist.
     * If `true` is passed, the column data will be updated to match the insert column data.
     * If `false` is passed, no update will be performed if the column data already exists.
     * @param array $params the parameters to be bound to the command.
     */
    public function upsert($insertColumns, $updateColumns = true, $params = [])
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Creates and executes an UPDATE SQL statement.
     * The method will properly escape the column names and bind the values to be updated.
     * @param array $columns the column data (name => value) to be updated.
     * @param array|string $condition the conditions that will be put in the WHERE part. Please
     * refer to [[Query::where()]] on how to specify conditions.
     * @param array $params the parameters to be bound to the query.
     */
    public function update($columns, $condition = '', $params = [])
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Creates and executes a DELETE SQL statement.
     * @param array|string $condition the conditions that will be put in the WHERE part. Please
     * refer to [[Query::where()]] on how to specify conditions.
     * @param array $params the parameters to be bound to the query.
     */
    public function delete($condition = '', $params = [])
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Builds and executes a SQL statement for truncating a DB table.
     */
    public function truncateTable()
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Builds and executes a SQL statement for adding a new DB column.
     * @param string $column the name of the new column. The name will be properly quoted by the method.
     * @param string $type the column type. The [[QueryBuilder::getColumnType()]] method will be invoked to convert abstract column type (if any)
     * into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     * For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     */
    public function addColumn($column, $type = null)
    {
        if ($type === null && $this->tableStructure !== null) {
            $type = $this->tableStructure->getUpgrader()->getColumnDefinitionString((string)$column, true);
            if ($type !== null) {
                return $this->addPredefinedTableMigration(__FUNCTION__, [$column, $type]);
            } else {
                throw new \LogicException("Nonexistent column definition for '{$column}'");
            }
        }
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args());
    }

    /**
     * Builds and executes a SQL statement for dropping a DB column.
     * @param string $column the name of the column to be dropped. The name will be properly quoted by the method.
     */
    public function dropColumn($column)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds and executes a SQL statement for renaming a column.
     * @param string $name the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     */
    public function renameColumn($name, $newName)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds and executes a SQL statement for changing the definition of a column.
     * @param string $column the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $type the new column type. The [[QueryBuilder::getColumnType()]] method will be invoked to convert abstract column type (if any)
     * into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     * For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     */
    public function alterColumn($column, $type = null)
    {
        if ($type === null && $this->tableStructure !== null) {
            $type = $this->tableStructure->getUpgrader()->getColumnDefinitionString((string)$column, false);
            if ($type !== null) {
                return $this->addPredefinedTableMigration(__FUNCTION__, [$column, $type], true);
            } else {
                throw new \LogicException("Nonexistent column definition for '{$column}'");
            }
        }
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds and executes a SQL statement for creating a primary key.
     * The method will properly quote the table and column names.
     * @param string $name the name of the primary key constraint.
     * @param string|array $columns comma separated string or array of columns that the primary key will consist of.
     */
    public function addPrimaryKey($name, $columns)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table, $columns]);
    }

    /**
     * Builds and executes a SQL statement for dropping a primary key.
     * @param string $name the name of the primary key constraint to be removed.
     */
    public function dropPrimaryKey($name)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table]);
    }

    /**
     * Builds a SQL statement for adding a foreign key constraint to an existing table.
     * The method will properly quote the table and column names.
     * @param string $name the name of the foreign key constraint.
     * @param string|array $columns the name of the column to that the constraint will be added on. If there are multiple columns, separate them with commas or use an array.
     * @param string $refTable the table that the foreign key references to.
     * @param string|array $refColumns the name of the column that the foreign key references to. If there are multiple columns, separate them with commas or use an array.
     * @param string $delete the ON DELETE option. Most DBMS support these options: RESTRICT, CASCADE, NO ACTION, SET DEFAULT, SET NULL
     * @param string $update the ON UPDATE option. Most DBMS support these options: RESTRICT, CASCADE, NO ACTION, SET DEFAULT, SET NULL
     */
    public function addForeignKey($name, $columns, $refTable, $refColumns, $delete = null, $update = null)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table, $columns, $refTable, $refColumns, $delete, $update]);
    }

    /**
     * Builds a SQL statement for dropping a foreign key constraint.
     * @param string $name the name of the foreign key constraint to be dropped. The name will be properly quoted by the method.
     */
    public function dropForeignKey($name)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table], true);
    }

    /**
     * Builds and executes a SQL statement for creating a new index.
     * @param string $name the name of the index. The name will be properly quoted by the method.
     * @param string|array $columns the column(s) that should be included in the index. If there are multiple columns, please separate them
     * by commas or use an array. Each column name will be properly quoted by the method. Quoting will be skipped for column names that
     * include a left parenthesis "(".
     * @param bool $unique whether to add UNIQUE constraint on the created index.
     */
    public function createIndex($name, $columns, $unique = false)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table, $columns, $unique]);
    }

    /**
     * Builds and executes a SQL statement for dropping an index.
     * @param string $name the name of the index to be dropped. The name will be properly quoted by the method.
     */
    public function dropIndex($name)
    {
        return $this->addPredefinedMigration(__FUNCTION__, [$name, $this->table], true);
    }

    /**
     * Builds and execute a SQL statement for adding comment to column.
     *
     * @param string $column the name of the column to be commented. The column name will be properly quoted by the method.
     * @param string $comment the text of the comment to be added. The comment will be properly quoted by the method.
     * @since 2.0.8
     */
    public function addCommentOnColumn($column, $comment)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds a SQL statement for adding comment to table.
     *
     * @param string $comment the text of the comment to be added. The comment will be properly quoted by the method.
     * @since 2.0.8
     */
    public function addCommentOnTable($comment)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds and execute a SQL statement for dropping comment from column.
     *
     * @param string $column the name of the column to be commented. The column name will be properly quoted by the method.
     * @since 2.0.8
     */
    public function dropCommentFromColumn($column)
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

    /**
     * Builds a SQL statement for dropping comment from table.
     *
     * @since 2.0.8
     */
    public function dropCommentFromTable()
    {
        return $this->addPredefinedTableMigration(__FUNCTION__, func_get_args(), true);
    }

}
