<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use yii\db\ColumnSchemaBuilder;
use yii\db\Connection;
use yii\db\QueryBuilder;
use yii\db\TableSchema;

/**
 *
 * @author Rick
 */
class TableUpgrader
{

    /**
     * @param Connection $db
     * @param string $table
     * @param array $columns
     * @param array $indexes
     * @param array $options
     */
    public static function create($db, $table, $columns, $indexes = [], $options = [])
    {
        if ($db->getSchema() instanceof \yii\db\mysql\Schema) {
            return new mysql\TableUpgrader($db, $table, $columns, $indexes, $options);
        }
        return new TableUpgrader($db, $table, $columns, $indexes, $options);
    }

    /**
     * 
     * @var string
     */
    protected $table;

    /**
     * 
     * @var Connection
     */
    protected $db;

    /**
     * 
     * @var TableSchema
     */
    protected $tableSchema;

    /**
     * 
     * @var QueryBuilder
     */
    protected $rawQueryBuilder;

    /**
     * 
     * @var array
     */
    protected $columns;

    /**
     * 
     * @var array
     */
    protected $indexes;

    /**
     * 
     * @var array
     */
    protected $options;

    /**
     * 
     * @var array
     */
    protected static $defaultOptions = [
        'updateCharset' => false,
        'usePreferredCollation' => true,
        'dropIndexes' => false,
    ];

    /**
     * @param Connection $db
     * @param string $table
     * @param array $columns
     * @param array $indexes
     * @param array $options
     */
    public function __construct($db, $table, $columns, $indexes = [], $options = [])
    {
        $this->db = $db;
        $this->table = $table;
        $this->options = (array)$options + (array)static::$defaultOptions;
        $this->tableSchema = $this->db->getTableSchema($this->table, true);
        $this->rawQueryBuilder = $this->db->getSchema()->createQueryBuilder();
        $this->rawQueryBuilder->typeMap = [];
        $this->columns = $this->prepareColumns($columns);
        $this->indexes = $this->prepareIndexes($indexes);
    }

    public function refresh()
    {
        $this->tableSchema = $this->db->getTableSchema($this->table, true);
    }

    public function execute()
    {
        $count = 0;
        foreach ($this->getCommands() as $command) {
            $command->execute();
            $count++;
        }
        return $count;
    }

    public function getTableName()
    {
        return $this->table;
    }

    public function getRawTableName()
    {
        return $this->db->getSchema()->getRawTableName($this->table);
    }

    public function needsCreate()
    {
        return $this->tableSchema === null;
    }

    public function tableExists()
    {
        return $this->tableSchema !== null;
    }

    protected function createCommand($sql, $refreshTableSchema = true)
    {
        $command = $this->db->createCommand($sql);
        if ($refreshTableSchema) {
            $method = new \ReflectionMethod($command, 'requireTableSchemaRefresh');
            $method->setAccessible(true);
            $method->invoke($command, $this->table);
        }
        return $command;
    }

    protected function prepareColumns($columns)
    {
        $typeMap = $this->db->getQueryBuilder()->typeMap;
        $definitions = [];
        foreach ($columns as $name => $params) {
            $definition = $this->prepareColumn($name, $params, $typeMap);
            if ($definition && $definition instanceof ColumnDefinition) {
                $definitions[$definition->getName()] = $definition;
            }
        }
        return $definitions;
    }

    protected function createColumnDefinition($column, $name, $params = [])
    {
        $definition = new ColumnDefinition($this->db, $column, $name, $params);
        $definition->insertSqlAfterType($this->getColumnCharsetOptions($definition));
        $definition->appendExtraSql($this->getColumnExtraOptions($definition));
        return $definition;
    }

    protected function prepareColumn($name, $params, $typeMap = [])
    {
        if ($params instanceof ColumnDefinition) {
            return $params;
        }
        if ($params instanceof ColumnSchemaBuilder) {
            return $this->createColumnDefinition($params, $name);
        }

        if (isset($params['type'])) {
            $typeString = $params['type'];
        } elseif (isset($params[0])) {
            $typeString = $params[0];
        } else {
            return false;
        }
        if (is_int($name) && isset($params['name'])) {
            $name = $params['name'];
        }
        if ($typeString instanceof ColumnSchemaBuilder) {
            return $this->createColumnDefinition($typeString, $name, $params);
        }

        $match = null;
        if (preg_match('/^(.+)\((.+?)\)\s*$/', $typeString, $match)) {
            $type = trim($match[1]);
            $length = trim($match[2]);
        } else {
            $type = trim($typeString);
            $length = isset($params['length']) ? $params['length'] : null;
        }
        if (!isset($typeMap[$type])) {
            $typeVal = strtolower($type);
            $typeKey = array_search($typeVal, $typeMap, true);
            if ($typeKey !== false) {
                $type = $typeKey;
            }
        }

        $column = $this->db->getSchema()->createColumnSchemaBuilder($type, $length);
        if (isset($params['notNull'])) {
            $params['notNull'] ? $column->notNull() : $column->null();
        } elseif (isset($params['null'])) {
            $params['null'] ? $column->null() : $column->notNull();
        }
        if (!empty($params['unique'])) {
            $column->unique();
        }
        if (isset($params['check'])) {
            $column->check($params['check']);
        }
        if (isset($params['defaultValue'])) {
            $column->defaultValue($params['defaultValue']);
        } elseif (isset($params['default'])) {
            $column->defaultValue($params['default']);
        } elseif (isset($params['defaultExpression'])) {
            $column->defaultExpression($params['defaultExpression']);
        } elseif (is_array($params)) { // cheking NULL default value
            if (array_key_exists('defaultValue', $params)) {
                $column->defaultValue($params['defaultValue']);
            } elseif (array_key_exists('default', $params)) {
                $column->defaultValue($params['default']);
            }
        }
        if (isset($params['comment'])) {
            $column->comment($params['comment']);
        }
        if (!empty($params['unsigned'])) {
            $column->unsigned();
        }
        if (isset($params['after'])) {
            $column->after($params['after']);
        }
        if (!empty($params['first']) || !empty($params['afterFirst'])) {
            $column->first();
        }
        if (isset($params['append'])) {
            $column->append($params['append']);
        }

        return $this->createColumnDefinition($column, $name, $params);
    }

    protected function prepareIndexes($indexes)
    {
        $definitions = [];
        foreach ($indexes as $name => $params) {
            $definition = $this->prepareIndex($name, $params);
            if ($definition && !empty($definition['columns'])) {
                $definitions[$definition['name']] = $definition;
            }
        }
        return $definitions;
    }

    protected function prepareIndex($name, $params)
    {
        $columns = array();
        $definition = array('unique' => false, 'primary' => false);
        if (is_array($params)) {
            $definition = $params + $definition;
            if (isset($params['name'])) {
                $name = $params['name'];
            }
            if (isset($params['columns'])) {
                $columns = (array)$params['columns'];
            } else {
                foreach ($params as $key => $column) {
                    if (is_int($key)) {
                        $columns[] = $column;
                    }
                }
                if (empty($columns)) {
                    $columns[] = $name;
                }
            }
        } elseif (is_int($name)) {
            $name = $params;
            $columns[] = (string)$params;
        } elseif (is_string($params)) {
            $columns[] = (string)$params;
        } else {
            $columns[] = $name;
        }

        $definition['name'] = $name;
        $definition['columns'] = $columns;
        return $definition;
    }

    public function getOptionsString()
    {
        $sql = $this->buildOptions($this->options);
        if (is_array($sql)) {
            return implode(' ', $sql);
        } else {
            return (string)$sql;
        }
    }

    protected function buildOptions($options)
    {
        return [];
    }

    public function getColumnsDefinitions()
    {
        return $this->columns;
    }

    public function reorderColumns($definitions)
    {
        $map = [];
        $toOrder = [];
        $noOrder = [];
        $reordered = [];
        $existingMap = [];

        if ($this->tableSchema) {
            $target = false;
            foreach ($this->tableSchema->getColumnNames() as $name) {
                if (isset($definitions[$name])) {
                    $target = $name;
                    continue;
                }
                if ($target !== false) {
                    $existingMap[$target][] = $name;
                } else {
                    $noOrder[$name] = $name;
                }
            }
        }

        $prev = null;
        foreach ($definitions as $name => $definition) {
            if ($definition->isPositionFirst()) {
                $reordered[] = $name;
            } else {
                $after = (string)$definition->getPositionAfter();
                if ($after === '' && $definition->isPositionKeep()) {
                    if ($prev !== null) {
                        $after = $prev;
                    } else {
                        $reordered[] = $name;
                        continue;
                    }
                }
                if ($after !== '') {
                    $map[$after][] = $name;
                    $toOrder[$name] = $name;
                } else {
                    $noOrder[$name] = $name;
                }
            }
            if (isset($existingMap[$name])) {
                foreach ($existingMap[$name] as $n) {
                    $noOrder[$n] = $n;
                }
            }
            $prev = $name;
        }

        $ix = 0;
        while ($toOrder || $noOrder) {
            if (isset($reordered[$ix])) {
                $current = $reordered[$ix];
            } else {
                if ($noOrder) {
                    $current = array_shift($noOrder);
                } elseif ($toOrder) {
                    $current = array_shift($toOrder);
                } else {
                    break;
                }
                $reordered[$ix] = $current;
            }
            if (isset($map[$current])) {
                $k = 0;
                foreach ($map[$current] as $name) {
                    if (isset($toOrder[$name])) {
                        array_splice($reordered, $ix + (++$k), 0, $name);
                        unset($toOrder[$name]);
                    }
                }
            }
            $ix++;
        }

        return $reordered;
    }

    public function getColumnsDefinitionsReordered()
    {
        $definitions = $this->getColumnsDefinitions();
        $reordered = $this->reorderColumns($definitions);
        $items = [];
        foreach ($reordered as $name) {
            if (isset($definitions[$name])) {
                $items[$name] = $definitions[$name];
            }
        }
        return $items;
    }

    public function getColumnDefinitionsStrings($newTable = false)
    {
        $items = [];
        foreach ($this->getColumnsDefinitionsReordered() as $name => $definition) {
            $items[$name] = $this->getColumnCreateDefinitionString($definition, $newTable ? null : false);
        }
        return $items;
    }

    /**
     * 
     * @param ColumnDefinition $definition
     * @return array
     */
    protected function getColumnExtraOptions($definition)
    {
        return [];
    }

    /**
     * 
     * @param ColumnDefinition $definition
     * @return array
     */
    protected function getColumnCharsetOptions($definition)
    {
        $options = [];

        list($charset, $collation) = $this->getColumnCharsetAndCollation($definition);
        if ($charset !== null || $collation !== null) {
            $sql = $this->buildColumnCharsetDefinition($charset, $collation);
            if ($sql !== null) {
                $options[] = $sql;
            }
        }

        return $options;
    }

    /**
     * 
     * @param ColumnDefinition $definition
     * @return array
     */
    protected function getColumnCharsetAndCollation($definition)
    {
        $isText = $definition->getTypeCategory() === ColumnSchemaBuilder::CATEGORY_STRING;
        $charset = $definition->getCharset();
        $collation = $definition->getCollation();
        if ($charset !== null || $collation !== null || $isText) {
            list($charset, $collation) = $this->applyCharsetDefaults($definition->getCharset(), $definition->getCollation());
        }
        if ($definition->isBinary()) {
            $collation = $this->getBinaryCollation($this->normalizeCharset($charset));
        }
        return array($charset, $collation);
    }

    protected function applyCharsetDefaults($charset, $collation)
    {
        if ($charset === true || $charset === null && !empty($this->options['updateCharset'])) {
            $charset = $this->getTableCharset();
        }
        if ($collation === true || $collation === null && !empty($this->options['usePreferredCollation'])) {
            if ($charset === null) {
                $charset = $this->getTableCharset();
            }
            $collation = $this->getPreferredCollation($this->normalizeCharset($charset));
        }
        return [$charset, $collation];
    }

    public function getDefaultCharset()
    {
        return $this->db->charset;
    }

    public function getTableCharset()
    {
        if (isset($this->options['charset']) && $this->options['charset'] !== true) {
            return $this->options['charset'];
        }
        return $this->getDefaultCharset();
    }

    protected function normalizeCharset($charset)
    {
        return $charset !== null ? $charset : $this->getTableCharset();
    }

    protected function getBinaryCollation($charsetId)
    {
        return null;
    }

    protected function getPreferredCollation($charsetId)
    {
        return null;
    }

    /**
     * 
     * @param string $charset
     * @param string|bool $collation
     * @return string
     */
    protected function buildColumnCharsetDefinition($charset, $collation = null)
    {
        return null;
    }

    protected function generateIndexForColumn($name, $unique = false, $primary = false)
    {
        return $this->prepareIndex($name, array('columns' => [$name], 'primary' => $primary, 'unique' => $primary || $unique));
    }

    public function getIndexesDefinitions($newTable = false)
    {
        $items = [];
        foreach ($this->getColumnsDefinitions() as $name => $columnDefinition) {
            if ($this->isIndexDefinitionNeeded($name, $columnDefinition, $newTable)) {
                $definition = $this->generateIndexForColumn($name, $columnDefinition->isUniqueIndex(), $columnDefinition->isPrimaryKey());
                $items[$definition['name']] = $definition;
            }
        }
        foreach ($this->indexes as $name => $definition) {
            $items[$name] = $definition;
        }
        return $items;
    }

    protected function isIndexDefinitionNeeded($name, $columnDefinition, $newTable = false)
    {
        if ($columnDefinition->isPrimaryKey() && !$columnDefinition->isPrimaryKeyType()) {
            return true;
        }
        if ($columnDefinition->isIndex() && !$columnDefinition->isUnique()) {
            return true;
        }
        if ($columnDefinition->isUnique() && !$newTable && $this->tableSchema && !empty($this->tableSchema->columns[$name])) {
            return true;
        }
        return false;
    }

    protected function getCreateTableCommand($table, $columns, $optionsString)
    {
        if ((string)$optionsString !== '') {
            return $this->createCommand($this->rawQueryBuilder->createTable($table, $columns, $optionsString));
        } else {
            return $this->createCommand($this->rawQueryBuilder->createTable($table, $columns));
        }
    }

    public function getCreateTableCommands()
    {
        $columns = $this->getColumnDefinitionsStrings(true);
        $optionsString = $this->getOptionsString();
        return [$this->getCreateTableCommand($this->table, $columns, $optionsString)];
    }

    protected function getCreateIndexCommand($name, $definition)
    {
        if (!empty($definition['primary'])) {
            return $this->db->createCommand()->addPrimaryKey($name, $this->table, $definition['columns']);
        } else {
            return $this->db->createCommand()->createIndex($name, $this->table, $definition['columns'], $definition['unique']);
        }
    }

    protected function getDropIndexCommand($name, $primary = false)
    {
        if ($primary) {
            return $this->db->createCommand()->dropPrimaryKey($name, $this->table);
        } else {
            return $this->db->createCommand()->dropIndex($name, $this->table);
        }
    }

    public function getCreateIndexesCommands()
    {
        $commands = [];
        foreach ($this->getIndexesDefinitions(true) as $name => $definition) {
            $commands[] = $this->getCreateIndexCommand($name, $definition);
        }
        return $commands;
    }

    protected function getStoredColumnsInfo()
    {
        return false;
    }

    protected function getStoredIndexesInfo()
    {
        return false;
    }

    protected function getStoredOptionsInfo()
    {
        return false;
    }

    public function getUpgradeTableCommands()
    {
        $commands = [];
        foreach ($this->getUpgradeOptionsCommands() as $command) {
            $commands[] = $command;
        }
        foreach ($this->getUpgradeColumnsCommands() as $command) {
            $commands[] = $command;
        }
        return $commands;
    }

    public function getUpgradeIndexesCommands()
    {
        $allCommands = [];
        $upgrades = $this->getIndexesUpgrades($this->getIndexesDefinitions());
        foreach ($upgrades as $name => $upgrade) {
            list($newIndex, $oldIndex, $hasInfo) = $upgrade;
            $commands = $this->getUpgradeIndexCommands($name, $newIndex, $oldIndex, $hasInfo);
            foreach ($commands as $command) {
                $allCommands[] = $command;
            }
        }
        return $allCommands;
    }

    protected function getUpgradeIndexCommands($name, $newIndex, $oldIndex, $hasInfo = true)
    {
        $commands = [];
        if (!$hasInfo) {
            return $commands;
        }
        if ($newIndex && !$oldIndex) {
            $commands[] = $this->getCreateIndexCommand($name, $newIndex);
        } elseif ($newIndex && $oldIndex) {
            $attrs = $this->getStoredIndexAttrs($oldIndex);
            if (isset($attrs['name'])) {
                $commands[] = $this->getDropIndexCommand($attrs['name'], !empty($attrs['primary']));
                $commands[] = $this->getCreateIndexCommand($name, $newIndex);
            }
        } elseif ($oldIndex && !$newIndex && !empty($this->options['dropIndexes'])) {
            $attrs = $this->getStoredIndexAttrs($oldIndex);
            if (isset($attrs['name'])) {
                $commands[] = $this->getDropIndexCommand($attrs['name'], !empty($attrs['primary']));
            }
        }
        return $commands;
    }

    protected function getUpgradeOptionsCommands()
    {
        $commands = [];
        $upgrades = $this->getOptionsUpgrade();
        if ($upgrades) {
            $command = $this->getAlterTableCommand($this->getOptionsString());
            if ($command) {
                $commands[] = $command;
            }
        }
        return $commands;
    }

    protected function getAlterTableCommand($optionsString)
    {
        return null;
    }

    protected function getUpgradeColumnsCommands()
    {
        $newColumns = $this->getColumnsDefinitions();
        $upgrades = $this->getColumnsUpgrades($newColumns);
        $orderUpgrades = $this->getColumnsOrderUpgrades($newColumns);
        $names = array_keys($orderUpgrades + $upgrades);
        $dropOldPrimaryKeyCommand = null;
        $dropOldAutoIncrementCommand = null;
        $allCommands = [];
        foreach ($names as $name) {
            $newColumn = $newColumns[$name];
            $upgrade = isset($upgrades[$name]) ? $upgrades[$name] : false;
            $orderUpgrade = isset($orderUpgrades[$name]) ? $orderUpgrades[$name] : false;
            $commands = $this->getUpgradeColumnCommands($name, $newColumn, $upgrade, $orderUpgrade);
            foreach ($commands as $command) {
                $allCommands[] = $command;
            }
            if ($newColumn && $upgrade && !empty($upgrade[1])) { // drop old pk & ai
                $schemaColumn = $upgrade[1];
                if (!$schemaColumn->isPrimaryKey && $newColumn->isPrimaryKeyType() && $dropOldPrimaryKeyCommand === null) {
                    $dropOldPrimaryKeyCommand = $this->getDropPrimaryKeyCommand();
                }
                if ($schemaColumn->autoIncrement && !$newColumn->isAutoIncrement()) {
                    $definitionString = $this->getColumnUpgradeDefinitionString($newColumn);
                    $dropOldAutoIncrementCommand = $this->getAlterColumnCommand($name, $definitionString);
                }
            }
        }
        if ($dropOldAutoIncrementCommand) {
            array_unshift($allCommands, $dropOldAutoIncrementCommand);
        }
        if ($dropOldPrimaryKeyCommand) {
            array_unshift($allCommands, $dropOldPrimaryKeyCommand);
        }
        return $allCommands;
    }

    public function getColumnDefinitionString($name, $create = false)
    {
        if (isset($this->columns[$name])) {
            if ($create) {
                return $this->getColumnCreateDefinitionString($this->columns[$name]);
            } else {
                return $this->getColumnUpgradeDefinitionString($this->columns[$name]);
            }
        }
        return null;
    }

    protected function getColumnUpgradeDefinitionString($definition, $after = null)
    {
        return $definition->getDefinitionStringReordered($after);
    }

    protected function getColumnCreateDefinitionString($definition, $after = null)
    {
        return $after === null ? $definition->getDefinitionString() : $definition->getDefinitionStringReordered($after);
    }

    protected function getDropPrimaryKeyCommand()
    {
        $indexes = $this->getStoredIndexesInfo();
        if ($indexes !== false) {
            foreach ($indexes as $index) {
                $attrs = $this->getStoredIndexAttrs($index);
                if (isset($attrs['name']) && !empty($attrs['primary'])) {
                    return $this->getDropIndexCommand($attrs['name'], true);
                }
            }
        } elseif ($this->tableSchema && $this->tableSchema->primaryKey && count($this->tableSchema->primaryKey) === 1) {
            return $this->db->createCommand()->dropPrimaryKey($this->tableSchema->primaryKey[0], $this->table);
        }
        return null;
    }

    protected function getUpgradeColumnCommands($name, $definition, $upgrade, $positionAfter)
    {
        $commands = [];
        if ($upgrade) {
            list($newColumn, $schemaColumn) = $upgrade;
            if ($schemaColumn) {
                $definitionString = $this->getColumnUpgradeDefinitionString($newColumn, $positionAfter);
                $commands[] = $this->getAlterColumnCommand($name, $definitionString);
            } else {
                $definitionString = $this->getColumnCreateDefinitionString($newColumn, $positionAfter);
                $commands[] = $this->getAddColumnCommand($name, $definitionString);
            }
        } else {
            $definitionString = $this->getColumnUpgradeDefinitionString($definition, $positionAfter);
            $commands[] = $this->getAlterColumnCommand($name, $definitionString);
        }
        return $commands;
    }

    protected function getAlterColumnCommand($name, $definitionString)
    {
        return $this->createCommand($this->rawQueryBuilder->alterColumn($this->table, $name, $definitionString));
    }

    protected function getAddColumnCommand($name, $definitionString)
    {
        return $this->createCommand($this->rawQueryBuilder->addColumn($this->table, $name, $definitionString));
    }

    protected function getOptionsUpgrade()
    {
        $oldOptions = $this->getStoredOptionsInfo();
        if ($oldOptions !== false && $this->checkOptionsNeedUpgrade($this->options, $oldOptions)) {
            return [$this->options, $oldOptions];
        }
        return null;
    }

    protected function checkOptionsNeedUpgrade($newOptions, $oldOptions)
    {
        return false;
    }

    protected function getColumnsUpgrades($newColumns)
    {
        $oldColumns = $this->getStoredColumnsInfo();
        $schemaColumns = $this->tableSchema ? $this->tableSchema->columns : [];
        $upgrades = [];
        foreach ($newColumns as $name => $newColumn) {
            $schemaColumn = isset($schemaColumns[$name]) ? $schemaColumns[$name] : null;
            $oldColumn = isset($oldColumns[$name]) ? $oldColumns[$name] : ($schemaColumn !== null ? false : null);
            if ($schemaColumn === null || $this->checkColumnNeedsUpgrade($newColumn, $schemaColumn, $oldColumn)) {
                $upgrades[$name] = [$newColumn, $schemaColumn, $oldColumn];
            }
        }
        return $upgrades;
    }

    protected function getColumnsOrderUpgrades($newColumns)
    {
        $schemaColumns = $this->tableSchema ? $this->tableSchema->columns : [];
        $currentOrder = array_keys($schemaColumns);
        $reordered = $this->reorderColumns($newColumns);

        $upgrades = [];
        $ending = false;
        foreach ($reordered as $i => $name) {
            if (isset($newColumns[$name])) {
                $prev = $i > 0 ? $reordered[$i - 1] : '';
                $oldIndex = array_search($name, $currentOrder);
                if ($oldIndex !== false) {
                    $oldPrev = $oldIndex > 0 ? $currentOrder[$oldIndex - 1] : '';
                } elseif ($ending) {
                    $oldPrev = $prev;
                } else {
                    $ending = true;
                    for ($j = $i + 1; $j < count($reordered); ++$j) {
                        if (isset($schemaColumns[$reordered[$j]])) {
                            $ending = false;
                            break;
                        }
                    }
                    if ($ending) {
                        $oldPrev = $prev;
                    }
                }
                if ($oldPrev !== $prev) {
                    if ($oldIndex !== false) {
                        array_splice($currentOrder, $oldIndex, 1);
                    }
                    $targetIndex = array_search($prev, $currentOrder);
                    if ($targetIndex !== false) {
                        array_splice($currentOrder, $targetIndex + 1, 0, $name);
                    }
                    $upgrades[$name] = $prev;
                }
            }
        }
        return $upgrades;
    }

    /**
     * 
     * @param ColumnDefinition $newColumn
     * @param \yii\db\ColumnSchema $schemaColumn
     * @param mixed $oldColumn
     * @return bool
     */
    protected function checkColumnNeedsUpgrade($newColumn, $schemaColumn, $oldColumn)
    {
        if ($this->checkColumnTypeChanged($newColumn, $schemaColumn, $oldColumn)) {
            return true;
        }
        if ($this->checkColumnDefaultValueChanged($newColumn, $schemaColumn, $oldColumn)) {
            return true;
        }
        if ($this->checkColumnPrimaryKeyChanged($newColumn, $schemaColumn, $oldColumn)) {
            return true;
        }

        $isNotNull = $newColumn->isNotNull();
        if ($isNotNull !== null && (!$isNotNull) !== $schemaColumn->allowNull) {
            return true;
        }

        $isUnsigned = $newColumn->isUnsigned();
        if ($isUnsigned !== null && $isUnsigned !== $schemaColumn->unsigned) {
            return true;
        }

        $comment = $newColumn->getColumnSchema()->comment;
        if ((string)$comment !== (string)$schemaColumn->comment) {
            return true;
        }

        return false;
    }

    /**
     * 
     * @param ColumnDefinition $newColumn
     * @param \yii\db\ColumnSchema $schemaColumn
     * @param mixed $oldColumn
     * @return bool
     */
    protected function checkColumnPrimaryKeyChanged($newColumn, $schemaColumn, $oldColumn)
    {
        if ($newColumn->isPrimaryKey() !== $schemaColumn->isPrimaryKey) {
            return true;
        }
        if ($newColumn->isAutoIncrement() !== $schemaColumn->autoIncrement) {
            return true;
        }
        return false;
    }

    /**
     * 
     * @param ColumnDefinition $newColumn
     * @param \yii\db\ColumnSchema $schemaColumn
     * @param mixed $oldColumn
     * @return bool
     */
    protected function checkColumnTypeChanged($newColumn, $schemaColumn, $oldColumn)
    {
        $type = $newColumn->getTypeWithLength();
        $resolvedType = $newColumn->getResolvedType();
        if ($resolvedType === null) {
            return (bool)strcasecmp($type, $schemaColumn->dbType);
        }

        $dbTypeParts = preg_split('/\s+/', $schemaColumn->dbType, 2);
        $dbType = $dbTypeParts[0];
        if (!strcasecmp($resolvedType, $schemaColumn->dbType)) {
            return false;
        }
        if (!strcasecmp($resolvedType, $dbType)) {
            return false;
        }
        if (!strcasecmp(preg_replace('/\s+/', '', $resolvedType), $dbType)) {
            return false;
        }

        return true;
    }

    /**
     * 
     * @param ColumnDefinition $newColumn
     * @param \yii\db\ColumnSchema $schemaColumn
     * @param mixed $oldColumn
     * @return bool
     */
    protected function checkColumnDefaultValueChanged($newColumn, $schemaColumn, $oldColumn)
    {
        $defaultValue = $newColumn->getDefaultValue();
        if (is_object($defaultValue)) {
            return !is_object($schemaColumn->defaultValue) || (string)$defaultValue !== (string)$schemaColumn->defaultValue;
        } if (is_numeric($defaultValue)) {
            return !is_numeric($schemaColumn->defaultValue) || (float)$defaultValue !== (float)$schemaColumn->defaultValue;
        } elseif ((string)$defaultValue === '') {
            return $defaultValue !== $schemaColumn->defaultValue;
        } else {
            return (string)$defaultValue !== (string)$schemaColumn->defaultValue;
        }
    }

    protected function getIndexesUpgrades($newIndexes)
    {
        $oldIndexes = $this->getStoredIndexesInfo();
        $upgrades = [];
        foreach ($newIndexes as $name => $newIndex) {
            if ($oldIndexes === false) {
                $upgrades[$name] = [$newIndex, null, false];
            } else {
                $oldName = $this->getCorrespondingIndexKey($name, $newIndex, $oldIndexes);
                $oldIndex = isset($oldIndexes[$oldName]) ? $oldIndexes[$oldName] : null;
                if ($oldIndex === null || $this->checkIndexNeedsUpgrade($newIndex, $oldIndex)) {
                    $upgrades[$name] = [$newIndex, $oldIndex, true];
                }
                if ($oldName !== null) {
                    unset($oldIndexes[$oldName]);
                }
            }
        }
        if ($oldIndexes !== false) {
            $upgrades += $this->getDropIndexesUpgrades($oldIndexes, $newIndexes);
        }
        return $upgrades;
    }

    protected function getDropIndexesUpgrades($oldIndexes, $newIndexes)
    {
        $upgrades = [];
        foreach ($oldIndexes as $oldName => $oldIndex) {
            $attrs = $this->getStoredIndexAttrs($oldIndex);
            if (!empty($attrs['primary'])) {
                continue; // old primary keys are either replaced in indexes upgrades or dropped in column upgrades
            }
            if (!isset($newIndexes[$oldName]) && isset($attrs['columns'])) {
                if (count($attrs['columns']) === 1) {
                    $columnName = $attrs['columns'][0];
                    if (isset($this->columns[$columnName])) {
                        $definition = $this->columns[$columnName];
                        if ($definition->isPrimaryKey()) {
                            continue;
                        }
                        if ($oldName === $columnName && ($definition->isIndex() || $definition->isUniqueIndex())) {
                            continue;
                        }
                    }
                }
                $upgrades[$oldName] = [null, $oldIndex, true];
            }
        }
        return $upgrades;
    }

    protected function checkIndexNeedsUpgrade($newIndex, $oldIndex)
    {
        return false;
    }

    protected function getStoredIndexAttrs($storedIndex)
    {
        return [
            'name' => null,
            'columns' => null,
            'unique' => false,
            'primary' => false,
        ];
    }

    protected function getCorrespondingIndexKey($name, $definition, $storedIndexes = null)
    {
        return $name;
    }

    public function getCommands()
    {
        if ($this->tableExists()) {
            $commands1 = $this->getUpgradeTableCommands();
            $commands2 = $this->getUpgradeIndexesCommands();
            return array_merge($commands1, $commands2);
        } else {
            $commands1 = $this->getCreateTableCommands();
            $commands2 = $this->getCreateIndexesCommands();
            return array_merge($commands1, $commands2);
        }
    }

}
