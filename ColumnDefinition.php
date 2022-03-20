<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use ReflectionMethod;
use ReflectionProperty;
use yii\db\ColumnSchemaBuilder;
use yii\db\Connection;
use yii\db\Schema;

/**
 *
 * @author Rick
 */
class ColumnDefinition
{

    /**
     * 
     * @var string
     */
    protected $name;

    /**
     * 
     * @var ColumnSchemaBuilder
     */
    protected $column;

    /**
     * 
     * @var array
     */
    protected $params;

    /**
     * 
     * @var Connection
     */
    protected $db;

    /**
     * 
     * @var string
     */
    protected $sqlAfterType = '';

    /**
     * 
     * @var array
     */
    private static $reflectionProperties = null;

    /**
     * 
     * @var array
     */
    private static $reflectionMethods = null;

    /**
     * 
     * @var int
     */
    private static $instanceCounter = 0;

    /**
     * 
     * @param Connection $db
     * @param ColumnSchemaBuilder $column
     * @param string $name
     * @param array $params
     */
    public function __construct($db, $column, $name, $params = array())
    {
        self::$instanceCounter++;
        $this->db = $db;
        $this->column = $column;
        $this->name = $name;
        $this->params = $params;
        $this->init();
    }

    public function __destruct()
    {
        self::$instanceCounter--;
        if (self::$instanceCounter <= 0) {
            self::$instanceCounter = 0;
            self::$reflectionMethods = null;
            self::$reflectionProperties = null;
        }
    }

    protected function init()
    {
        if ($this->isPrimaryKey() && $this->isAutoIncrement() && !$this->isPrimaryKeyType()) {
            $type = $newType = $this->getType();
            if ($type === Schema::TYPE_INTEGER) {
                $newType = $this->isUnsigned() ? Schema::TYPE_UPK : Schema::TYPE_PK;
            } elseif ($type === Schema::TYPE_BIGINT) {
                $newType = $this->isUnsigned() ? Schema::TYPE_UBIGPK : Schema::TYPE_BIGPK;
            }
            if ($type !== $newType) {
                $this->getColumnReflectionProperty('type')->setValue($this->column, $newType);
            }
        }
        if ($this->getPositionAfter() === true) {
            $this->getColumnReflectionProperty('after')->setValue($this->column, null);
            $this->params['after'] = true;
        }
    }

    public function getName()
    {
        return $this->name;
    }

    public function getColumnSchema()
    {
        return $this->column;
    }

    public function getDefinitionString()
    {
        return $this->generateDefinitionString($this->column);
    }

    public function getDefinitionStringReordered($after = false)
    {
        return $this->generateDefinitionString($this->getColumnSchemaReordered($after));
    }

    protected function generateDefinitionString($schema)
    {
        $parts = $this->getDefinitionParts($schema);
        $resolved = $this->resolveType($parts[0]);
        if ($resolved !== null) {
            unset($parts[0]);
            $parts = array_merge($resolved, $parts);
        }
        if ((string)$this->sqlAfterType !== '') {
            array_splice($parts, 1, 0, (string)$this->sqlAfterType);
        }
        return implode(' ', $parts);
    }

    protected function getDefinitionParts($schema)
    {
        $string = (string)$schema;
        $typeString = $this->getTypeWithLength();
        if (!strncmp($string, $typeString, strlen($typeString))) {
            $type = trim($typeString);
            $extra = trim(substr($string, strlen($typeString)));
            return $extra !== '' ? [$type, $extra] : [$type];
        }

        $parsed = $this->parseDefinitionString($string);
        return $parsed !== null ? $parsed : [$string];
    }

    protected function parseDefinitionString($string)
    {
        $match = null;
        if (preg_match('/^(\S+\s*(?:\(.+?\))?)(.*)$/i', trim($string), $match)) {
            $type = trim($match[1]);
            $extra = trim($match[2]);
            return $extra !== '' ? [$type, $extra] : [$type];
        } else {
            return null;
        }
    }

    protected function resolveType($typeString)
    {
        $queryBuilder = $this->db->getQueryBuilder();
        $normalizedType = preg_replace('/\s+\(/', '(', strtolower($typeString));
        $resolvedType = $queryBuilder->getColumnType($normalizedType);

        $npos = strpos($normalizedType, '(');
        $rpos = strpos($resolvedType, '(');
        if ($npos !== false && $rpos === false) {
            $typeId = trim(substr($normalizedType, 0, $npos));
            $cpos = strpos($normalizedType, ')', $npos);
            if (isset($queryBuilder->typeMap[$typeId]) && $cpos !== false) {
                $target = $queryBuilder->typeMap[$typeId];
                if (strpos($target, '(') === false) {
                    $lengthString = substr($normalizedType, $npos, $cpos - $npos + 1);
                    $qb = clone $queryBuilder;
                    $qb->typeMap[$typeId] = $target.preg_replace('/\s+/', '', $lengthString);
                    $resolvedType = $qb->getColumnType($normalizedType);
                }
            }
        }

        return $this->parseDefinitionString($resolvedType);
    }

    protected function getColumnSchemaReordered($after = false)
    {
        $clone = clone $this->column;
        $clone->after((string)$after !== '' ? $after : null);
        $this->getColumnReflectionProperty('isFirst')->setValue($clone, $after === '');
        return $clone;
    }

    public function getParams()
    {
        return $this->params;
    }

    public function getType()
    {
        return $this->getColumnValue('type');
    }

    public function getTypeWithLength()
    {
        $type = $this->getType();
        $lengthString = (string)$this->callColumnMethod('buildLengthString');
        if ($lengthString !== '') {
            $type .= $lengthString;
        }
        return $type;
    }

    public function getResolvedType()
    {
        $resolvedType = $this->resolveType($this->getTypeWithLength());
        return isset($resolvedType[0]) ? $resolvedType[0] : null;
    }

    public function getTypeCategory()
    {
        return $this->callColumnMethod(__FUNCTION__);
    }

    public function isPrimaryKeyType()
    {
        return $this->getTypeCategory() === ColumnSchemaBuilder::CATEGORY_PK;
    }

    public function isPrimaryKey()
    {
        return $this->isPrimaryKeyType() || !empty($this->params['primary']);
    }

    public function isAutoIncrement()
    {
        if ($this->isPrimaryKeyType()) {
            return true;
        }
        if (empty($this->params['primary'])) {
            return false;
        }
        if (isset($this->params['autoIncrement'])) {
            return (bool)$this->params['autoIncrement'];
        }
        return $this->canBeAutoIncrement();
    }

    protected function canBeAutoIncrement()
    {
        $category = $this->getTypeCategory();
        if ($category === ColumnSchemaBuilder::CATEGORY_STRING) {
            return false;
        }
        if ($category === ColumnSchemaBuilder::CATEGORY_TIME) {
            return false;
        }
        return true;
    }

    public function isUnique()
    {
        return (bool)$this->getColumnValue('isUnique');
    }

    public function isIndex()
    {
        return !empty($this->params['index']);
    }

    public function isUniqueIndex()
    {
        return $this->isUnique();
    }

    public function isPositionFirst()
    {
        return (bool)$this->getColumnValue('isFirst');
    }

    public function isPositionKeep()
    {
        return isset($this->params['after']) && $this->params['after'] === true;
    }

    public function getPositionAfter()
    {
        return $this->getColumnValue('after');
    }

    public function isNotNull()
    {
        return $this->getColumnValue('isNotNull');
    }

    public function isUnsigned()
    {
        return $this->getColumnValue('isUnsigned');
    }

    public function getDefaultValue()
    {
        return $this->getColumnValue('default');
    }

    public function getCharset()
    {
        return isset($this->params['charset']) ? $this->params['charset'] : null;
    }

    public function getCollation()
    {
        return isset($this->params['collation']) ? $this->params['collation'] : null;
    }

    public function isBinary()
    {
        return !empty($this->params['binary']);
    }

    public function insertSqlAfterType($sql)
    {
        $sqlString = is_array($sql) ? implode(' ', $sql) : (string)$sql;
        if ($sqlString !== '') {
            $this->sqlAfterType = ltrim($this->sqlAfterType.' '.$sqlString);
        }
    }

    public function appendExtraSql($sql)
    {
        $sqlString = is_array($sql) ? implode(' ', $sql) : (string)$sql;
        if ($sqlString !== '') {
            $append = ltrim($this->getColumnValue('append').' '.$sqlString);
            $this->getColumnReflectionProperty('append')->setValue($this->column, $append);
        }
    }

    private static function getReflectionMethod($key, $name, $class)
    {
        if (isset(self::$reflectionMethods[$key])) {
            $method = self::$reflectionMethods[$key];
        } else {
            $method = new ReflectionMethod($class, $name);
            $method->setAccessible(true);
            self::$reflectionMethods[$key] = $method;
        }
        return $method;
    }

    private static function getReflectionProperty($key, $name, $class)
    {
        if (isset(self::$reflectionProperties[$key])) {
            $property = self::$reflectionProperties[$key];
        } else {
            $property = new ReflectionProperty($class, $name);
            $property->setAccessible(true);
            self::$reflectionProperties[$key] = $property;
        }
        return $property;
    }

    protected function getColumnReflectionMethod($name)
    {
        return self::getReflectionMethod($name, $name, ColumnSchemaBuilder::class);
    }

    protected function getColumnReflectionProperty($name)
    {
        return self::getReflectionProperty($name, $name, ColumnSchemaBuilder::class);
    }

    protected function callColumnMethod($name)
    {
        $method = $this->getColumnReflectionMethod($name);
        return $method->invokeArgs($this->column, array_slice(func_get_args(), 1));
    }

    protected function getColumnValue($name)
    {
        $property = $this->getColumnReflectionProperty($name);
        return $property->getValue($this->column);
    }

}
