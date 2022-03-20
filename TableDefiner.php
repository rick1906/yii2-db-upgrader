<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use InvalidArgumentException;

/**
 *
 * @author Rick
 */
abstract class TableDefiner implements TableDefinerInterface
{

    public static function getTableDefinitions()
    {
        return (new static())->getDefinitions();
    }

    /**
     * 
     * @var DatabaseUpgrader|null
     */
    protected $dbUpgrader;

    /**
     * 
     * @var array
     */
    protected $definitions = [];

    public final function __construct($dbUpgrader = null)
    {
        $this->dbUpgrader = $dbUpgrader;
        $this->defineTables();
    }

    protected abstract function defineTables();

    protected function createTableStructure($table, $config = [])
    {
        if ($this->dbUpgrader !== null) {
            $config['dbUpgrader'] = $this->dbUpgrader;
        }
        return new TableStructure($table, $config);
    }

    public function defineTable($name, $definition)
    {
        if ($definition instanceof TableStructure) {
            $this->definitions[$name] = $definition;
            return $definition;
        }
        if (is_callable($definition)) {
            $ts = $this->createTableStructure($name);
            call_user_func($definition, $ts);
            $this->definitions[$name] = $ts;
            return $ts;
        }
        if (is_array($definition)) {
            $ts = $this->createTableStructure($name, $definition);
            $this->definitions[$name] = $ts;
            return $ts;
        }
        throw new InvalidArgumentException("Invalid definition of table '{$name}'");
    }

    public function getDefinitions()
    {
        return $this->definitions;
    }

}
