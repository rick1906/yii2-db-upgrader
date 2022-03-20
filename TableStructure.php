<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use Yii;
use yii\db\Connection;
use yii\db\SchemaBuilderTrait;
use yii\di\Instance;

/**
 *
 * @author Rick
 */
class TableStructure extends MigrationsContainer
{
    use SchemaBuilderTrait;

    /**
     * 
     * @var string|array|Connection
     */
    public $db = 'db';

    /**
     * 
     * @var string
     */
    public $table;

    /**
     * 
     * @var array
     */
    public $columns = array();

    /**
     * 
     * @var array
     */
    public $indexes = array();

    /**
     * 
     * @var array
     */
    public $options = array();

    /**
     * 
     * @var TableUpgrader
     */
    protected $tableUpgrader = null;

    public function __construct($table, $config = [])
    {
        if ($table && !is_scalar($table)) {
            $config = $table;
        } else {
            $config['table'] = $table;
        }
        parent::__construct($config);
    }

    protected function getDb()
    {
        return $this->db;
    }

    public function defineColumns($columns)
    {
        $this->columns = $columns;
        $this->tableUpgrader = null;
    }

    public function defineIndexes($indexes)
    {
        $this->indexes = $indexes;
        $this->tableUpgrader = null;
    }

    public function defineOptions($options)
    {
        $this->options = $options;
        $this->tableUpgrader = null;
    }

    public function in($version)
    {
        return new TableMigrationBuilder($version, $this->migrations, false, $this);
    }

    public function since($version, $featureId = null)
    {
        return new TableMigrationBuilder($version, $this->migrations, (string)$featureId !== '' ? $featureId : true, $this);
    }

    public function defer($version, $featureId = null)
    {
        return new TableMigrationBuilder($version, $this->deferredMigrations, (string)$featureId !== '' ? $featureId : true, $this);
    }

    public function dropIn($version)
    {
        $this->enabled = false;
        return $this->in($version)->addPredefinedMigration('dropTable', [$this->table], true);
    }

    /**
     * 
     * @return TableUpgrader
     */
    protected function createUpgrader()
    {
        $upgrader = $this->getDbUpgrader();
        if ($upgrader) {
            return $upgrader->createTableUpgrader($this->getDb(), $this->table, $this->columns, $this->indexes, $this->options);
        }
        return TableUpgrader::create($this->getDb(), $this->table, $this->columns, $this->indexes, $this->options);
    }

    /**
     * 
     * @return TableUpgrader
     */
    public function getUpgrader()
    {
        if ($this->tableUpgrader === null) {
            $this->tableUpgrader = $this->createUpgrader();
        }
        return $this->tableUpgrader;
    }

    /**
     * 
     * @return string
     */
    public function getTable()
    {
        return $this->table;
    }

}
