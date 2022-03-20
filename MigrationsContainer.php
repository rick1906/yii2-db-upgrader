<?php

/*
 * Copyright (c) 2021 Rick
 *
 * This file is a part of a project 'megabike'.
 * MIT licence.
 */

namespace megabike\yii2\migration;

use Yii;
use yii\base\Component;
use yii\db\Connection;
use yii\di\Instance;

/**
 *
 * @author Rick
 */
abstract class MigrationsContainer extends Component
{

    /**
     * 
     * @var string|array|Connection
     */
    public $db = 'db';

    /**
     * 
     * @var string|DatabaseUpgrader
     */
    public $dbUpgrader = 'dbUpgrader';

    /**
     * 
     * @var bool
     */
    public $enabled = true;

    /**
     * 
     * @var bool
     */
    public $newerFirst = true;

    /**
     * 
     * @var mixed
     */
    public $version = null;

    /**
     * 
     * @var array
     */
    protected $migrations = [];

    /**
     * 
     * @var array
     */
    protected $deferredMigrations = [];

    public function init()
    {
        parent::init();
        $this->db = Instance::ensure($this->db, Connection::className());
    }
    
    protected function getDbUpgrader()
    {
        if (!is_object($this->dbUpgrader)) {
            if (is_string($this->dbUpgrader) && Yii::$app) {
                $this->dbUpgrader = Yii::$app->get($this->dbUpgrader, false);
            }
            return null;
        }
        return $this->dbUpgrader;
    }

    public function defineVersion($version)
    {
        $this->version = $version;
    }

    public abstract function in($version);

    public abstract function since($version, $featureId = null);

    public abstract function defer($version, $featureId = null);

    public function addMigration($version, $up, $down = false, $params = [])
    {
        $this->migrations[] = MigrationBuilder::createMigrationConfig($version, $up, $down, $params);
    }

    /**
     * 
     * @return array
     */
    public function getMigrations()
    {
        return $this->migrations;
    }

    /**
     * 
     * @return array
     */
    public function getDeferredMigrations()
    {
        return $this->deferredMigrations;
    }

    /**
     * 
     * @return mixed
     */
    public function getVersion()
    {
        return $this->version;
    }

    /**
     * 
     * @return bool
     */
    public function isEnabled()
    {
        return $this->enabled;
    }

    /**
     * 
     * @return bool
     */
    public function isOrderNewerFirst()
    {
        return $this->newerFirst;
    }

}
