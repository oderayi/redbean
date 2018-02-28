<?php

namespace RedBeanPHP\QueryWriter;

use RedBeanPHP\Adapter as Adapter;
use RedBeanPHP\Adapter\DBAdapter as DBAdapter;
use RedBeanPHP\QueryWriter as QueryWriter;
use RedBeanPHP\QueryWriter\AQueryWriter as AQueryWriter;
use RedBeanPHP\RedException\SQL as SQLException;

/**
 * RedBeanPHP SqlServerWriter.
 * This is a QueryWriter class for RedBeanPHP.
 * This QueryWriter provides support for the SqlServer database platform.
 *
 * @file    RedBeanPHP/QueryWriter/SqlServer.php
 * @author  Steven Oderayi, Diego Vieira, Gabor de Mooij and the RedBeanPHP Community
 * @license BSD/GPLv2
 *
 * @copyright
 * (c) G.J.G.T. (Gabor) de Mooij and the RedBeanPHP Community.
 * This source file is subject to the BSD/GPLv2 License that is bundled
 * with this source code in the file license.txt.
 */
class SqlServer extends AQueryWriter implements QueryWriter
{
    /**
     * Data types
     */
    const C_DATATYPE_BOOL = 0;
    const C_DATATYPE_INT32 = 2;
    const C_DATATYPE_INT64 = 3;
    const C_DATATYPE_DOUBLE = 4;
    const C_DATATYPE_TEXT7 = 5;
    const C_DATATYPE_TEXT8 = 6;
    const C_DATATYPE_TEXT32 = 7;
    const C_DATATYPE_SPECIAL_DATE = 80;
    const C_DATATYPE_SPECIAL_DATETIME = 81;
    const C_DATATYPE_SPECIAL_POINT = 90;
    const C_DATATYPE_SPECIAL_LINESTRING = 91;
    const C_DATATYPE_SPECIAL_POLYGON = 92;
    const C_DATATYPE_SPECIAL_MONEY = 93;

    const C_DATATYPE_SPECIFIED = 99;

    /**
     * @var DBAdapter
     */
    protected $adapter;

    /**
     * @var string
     */
    protected $quoteCharacter = '"';

    /**
     * @see AQueryWriter::getKeyMapForType
     */
    protected function getKeyMapForType($type)
    {
        $table = $this->esc($type, true);
        $keys = $this->adapter->get("
		SELECT RC.CONSTRAINT_NAME 'name',
            KF.TABLE_NAME 'table',
            KF.COLUMN_NAME 'from',
            KP.COLUMN_NAME 'to',
            RC.UPDATE_RULE on_update,
            RC.DELETE_RULE on_delete
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KF ON RC.CONSTRAINT_NAME = KF.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KP ON RC.UNIQUE_CONSTRAINT_NAME = KP.CONSTRAINT_NAME
            WHERE KF.TABLE_NAME = :table", array(':table' => $table));
        $keyInfoList = array();
        foreach ($keys as $k) {
            $label = $this->makeFKLabel($k['from'], $k['table'], $k['to']);
            $keyInfoList[$label] = array(
                'name' => $k['name'],
                'from' => $k['from'],
                'table' => $k['table'],
                'to' => $k['to'],
                'on_update' => $k['on_update'],
                'on_delete' => $k['on_delete'],
            );
        }
        return $keyInfoList;
    }

    /**
     * Inserts a record into the database using a series of insert columns
     * and corresponding insertvalues. Returns the insert id.
     *
     * @param string $table         table to perform query on
     * @param array  $insertcolumns columns to be inserted
     * @param array  $insertvalues  values to be inserted
     *
     * @return integer
     */
    public function insertRecord($type, $insertcolumns, $insertvalues)
    {
        $default = $this->defaultValue;
        $table = $this->esc($type);

        if (count($insertvalues) > 0 && is_array($insertvalues[0]) && count($insertvalues[0]) > 0) {

            $insertSlots = array();
            foreach ($insertcolumns as $k => $v) {
                $insertcolumns[$k] = $this->esc($v);

                if (isset(self::$sqlFilters['w'][$type][$v])) {
                    $insertSlots[] = self::$sqlFilters['w'][$type][$v];
                } else {
                    $insertSlots[] = '?';
                }
            }

            $insertSQL = "INSERT INTO $table ( " . implode(',', $insertcolumns) . " ) VALUES
			( " . implode(',', $insertSlots) . " ) ";

            $ids = array();
            foreach ($insertvalues as $i => $insertvalue) {
                $ids[] = $this->adapter->getCell($insertSQL, $insertvalue, $i);
            }

            $result = count($ids) === 1 ? array_pop($ids) : $ids;
        } else {
            $result = $this->adapter->getCell("INSERT INTO $table DEFAULT VALUES");
        }

        $last_id = $this->adapter->getInsertID();

        return $last_id;
    }

    /**
     * Constructor
     *
     * @param Adapter $adapter Database Adapter
     */
    public function __construct(Adapter $adapter)
    {
        $this->typeno_sqltype = array(
            SqlServer::C_DATATYPE_BOOL => ' BIT ',
            SqlServer::C_DATATYPE_INT32 => ' INT ',
            SqlServer::C_DATATYPE_INT64 => ' BIGINT ',
            SqlServer::C_DATATYPE_DOUBLE => ' FLOAT ',
            SqlServer::C_DATATYPE_TEXT7 => ' NVARCHAR(191) ',
            SqlServer::C_DATATYPE_TEXT8 => ' NVARCHAR(255) ',
            SqlServer::C_DATATYPE_TEXT32 => ' NTEXT ',
            SqlServer::C_DATATYPE_SPECIAL_DATE => ' DATE ',
            SqlServer::C_DATATYPE_SPECIAL_DATETIME => ' DATETIME ',
            SqlServer::C_DATATYPE_SPECIAL_POINT => ' POINT ',
            SqlServer::C_DATATYPE_SPECIAL_LINESTRING => ' LINESTRING ',
            SqlServer::C_DATATYPE_SPECIAL_POLYGON => ' POLYGON ',
            SqlServer::C_DATATYPE_SPECIAL_MONEY => ' MONEY ',
        );

        $this->sqltype_typeno = array();

        foreach ($this->typeno_sqltype as $k => $v) {
            $this->sqltype_typeno[trim(strtolower($v))] = $k;
        }

        $this->adapter = $adapter;
    }

    /**
     * This method returns the datatype to be used for primary key IDS and
     * foreign keys. Returns one if the data type constants.
     *
     * @return integer
     */
    public function getTypeForID()
    {
        return self::C_DATATYPE_INT32;
    }

    /**
     * @see QueryWriter::getTables
     */
    public function getTables()
    {
        return $this->adapter->getCol('SELECT * FROM sys.Tables');
    }

    /**
     * @see QueryWriter::createTable
     */
    public function createTable($table)
    {
        $sql = "CREATE TABLE [$table] ([id] [int] IDENTITY(1,1) NOT NULL, CONSTRAINT [PK_$table] PRIMARY KEY ( [id] ))";

        $this->adapter->exec($sql);
    }

    /**
     * @see QueryWriter::glueLimitOne
     */
    public function glueLimitOne($sql = '')
    {
        return $sql;
    }

    /**
     * @see QueryWriter::getColumns
     */
    public function getColumns($table)
    {
        $q = "SELECT c.name, c.max_length, y.name as data_type
				FROM   sys.columns c
				INNER JOIN sys.tables t ON c.object_id = t.object_id
				INNER JOIN sys.types y ON y.system_type_id = c.system_type_id
				WHERE  t.name = '$table'";

        $columnsRaw = $this->adapter->get($q);
        $columns = array();

        foreach ($columnsRaw as $r) {
            $columns[$r['name']] = $r['data_type'] . (is_numeric($r['max_length']) ? '(' . $r['max_length'] . ')' : '');
        }
        return $columns;
    }

    /**
     * @see QueryWriter::scanType
     */
    public function scanType($value, $flagSpecial = false)
    {
        $this->svalue = $value;
        if (is_null($value)) {
            return SqlServer::C_DATATYPE_BOOL;
        }

        if ($value === INF) {
            return SqlServer::C_DATATYPE_TEXT7;
        }

        if ($flagSpecial) {
            if (preg_match('/^-?\d+\.\d{2}$/', $value)) {
                return SqlServer::C_DATATYPE_SPECIAL_MONEY;
            }
            if (preg_match('/^POINT\(/', $value)) {
                return SqlServer::C_DATATYPE_SPECIAL_POINT;
            }
            if (preg_match('/^LINESTRING\(/', $value)) {
                return SqlServer::C_DATATYPE_SPECIAL_LINESTRING;
            }
            if (preg_match('/^POLYGON\(/', $value)) {
                return SqlServer::C_DATATYPE_SPECIAL_POLYGON;
            }
        }

        if (preg_match('/^\d{4}\-\d\d-\d\d$/', $value)) {
            return SqlServer::C_DATATYPE_SPECIAL_DATE;
        }
        if (preg_match('/^\d{4}\-\d\d-\d\d\s\d\d:\d\d:\d\d$/', $value)) {
            return SqlServer::C_DATATYPE_SPECIAL_DATETIME;
        }

        //setter turns TRUE FALSE into 0 and 1 because database has no real bools (TRUE and FALSE only for test?).
        if ($value === false || $value === true || $value === '0' || $value === '1') {
            return SqlServer::C_DATATYPE_BOOL;
        }

        if (is_float($value)) {
            return self::C_DATATYPE_DOUBLE;
        }

        if (!$this->startsWithZeros($value)) {

            if (is_numeric($value) && (floor($value) == $value) && $value >= 0 && $value <= 2147483647) {
                return SqlServer::C_DATATYPE_INT32;
            }

            if (is_numeric($value) && (floor($value) == $value) && $value >= 0 && $value <= 9223372036854775807) {
                return SqlServer::C_DATATYPE_INT64;
            }

            if (is_numeric($value)) {
                return SqlServer::C_DATATYPE_DOUBLE;
            }
        }

        if (mb_strlen($value, 'UTF-8') <= 191) {
            return SqlServer::C_DATATYPE_TEXT7;
        }

        if (mb_strlen($value, 'UTF-8') <= 255) {
            return SqlServer::C_DATATYPE_TEXT8;
        }

        return SqlServer::C_DATATYPE_TEXT32;
    }

    /**
     * @see QueryWriter::code
     */
    public function code($typedescription, $includeSpecials = true)
    {
        if (isset($this->sqltype_typeno[$typedescription])) {
            $r = $this->sqltype_typeno[$typedescription];
        } else {
            $r = self::C_DATATYPE_SPECIFIED;
        }

        if ($includeSpecials) {
            return $r;
        }

        if ($r >= QueryWriter::C_DATATYPE_RANGE_SPECIAL) {
            return self::C_DATATYPE_SPECIFIED;
        }

        return $r;
    }

    /**
     * @see QueryWriter::addUniqueIndex
     */
    public function addUniqueConstraint($type, $properties)
    {
        $tableNoQ = $this->esc($type, true);
        $columns = array();
        foreach ($properties as $key => $column) {
            $columns[$key] = $this->esc($column);
        }

        $table = $this->esc($type);
        sort($columns); // Else we get multiple indexes due to order-effects
        $name = 'UQ_' . sha1(implode(',', $columns));
        try {
            $sql = "CREATE UNIQUE INDEX $name
                    ON $table (" . implode(',', $columns) . ")";
            $this->adapter->exec($sql);
        } catch (SQLException $e) {
            //do nothing, dont use alter table ignore, this will delete duplicate records in 3-ways!
            return false;
        }
        return true;
    }

    /**
     * @see QueryWriter::addIndex
     */
    public function addIndex($type, $name, $property)
    {
        try {
            $table = $this->esc($type);
            $name = preg_replace('/\W/', '', $name);
            $column = $this->esc($property);
            $this->adapter->exec("CREATE INDEX $name ON $table ($column) ");
            return true;
        } catch (SQLException $e) {
            return false;
        }
    }

    /**
     * @see QueryWriter::addFK
     * @return bool
     */
    public function addFK($type, $targetType, $property, $targetProperty, $isDependent = false)
    {
        $table = $this->esc($type);
        $targetTable = $this->esc($targetType);
        $targetTableNoQ = $this->esc($targetType, true);
        $field = $this->esc($property);
        $fieldNoQ = $this->esc($property, true);
        $targetField = $this->esc($targetProperty);
        $targetFieldNoQ = $this->esc($targetProperty, true);
        $tableNoQ = $this->esc($type, true);
        $fieldNoQ = $this->esc($property, true);
        if (!is_null($this->getForeignKeyForTypeProperty($tableNoQ, $fieldNoQ))) {
            return false;
        }

        //Widen the column if it's incapable of representing a foreign key (at least INT).
        $columns = $this->getColumns($tableNoQ);
        $idType = $this->getTypeForID();
        if ($this->code($columns[$fieldNoQ]) !== $idType) {
            $this->widenColumn($type, $property, $idType);
        }

        $fkName = 'fk_' . ($tableNoQ . '_' . $fieldNoQ);
        $cName = 'c_' . $fkName;
        try {
            $this->adapter->exec("ALTER TABLE [{$table}]
				ADD CONSTRAINT [$fkName] FOREIGN KEY ( [$fieldNoQ] ) REFERENCES [{$targetTableNoQ}] (
				[{$targetFieldNoQ}]) ON DELETE " . ($isDependent ? 'CASCADE' : 'NO ACTION') . ' ON UPDATE NO ACTION ;');
        } catch (SQLException $e) {
            // Failure of fk-constraints is not a problem
        }
        return true;
    }

    /**
     * @see QueryWriter::sqlStateIn
     */
    public function sqlStateIn($state, $list)
    {
        $stateMap = array(
            '42S02' => QueryWriter::C_SQLSTATE_NO_SUCH_TABLE,
            '42S22' => QueryWriter::C_SQLSTATE_NO_SUCH_COLUMN,
            '23000' => QueryWriter::C_SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION,
        );

        return in_array((isset($stateMap[$state]) ? $stateMap[$state] : '0'), $list);
    }

    /**
     * @see QueryWriter::widenColumn
     */
    public function widenColumn($type, $property, $dataType)
    {
        if (!isset($this->typeno_sqltype[$dataType])) {
            return false;
        }

        $table = $this->esc($type);
        $column = $this->esc($property);

        $newType = $this->typeno_sqltype[$dataType];

        $this->adapter->exec("ALTER TABLE [$table] ALTER COLUMN $column $newType ");

        return true;
    }

    /**
     * @see QueryWriter::wipe
     */
    public function wipe($type)
    {
        $table = $this->esc($type);

        $this->adapter->exec("TRUNCATE TABLE $table ");
    }

    /**
     * @see QueryWriter::wipeAll
     */
    public function wipeAll()
    {
        foreach ($this->getTables() as $t) {
            try {

                $foreignKeys = $this->adapter->getAssoc("SELECT
				    'ALTER TABLE ' +  OBJECT_SCHEMA_NAME(parent_object_id) +
				    '.[' + OBJECT_NAME(parent_object_id) +
				    '] DROP CONSTRAINT ' + name
				FROM sys.foreign_keys
				WHERE referenced_object_id = object_id('$t')");
                if (count($foreignKeys)) {
                    foreach ($foreignKeys as $sql) {
                        $this->adapter->exec($sql);
                    }
                }

                $this->adapter->exec("IF OBJECT_ID('[$t]', 'U') IS NOT NULL DROP TABLE [$t];");
            } catch (SQLException $e) {
            }
        }
    }
}
