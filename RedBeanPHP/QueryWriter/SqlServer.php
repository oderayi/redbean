<?php

namespace RedBeanPHP\QueryWriter;

use RedBeanPHP\QueryWriter\AQueryWriter as AQueryWriter;
use RedBeanPHP\QueryWriter as QueryWriter;
use RedBeanPHP\Adapter\DBAdapter as DBAdapter;
use RedBeanPHP\Adapter as Adapter;
use RedBeanPHP\RedException\SQL as SQLException;

/**
 * RedBeanPHP SqlServerWriter.
 * This is a QueryWriter class for RedBeanPHP.
 * This QueryWriter provides support for the SqlServer/MariaDB database platform.
 *
 * @file    RedBeanPHP/QueryWriter/SqlServer.php
 * @author  Gabor de Mooij and the RedBeanPHP Community
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
	const C_DATATYPE_BOOL             = 0;
	const C_DATATYPE_UINT32           = 2;
	const C_DATATYPE_DOUBLE           = 3;
	const C_DATATYPE_TEXT7            = 4; //InnoDB cant index varchar(255) utf8mb4 - so keep 191 as long as possible
	const C_DATATYPE_TEXT8            = 5;
	const C_DATATYPE_TEXT16           = 6;
	const C_DATATYPE_TEXT32           = 7;
	const C_DATATYPE_SPECIAL_DATE     = 80;
	const C_DATATYPE_SPECIAL_DATETIME = 81;
	const C_DATATYPE_SPECIAL_POINT    = 90;
	const C_DATATYPE_SPECIAL_LINESTRING = 91;
	const C_DATATYPE_SPECIAL_POLYGON    = 92;
	const C_DATATYPE_SPECIAL_MONEY    = 93;

	const C_DATATYPE_SPECIFIED        = 99;

	/**
	 * @var DBAdapter
	 */
	protected $adapter;

	/**
	 * @var string
	 */
	protected $quoteCharacter = '`';

	/**
	 * @see AQueryWriter::getKeyMapForType
	 */
	protected function getKeyMapForType( $type )
	{
		$databaseName = $this->adapter->getCell('SELECT DATABASE()');
		$table = $this->esc( $type, TRUE );
		$keys = $this->adapter->get('
			SELECT
				information_schema.key_column_usage.constraint_name AS `name`,
				information_schema.key_column_usage.referenced_table_name AS `table`,
				information_schema.key_column_usage.column_name AS `from`,
				information_schema.key_column_usage.referenced_column_name AS `to`,
				information_schema.referential_constraints.update_rule AS `on_update`,
				information_schema.referential_constraints.delete_rule AS `on_delete`
				FROM information_schema.key_column_usage
				INNER JOIN information_schema.referential_constraints
				ON information_schema.referential_constraints.constraint_name = information_schema.key_column_usage.constraint_name
			WHERE
				information_schema.key_column_usage.table_schema = :database
				AND information_schema.referential_constraints.constraint_schema  = :database
				AND information_schema.key_column_usage.constraint_schema  = :database
				AND information_schema.key_column_usage.table_name = :table
				AND information_schema.key_column_usage.constraint_name != \'PRIMARY\'
				AND information_schema.key_column_usage.referenced_table_name IS NOT NULL
		', array( ':database' => $databaseName, ':table' => $table ) );
		$keyInfoList = array();
		foreach ( $keys as $k ) {
			$label = $this->makeFKLabel( $k['from'], $k['table'], $k['to'] );
			$keyInfoList[$label] = array(
				'name'          => $k['name'],
				'from'          => $k['from'],
				'table'         => $k['table'],
				'to'            => $k['to'],
				'on_update'     => $k['on_update'],
				'on_delete'     => $k['on_delete']
			);
		}
		return $keyInfoList;
	}

	/**
	 * Constructor
	 *
	 * @param Adapter $adapter Database Adapter
	 */
	public function __construct( Adapter $adapter )
	{
		$this->typeno_sqltype = array(
			SqlServer::C_DATATYPE_BOOL             => ' TINYINT(1) UNSIGNED ',
			SqlServer::C_DATATYPE_UINT32           => ' INT(11) UNSIGNED ',
			SqlServer::C_DATATYPE_DOUBLE           => ' DOUBLE ',
			SqlServer::C_DATATYPE_TEXT7            => ' VARCHAR(191) ',
			SqlServer::C_DATATYPE_TEXT8	           => ' VARCHAR(255) ',
			SqlServer::C_DATATYPE_TEXT16           => ' TEXT ',
			SqlServer::C_DATATYPE_TEXT32           => ' LONGTEXT ',
			SqlServer::C_DATATYPE_SPECIAL_DATE     => ' DATE ',
			SqlServer::C_DATATYPE_SPECIAL_DATETIME => ' DATETIME ',
			SqlServer::C_DATATYPE_SPECIAL_POINT    => ' POINT ',
			SqlServer::C_DATATYPE_SPECIAL_LINESTRING => ' LINESTRING ',
			SqlServer::C_DATATYPE_SPECIAL_POLYGON => ' POLYGON ',
			SqlServer::C_DATATYPE_SPECIAL_MONEY    => ' DECIMAL(10,2) '
		);

		$this->sqltype_typeno = array();

		foreach ( $this->typeno_sqltype as $k => $v ) {
			$this->sqltype_typeno[trim( strtolower( $v ) )] = $k;
		}

		$this->adapter = $adapter;

		$this->encoding = $this->adapter->getDatabase()->getSqlServerEncoding();
	}

	/**
	 * This method returns the datatype to be used for primary key IDS and
	 * foreign keys. Returns one if the data type constants.
	 *
	 * @return integer
	 */
	public function getTypeForID()
	{
		return self::C_DATATYPE_UINT32;
	}

	/**
	 * @see QueryWriter::getTables
	 */
	public function getTables()
	{
		return $this->adapter->getCol( 'SELECT * FROM sys.Tables' );
	}

	/**
	 * @see QueryWriter::createTable
	 */
	public function createTable( $table )
	{
		$table = $this->esc( $table );

		//$encoding = $this->adapter->getDatabase()->getSqlserverEncoding();
		$sql   = "CREATE TABLE [$table] ([id] [int] IDENTITY(1,1) NOT NULL, CONSTRAINT [PK_$table] PRIMARY KEY ( [id] ))";

		$this->adapter->exec( $sql );
	}

	/**
	 * @see QueryWriter::getColumns
	 */
	public function getColumns( $table )
	{
		$columnsRaw = $this->adapter->get( "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" . $this->esc( $table ) . "'" );

		$columns = array();
		foreach ( $columnsRaw as $r ) {
			$columns[$r['COLUMN_NAME']] = array(
					'TYPE' => $r['DATA_TYPE'],
					'LENGTH' => $r['CHARACTER_MAXIMUM_LENGTH'],
			);
		}

		return $columns;
	}

	/**
	 * @see QueryWriter::scanType
	 */
	public function scanType( $value, $flagSpecial = FALSE )
	{
		$this->svalue = $value;

		if ( is_null( $value ) ) return SqlServer::C_DATATYPE_BOOL;
		if ( $value === INF ) return SqlServer::C_DATATYPE_TEXT7;

		if ( $flagSpecial ) {
			if ( preg_match( '/^-?\d+\.\d{2}$/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_MONEY;
			}
			if ( preg_match( '/^\d{4}\-\d\d-\d\d$/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_DATE;
			}
			if ( preg_match( '/^\d{4}\-\d\d-\d\d\s\d\d:\d\d:\d\d$/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_DATETIME;
			}
			if ( preg_match( '/^POINT\(/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_POINT;
			}
			if ( preg_match( '/^LINESTRING\(/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_LINESTRING;
			}
			if ( preg_match( '/^POLYGON\(/', $value ) ) {
				return SqlServer::C_DATATYPE_SPECIAL_POLYGON;
			}
		}

		//setter turns TRUE FALSE into 0 and 1 because database has no real bools (TRUE and FALSE only for test?).
		if ( $value === FALSE || $value === TRUE || $value === '0' || $value === '1' ) {
			return SqlServer::C_DATATYPE_BOOL;
		}

		if ( is_float( $value ) ) return self::C_DATATYPE_DOUBLE;

		if ( !$this->startsWithZeros( $value ) ) {

			if ( is_numeric( $value ) && ( floor( $value ) == $value ) && $value >= 0 && $value <= 4294967295 ) {
				return SqlServer::C_DATATYPE_UINT32;
			}

			if ( is_numeric( $value ) ) {
				return SqlServer::C_DATATYPE_DOUBLE;
			}
		}

		if ( mb_strlen( $value, 'UTF-8' ) <= 191 ) {
			return SqlServer::C_DATATYPE_TEXT7;
		}

		if ( mb_strlen( $value, 'UTF-8' ) <= 255 ) {
			return SqlServer::C_DATATYPE_TEXT8;
		}

		if ( mb_strlen( $value, 'UTF-8' ) <= 65535 ) {
			return SqlServer::C_DATATYPE_TEXT16;
		}

		return SqlServer::C_DATATYPE_TEXT32;
	}

	/**
	 * @see QueryWriter::code
	 */
	public function code( $typedescription, $includeSpecials = FALSE )
	{
		if ( isset( $this->sqltype_typeno[$typedescription] ) ) {
			$r = $this->sqltype_typeno[$typedescription];
		} else {
			$r = self::C_DATATYPE_SPECIFIED;
		}

		if ( $includeSpecials ) {
			return $r;
		}

		if ( $r >= QueryWriter::C_DATATYPE_RANGE_SPECIAL ) {
			return self::C_DATATYPE_SPECIFIED;
		}

		return $r;
	}

	/**
	 * @see QueryWriter::addUniqueIndex
	 */
	public function addUniqueConstraint( $type, $properties )
	{
		$tableNoQ = $this->esc( $type, TRUE );
		$columns = array();
		foreach( $properties as $key => $column ) $columns[$key] = $this->esc( $column );
		$table = $this->esc( $type );
		sort( $columns ); // Else we get multiple indexes due to order-effects
		$name = 'UQ_' . sha1( implode( ',', $columns ) );
		try {
			$sql = "CREATE UNIQUE INDEX $name
                    ON $table (" . implode( ',', $columns ) . ")";
			$this->adapter->exec( $sql );
		} catch ( SQLException $e ) {
			//do nothing, dont use alter table ignore, this will delete duplicate records in 3-ways!
			return FALSE;
		}
		return TRUE;
	}

	/**
	 * @see QueryWriter::addIndex
	 */
	public function addIndex( $type, $name, $property )
	{
		try {
			$table  = $this->esc( $type );
			$name   = preg_replace( '/\W/', '', $name );
			$column = $this->esc( $property );
			$this->adapter->exec( "CREATE INDEX $name ON $table ($column) " );
			return TRUE;
		} catch ( SQLException $e ) {
			return FALSE;
		}
	}

	/**
	 * @see QueryWriter::addFK
	 * @return bool
	 */
	public function addFK( $type, $targetType, $property, $targetProperty, $isDependent = FALSE )
	{
		$table = $this->esc( $type );
		$targetTable = $this->esc( $targetType );
		$targetTableNoQ = $this->esc( $targetType, TRUE );
		$field = $this->esc( $property );
		$fieldNoQ = $this->esc( $property, TRUE );
		$targetField = $this->esc( $targetProperty );
		$targetFieldNoQ = $this->esc( $targetProperty, TRUE );
		$tableNoQ = $this->esc( $type, TRUE );
		$fieldNoQ = $this->esc( $property, TRUE );
		if ( !is_null( $this->getForeignKeyForTypeProperty( $tableNoQ, $fieldNoQ ) ) ) return FALSE;

		//Widen the column if it's incapable of representing a foreign key (at least INT).
		$columns = $this->getColumns( $tableNoQ );
		$idType = $this->getTypeForID();
		if ( $this->code( $columns[$fieldNoQ] ) !==  $idType ) {
			$this->widenColumn( $type, $property, $idType );
		}

		$fkName = 'fk_'.($tableNoQ.'_'.$fieldNoQ);
		$cName = 'c_'.$fkName;
		try {
			$this->adapter->exec( "ALTER TABLE [{$table}]
				ADD CONSTRAINT [$cName] FOREIGN KEY ( [$fkName] ) REFERENCES [{$targetTableNoQ}] (
				[{$targetFieldNoQ}]) ON DELETE " . ( $isDependent ? 'CASCADE' : 'SET NULL' ) . ' ON UPDATE SET NULL ;' );
		} catch ( SQLException $e ) {
			// Failure of fk-constraints is not a problem
		}
		return true;
	}

	/**
	 * @see QueryWriter::sqlStateIn
	 */
	public function sqlStateIn( $state, $list )
	{
		$stateMap = array(
			'42S02' => QueryWriter::C_SQLSTATE_NO_SUCH_TABLE,
			'42S22' => QueryWriter::C_SQLSTATE_NO_SUCH_COLUMN,
			'23000' => QueryWriter::C_SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION
		);

		return in_array( ( isset( $stateMap[$state] ) ? $stateMap[$state] : '0' ), $list );
	}

	/**
	 * @see QueryWriter::wipeAll
	 */
	public function wipeAll()
	{
		foreach ( $this->getTables() as $t ) {
			try {
				$this->adapter->exec( "EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'; IF OBJECT_ID('[$t]', 'U') IS NOT NULL DROP TABLE [$t]; EXEC sp_msforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all';" );
			} catch ( SQLException $e ) {
			}
		}
	}
}
