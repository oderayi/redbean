<?php

namespace RedUNIT;

/**
 * Base
 *
 * This is the base class for all multi-driver Unit Tests.
 * By default base class derived tests will offer 'test rounds' for
 * all well known RedBeanPHP drivers: mysql (MySQL/MariaDB), pgsql (PostgreSQL),
 * sqlite (SQLite3) and CUBRID (CUBRID).
 *
 * @file    RedUNIT/Base.php
 * @desc    Base class for all drivers that support all database systems.
 * @author  Gabor de Mooij and the RedBeanPHP Community
 * @license New BSD/GPLv2
 *
 * (c) G.J.G.T. (Gabor) de Mooij and the RedBeanPHP Community.
 * This source file is subject to the New BSD/GPLv2 License that is bundled
 * with this source code in the file license.txt.
 */
class Base extends RedUNIT
{

	/**
	 * List of DB drivers
	 *
	 * @var array
	 */
	protected static $driverList = array( 'mysql', 'pgsql', 'sqlite', 'CUBRID' );

	/**
	 * Adds a driver to the list.
	 *
	 * @param string $driverID
	 */
	public static function addToDriverList( $driverID )
	{
		self::$driverList[] = $driverID;
	}

	/**
	 * What drivers should be loaded for this test pack?
	 *
	 * @return array
	 */
	public function getTargetDrivers()
	{
		return self::$driverList;
	}
}
