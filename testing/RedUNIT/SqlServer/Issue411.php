<?php

namespace RedUNIT\Sqlserver;

use RedUNIT\Sqlserver as Sqlserver;
use RedBeanPHP\Facade as R;

/**
 * Issue 411
 *
 * @file    RedUNIT/Sqlserver/Issue411.php
 * @desc    Tests intermediate varchar 191 type for Sqlserver utf8mb4.
 * @author  Diego Vieira, Gabor de Mooij and the RedBeanPHP Community
 * @license New BSD/GPLv2
 *
 * (c) G.J.G.T. (Gabor) de Mooij and the RedBeanPHP Community.
 * This source file is subject to the New BSD/GPLv2 License that is bundled
 * with this source code in the file license.txt.
 */
class Issue411 extends Sqlserver
{

	/**
	 * Test varchar 191 condition.
	 *
	 * @return void
	 */
	public function testInnoDBIndexLimit()
	{
		R::nuke();
		$book = R::dispense( 'book' );
		$book->text = 'abcd';
		R::store( $book );
		$columns = R::inspect( 'book' );
		asrt( isset( $columns['text'] ), TRUE );
		asrt( $columns['text'], 'varchar(191)' );
		$book = $book->fresh();
		$book->text = str_repeat( 'x', 190 );
		R::store( $book );
		$columns = R::inspect( 'book' );
		asrt( isset( $columns['text'] ), TRUE );
		asrt( $columns['text'], 'varchar(191)' );
		$book = $book->fresh();
		$book->text = str_repeat( 'x', 191 );
		R::store( $book );
		$columns = R::inspect( 'book' );
		asrt( isset( $columns['text'] ), TRUE );
		asrt( $columns['text'], 'varchar(191)' );
		$book = $book->fresh();
		$book->text = str_repeat( 'x', 192 );
		R::store( $book );
		$columns = R::inspect( 'book' );
		asrt( isset( $columns['text'] ), TRUE );
		asrt( $columns['text'], 'varchar(255)' );
	}
}
