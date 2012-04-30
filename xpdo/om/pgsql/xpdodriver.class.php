<?php


/**
 * Include the parent {@link xPDODriver} class.
 */
require_once (dirname(dirname(__FILE__)) . '/xpdodriver.class.php');

/**
 * The PostgreSQL implementation of the xPDODriver class.
 *
 * @package xpdo
 * @subpackage om.pgsql
 */
class xPDODriver_pgsql extends xPDODriver {
    public $quoteChar = "'";
    public $escapeOpenChar = '"';
    public $escapeCloseChar = '"';
    public $_currentTimestamps= array (
        'CURRENT_TIMESTAMP',
        'NOW()',
        'LOCALTIMESTAMP',
        'STATEMENT_TIMESTAMP()',
        'TRANSACTION_TIMESTAMP()'
    );
    public $_currentDates= array (
        'CURRENT_DATE'
    );
    public $_currentTimes= array (
        'CURRENT_TIME',
        'LOCALTIME'
    );

    /**
     * Get a pgsql xPDODriver instance.
     *
     * @param xPDO &$xpdo A reference to a specific xPDO instance.
     */
    function __construct(xPDO &$xpdo) {
        parent :: __construct($xpdo);
        $this->dbtypes['integer']= array('/INT/i', '/SERIAL/i');
        $this->dbtypes['boolean']= array('/^BOOL/i');
        $this->dbtypes['float']= array('/^DEC/i', '/^NUMERIC/i', '/^FLOAT/i', '/^DOUBLE\s+PRECISION/i', '/^REAL/i', '/^MONEY$');
        $this->dbtypes['string']= array('/CHAR/i', '/^CHARACTER\s+VARYING/i', '/TEXT/i', '/^ENUM$/i', '/^SET$/i', '/^TIME$/i', '/^YEAR$/i', '/^CIDR$/i', '/^INET$/i', '/^MACADDR$/i', '/^UUID$/i', '/^XML$');
        $this->dbtypes['timestamp']= array('/^TIMESTAMP/i');
        $this->dbtypes['datetime']= array('/^TIMESTAMP/i');
        $this->dbtypes['date']= array('/^DATE$/i');
        $this->dbtypes['binary']= array('/BYTEA/i');
        $this->dbtypes['bit']= array('/^BIT/i');
    }
}
