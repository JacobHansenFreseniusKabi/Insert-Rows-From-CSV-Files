<?php
// THIS PHP FILE CONTAINS FUNCTIONS THAT ARE NOT SPECIFIC TO A SINGLE FILE



// SQL functions

function rollbackOrDie($conn) {

    if (sqlsrv_rollback($conn) === false) {

        print(PHP_EOL."Transaction failed to roll back, therefore exit program to prevent further failures".PHP_EOL);
        die(print_r(sqlsrv_errors(), true));

    }

    else {

        print(PHP_EOL."Transaction rolled back successfully");

    }

}


?>