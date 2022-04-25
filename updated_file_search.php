<?php

/*
Program function:

    1. Iterate through all directories under the absolute path C:\MachinePrograms\SCADA\ProdLog

    2. For each file found within these directories
        a) get last modified unix timestamp from corresponding row in file table
        b) get last modified unix timestamp from CSV file
    
    3. Compare the timestamp from CSV file with timestamp from database
        if greater -> file has been modified
        if equal -> no update has taken place, therefore skip step 4
        if less -> this should NOT happen, stop program
        
    4. Add the filepath and modified timestamp from file to updated_files table in database, as well as updating the modified timestamp in file table for corresponding file

*/

include 'write.php';
include 'toolbox.php';

function checkFileUpdate($file_path, $conn) {
    // check if file has been updated, and if that's the case, update database

    // get file modified time of old file in database

    if (sqlsrv_begin_transaction($conn) === false) {
        // if transaction cannot begin, continue to next file

        print(PHP_EOL."Could not begin new transaction. Contine to next file.".PHP_EOL);
        print(print_r(sqlsrv_errors(), true));
        return;

    }

    $SQL_get_last_modified = <<<SQL

        IF NOT EXISTS (
            SELECT * 
            FROM prod_logging.dbo.[file] 
            WHERE path='{$file_path}'
            )

            BEGIN 
                INSERT INTO prod_logging.dbo.[file] VALUES ('{$file_path}', '', '')
            END;
            
        SELECT last_modified 
        FROM prod_logging.dbo.[file] 
        WHERE path='{$file_path}'

    SQL;

    if ($SQL_get_last_modified === false) {
        // the query was NOT successfully excecuted, therefore rollback transaction

        print_r(sqlsrv_errors(), true);
        print(PHP_EOL."Attempting to roll back transaction ...");
        
        rollbackOrDie($conn);
        
    } else {
        // the query was successfully excecuted, therefore attempt to commit transaction

       if (sqlsrv_commit($conn) === false) {
           // commit failed, therefore try to roll back transaction

           print(PHP_EOL."Commit failed. Try to roll back transaction.");
           rollbackOrDie($conn);

       } else {

        print(PHP_EOL."Transaction was commited");

       }

    }

    $params = array();
    $options =  array( "Scrollable" => SQLSRV_CURSOR_KEYSET );
    $get_last_modified = sqlsrv_query($conn, $SQL_get_last_modified);

    if ($get_last_modified === false)  {
        // query was NOT executed properly, therefore attempt to roll back transaction

        print("Could not get last modified from database");

    }

    // file modified time of both old and new file
    $unix_timestamp_db = sqlsrv_fetch_array($get_last_modified)[0];
    $unix_timestamp_file = filemtime($file_path);

    // get number of rows from CSV file
    $csv_file = file($file_path);
    $last_row = array_pop($csv_file);
    $data = str_getcsv($last_row);

    if ($unix_timestamp_file > $unix_timestamp_db) {
        // if file in folder has been updated, update last_modified column in file table and insert new row into updated_file table

        print(PHP_EOL.$unix_timestamp_file);
        print(PHP_EOL.$unix_timestamp_db);

        if (sqlsrv_begin_transaction($conn) === false) {
            // if transaction cannot begin, exit program

            die(print_r(sqlrv_errors(), true));

        }

        $SQL_update_modified_time = "UPDATE prod_logging.dbo.[file] SET last_modified=$unix_timestamp_file WHERE path='$file_path'";
        sqlsrv_query($conn, $SQL_update_modified_time) or ("Could not update rows");

        $SQL_insert_modified_time = <<<SQL
            INSERT INTO prod_logging.dbo.updated_file 
            VALUES (
                newid(), 
                $unix_timestamp_file, 
                (SELECT id FROM prod_logging.dbo.[file] WHERE path='{$file_path}')
                );
        SQL;
        $insert_modified_time = sqlsrv_query($conn, $SQL_insert_modified_time);

        if ($insert_modified_time === false) {
            // query failed, therefore attempt to roll back transaction

            rollbackOrDie($conn);

        } else {
            // query succeeded, therefore attempt to commit transaction

            if (sqlsrv_commit($conn) === false) {
                // commit failed, therefore attempt to roll back transaction

                rollbackOrDie($conn);

            }

        }

    } elseif ($unix_timestamp_file == $unix_timestamp_db) {

        print(PHP_EOL.'No updates at '.$file_path);
    
    } else {
        // modified time stamp in file is less than in database, which should NOT occour

        die(PHP_EOL."ERROR! Modified time in file $file_path is less than what is stored in database. This could mean that wrong file was read or that wrong modified time was stored in database.");

    }

}

$rootDirectory = '/../../../MachinePrograms/SCADA/ProdLog';
    
$categories = scandir($rootDirectory);

foreach ($categories as $category) {
    // for each category, get production lines

    if (is_dir($rootDirectory.'/'.$category) AND !($category == '.' OR $category == '..')) {

        $productionLines = scandir($rootDirectory.'/'.$category);

        foreach ($productionLines as $productionLine) {
            // for each production line, get csv files

            if (is_dir($rootDirectory.'/'.$category.'/'.$productionLine) AND !($productionLine == '.' OR $productionLine == '..')) {

                $files = scandir($rootDirectory.'/'.$category.'/'.$productionLine);
                
                foreach ($files as $file) {
                    // for each CSV file, check if file has been updated

                    $file_path = $rootDirectory.'/'.$category.'/'.$productionLine.'/'.$file;
                    
                    if (is_file($file_path) AND substr($file_path, -4) == ".csv") {
                        
                        checkFileUpdate($file_path, $conn);

                    }

                }
            
            }

        }

    }

}

sqlsrv_close($conn);
