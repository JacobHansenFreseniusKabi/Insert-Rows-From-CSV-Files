<?php

/*
Program function:

    1. Select all file paths from table and add to local array

    2. For each file in array
        a) go to filepath
        b) get line name
        c) get number of rows within CSV file
        d) from database get number of rows of the same CSV file the last time it was imported

    3. Compare number of rows within CSV file with number stored in database
        if greater -> insert new rows into database
        if less or equal -> file name is the same, but the data within it is new, therefore import all data

    4. When array is iterated through, close connection to database and end program

Errors / tasks to complete:

    New transaction is not allowed because there are other threads running in the session 

*/

include 'write.php';
include 'toolbox.php';

function insertAllRows($file_path, $rows_in_csv_file, $line_name, $conn) {

    $last_row_csv_file = $rows_in_csv_file + 1;

    if (sqlsrv_begin_transaction($conn) === false) {
        // if transaction cannot begin, continue to next file

        print(PHP_EOL."Could not begin new transaction. Contine to next file.".PHP_EOL);
        print(print_r(sqlsrv_errors(), true));
        return;

    }

    $SQL_bulk_insert = <<<SQL

        USE prod_logging;

        IF EXISTS (
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME='temp_table'
            ) 
            BEGIN 
                DROP TABLE prod_logging.dbo.temp_table
            END;

        CREATE TABLE temp_table (
            var_name VARCHAR(50),
            time_string VARCHAR(50),
            var_value FLOAT,
            validity TINYINT,
            time_ms VARCHAR(20));


        BULK INSERT temp_table 
        FROM '{$file_path}' 
        WITH (
            FIRSTROW = 2, 
            LASTROW = {$last_row_csv_file},
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '0x0a');
                
                
        UPDATE temp_table
        SET time_string = SUBSTRING(time_string, 2, LEN(time_string))
        WHERE time_string LIKE '"%';


        UPDATE temp_table
        SET time_string = SUBSTRING(time_string, 1, LEN(time_string) - 1)
        WHERE time_string LIKE '%"';


        MERGE $line_name Target
        USING temp_table Source
        ON ((Target.var_name = Source.var_name) AND (Target.time_ms = Source.time_ms))

                    
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (var_name,time_string,var_value,validity,time_ms)
            VALUES (Source.var_name, Source.time_string, Source.var_value, Source.validity, Source.time_ms);


        DROP TABLE temp_table;


        DELETE FROM updated_file 
        WHERE file_id = (SELECT id FROM [file] WHERE path='{$file_path}');


        UPDATE [file]
        SET last_number_rows = {$rows_in_csv_file}
        WHERE path='{$file_path}';

    SQL;

    $bulk_insert = sqlsrv_query($conn, $SQL_bulk_insert);

    if ($bulk_insert === false) {
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

}

function insertNewRows($file_path, $rows_in_db, $rows_in_csv_file, $line_name, $conn) {

    $first_row_csv_file = $rows_in_db + 1;
    $last_row_csv_file = $rows_in_csv_file + 1;

    print(PHP_EOL.$first_row_csv_file." and ".$last_row_csv_file);

    if (sqlsrv_begin_transaction($conn) === false) {
        // if transaction cannot begin, continue to next file

        print(PHP_EOL."Could not begin new transaction. Contine to next file.".PHP_EOL);
        print(print_r(sqlsrv_errors(), true));
        return;

    }

    $SQL_bulk_insert = <<<SQL

        USE prod_logging;

        IF EXISTS (
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME='temp_table'
            ) 
            BEGIN 
                DROP TABLE prod_logging.dbo.temp_table
            END;

        CREATE TABLE temp_table (
            var_name VARCHAR(50),
            time_string VARCHAR(50),
            var_value FLOAT,
            validity TINYINT,
            time_ms VARCHAR(20));


        BULK INSERT temp_table 
        FROM '{$file_path}' 
        WITH (
            FIRSTROW = {$first_row_csv_file}, 
            LASTROW = {$last_row_csv_file},
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '0x0a');
        
        
        UPDATE temp_table
        SET time_string = SUBSTRING(time_string, 2, LEN(time_string))
        WHERE time_string LIKE '"%';


        UPDATE temp_table
        SET time_string = SUBSTRING(time_string, 1, LEN(time_string)-1)
        WHERE time_string LIKE '%"';


        MERGE $line_name Target
        USING temp_table Source
        ON ((Target.var_name = Source.var_name) AND (Target.time_ms = Source.time_ms))

            
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (var_name,time_string,var_value,validity,time_ms)
            VALUES (Source.var_name, Source.time_string, Source.var_value, Source.validity, Source.time_ms);


        DROP TABLE temp_table;


        DELETE FROM updated_file 
        WHERE file_id = (SELECT id FROM [file] WHERE path='{$file_path}');


        UPDATE [file]
        SET last_number_rows = {$rows_in_csv_file}
        WHERE path='{$file_path}';

    SQL;

    $bulk_insert = sqlsrv_query($conn, $SQL_bulk_insert);

    if ($bulk_insert === false) {
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

}

// set NOCOUNT on
$SQL_set_nocount_on = "SET NOCOUNT ON";
$set_nocount_on = sqlsrv_query($conn, $SQL_set_nocount_on);

if ($set_nocount_on === false) {
    print(PHP_EOL."NOOOO");
} else {
    print(PHP_EOL."YEAH");
}

// get GUID and file paths stored in database table
$SQL_get_file_paths = "SELECT guid, path, last_number_rows FROM updated_file INNER JOIN [file] ON file_id = [file].id ORDER BY modified DESC";
$params = array();
$options =  array( "Scrollable" => SQLSRV_CURSOR_KEYSET );
$get_updated_file_rows = sqlsrv_query($conn, $SQL_get_file_paths, $params, $options);

if ($get_file_paths === false) {

    print("Could not retrieve file paths");
    print_r(sqlsrv_errors(), true);

}

$file_path_array = [];
//$last_number_rows_array = [];

while ($updated_file_row = sqlsrv_fetch_array($get_updated_file_rows)) {
    // for each row in updated_file table, add file path to array and delete selected row from table

    if (!in_array($updated_file_row[1], $file_path_array)) {
        // only add file path and last number of rows to arrays if file path does not exists in array to begin with

        array_unshift($file_path_array, $updated_file_row[1]);
        //$file_path_array[] = $updated_file_row[1];
        //$last_number_rows_array[] = $updated_file_row[2];

    }

    /*
    $SQL_delete_file_path = "DELETE FROM updated_file WHERE guid='{$updated_file_row[0]}'";
    $delete_file_path = sqlsrv_query($conn, $SQL_delete_file_path);

    if ($delete_file_path === false) {

        print("Could not delete file path $updated_file_row[1] at $updated_file_row[0]");
    
    }
    */

}

print(PHP_EOL."Length of array = ".count($file_path_array)); // LENGTH OF ARRAY

//$row_index = 0;

for ($file_path_index = 0; $file_path_index < count($file_path_array); $file_path_index++) {

    $file_path = $file_path_array[$file_path_index];
    
    if (!file_exists($file_path)) {
        print("File does not exist");
        continue;
    }

    // number of rows the CSV file had at last import to database
    //$last_number_rows = $last_number_rows[$row_index]; 
    
    // read csv-file and get number of rows
    $csv_file = file($file_path);
    $last_row = array_pop($csv_file);
    $data = str_getcsv($last_row);
    
    if (is_numeric($data[1])) {
        // second index in array from last line in csv file contains the number of rows in file
        $rows_in_csv_file = $data[1] - 1;
    } else {
        // count number of rows in csv file the normal way
        $rows_in_csv_file = count($csv_file);
    }

    print(PHP_EOL."$rows_in_csv_file at $file_path");

    // find line name by manipulating file path string
    $end_position = strrpos($file_path, "/");
    $sub_str = substr($file_path, 0, $end_position);
    $start_position = strrpos($sub_str, "/");
    $line_name = strtolower(substr($sub_str, $start_position + 1));

    // get number of rows of the last time the file was modified from database
    $SQL_get_rows_in_db = "SELECT last_number_rows FROM [file] WHERE path='$file_path'";
    $get_rows_in_db = sqlsrv_query($conn, $SQL_get_rows_in_db, $params, $options);

    if ($rows_in_db_array === false) {
        print(PHP_EOL."Could not retrieve rows at $file_path");
        continue;
    }

    if ($rows_in_db_array = sqlsrv_fetch_array($get_rows_in_db)) {
        // fetch result from query as array
        $rows_in_db = $rows_in_db_array[0];
    }

    if ($rows_in_db == 0) {
        // the table is empty, therefore bulk insert from new CSV file

        insertAllRows($file_path, $rows_in_csv_file, $line_name, $conn);

    } elseif ($rows_in_csv_file > $rows_in_db) {
        // only insert new non-matching rows

        insertNewRows($file_path, $rows_in_db, $rows_in_csv_file, $line_name, $conn);

    } else {
        // all data within file is new, therefore bulk insert from CSV file

        insertAllRows($file_path, $rows_in_csv_file, $line_name, $conn);

    }

    //$row_index++;

}

sqlsrv_close($conn);

// USE WHEN PROGRAM WORKS

/*

    $SQL_bulk_insert = <<<SQL

        USE prod_logging;

        IF EXISTS (
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME='temp_table'
            ) 
            BEGIN 
                DROP TABLE prod_logging.dbo.temp_table
            END;

        CREATE TABLE temp_table (
            record_id INT,
            time_stamp_local VARCHAR(50),
            delta_to_utc VARCHAR(5),
            time_stamp_utc VARCHAR(50),
            user_id VARCHAR(50),
            object_id VARCHAR(50),
            description TEXT,
            comment TEXT,
            checksum VARCHAR(6)
        
        
        );


        BULK INSERT temp_table 
        FROM '{$file_path}' 
        WITH (
            FIRSTROW = 2, 
            LASTROW = {$last_row_csv_file},
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '0x0a');
                
                
        UPDATE temp_table
        SET time_stamp_local = SUBSTRING(time_stamp_local, 2, LEN(time_stamp_local)), 
        delta_to_utc = SUBSTRING(delta_to_utc, 2, LEN(delta_to_utc)),
        time_stamp_utc = SUBSTRING(time_stamp_utc, 2, LEN(time_stamp_utc))
        WHERE time_stamp_local LIKE '"%';


        UPDATE temp_table
        SET time_stamp_local = SUBSTRING(time_stamp_local, 1, LEN(time_stamp_local) - 1),
        delta_to_utc = SUBSTRING(delta_to_utc, 1, LEN(delta_to_utc) - 1),
        time_stamp_utc = SUBSTRING(delta_to_utc, 1, LEN(delta_to_utc) - 1)
        WHERE time_stamp_local LIKE '%"';


        MERGE $line_name Target
        USING temp_table Source
        ON ((Target.record_id = Source.record_id) AND (Target.checksum = Source.checksum))

                    
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (record_id, time_stamp_local, delta_to_utc, time_stamp_utc, user_id, object_id, description, comment, checksum)
            VALUES (Source.record_id, Source.time_stamp_local, Source.delta_to_utc, Source.time_stamp_utc, Source.user_id, Source.object_id, Source.description, Source.comment, Source.checksum);


        DROP TABLE temp_table;


        UPDATE [file]
        SET last_number_rows = {$rows_in_csv_file}
        WHERE path='{$file_path}';

    SQL;

*/


?>
