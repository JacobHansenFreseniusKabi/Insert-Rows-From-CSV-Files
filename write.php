<?php
// please fill in correct details in inline comments

// connect to server and database
$instance = "/*computer name*/\SQLEXPRESS";
$connectionInfo = array("Database"=>"/*database name*//", "UID"=>"/*username*//", "PWD"=>"/*password*//");

$conn = sqlsrv_connect($instance, $connectionInfo);
if ($conn) {
    echo "<script>alert('Connection established to $instance.')</script>";
} else {
    echo "<script>alert('Connection failed')</script>";
    print_r($connectionInfo);
    die(print_r(sqlsrv_errors(), true));
}

?>