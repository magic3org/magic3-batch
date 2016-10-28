package db

import (
//	"database/sql"
 //   "fmt"

    //_ "github.com/go-sql-driver/mysql"
)

//var con *sql.DB

/*
機能: 最も古いアクセスログを取得
*
* @param array  	$row		取得レコード
* @param bool					true=成功、false=失敗
*/
func getOldAccessLog() {
// 先頭のアクセスログを取得
/*var serial int64
query := "SELECT min(al_serial) FROM _access_log"
if err := db.QueryRow(query).Scan(&serial); err != nil {
    return err
}
fmt.Println(serial)*/
    return nil
/*		$serial = 0;
    $queryStr  = 'SELECT min(al_serial) as m FROM _access_log ';
    $ret = $this->selectRecord($queryStr, array(), $row);
    if ($ret) $serial = $row['m'];
    
    $queryStr  = 'SELECT * FROM _access_log ';
    $queryStr .=   'WHERE al_serial = ?';
    $ret = $this->selectRecord($queryStr, array($serial), $row);
    return $ret;*/
}