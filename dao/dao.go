package dao

import _ "github.com/go-sql-driver/mysql"

/*
機能: 最も古いアクセスログを取得
*
* @param array  	$row		取得レコード
* @param bool					true=成功、false=失敗
*/
func GetOldAccessLog() (row map[string]interface{}, err error) {
	// 先頭のアクセスログを取得
	var serial int64
	query := "SELECT min(al_serial) FROM _access_log"
	if err := db.QueryRow(query).Scan(&serial); err != nil { // レコードなしの場合は終了
		return nil, err
	}

	query = "SELECT * FROM _access_log "
	query += "WHERE al_serial = ?"
	row, err = selectRecord(query, serial)
	if err != nil {
		return nil, err
	}

	// 正常終了
	return row, nil
	/*	serial = -1
		query = "SELECT * FROM _access_log "
		query += "WHERE al_serial = ?"
		//row := db.QueryRow(query, serial)
		rows, err := db.Query(query, serial)
		for rows.Next() {
			results := make(map[string]interface{})
			err = rows.MapScan(results)
		}*/
	/*	var myMap = make(map[string]interface{})
		rows, err := db.Query("SELECT * FROM _access_log")
		defer rows.Close()
		if err != nil {
			log.Fatal(err)
		}
		colNames, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		cols := make([]interface{}, len(colNames))
		colPtrs := make([]interface{}, len(colNames))
		for i := 0; i < len(colNames); i++ {
			colPtrs[i] = &cols[i]
		}
		for rows.Next() {
			err = rows.Scan(colPtrs...)
			if err != nil {
				log.Fatal(err)
			}
			for i, col := range cols {
				myMap[colNames[i]] = col
			}
			// Do something with the map
			for key, val := range myMap {
				fmt.Println("Key:", key, "Value Type:", reflect.TypeOf(val))
			}
			break
		}
		fmt.Println(myMap["al_serial"])
		row := db.QueryRow(query, serial)
		return row, nil
	*/
	/*		$serial = 0;
	$queryStr  = 'SELECT min(al_serial) as m FROM _access_log ';
	$ret = $this->selectRecord($queryStr, array(), $row);
	if ($ret) $serial = $row['m'];

	$queryStr  = 'SELECT * FROM _access_log ';
	$queryStr .=   'WHERE al_serial = ?';
	$ret = $this->selectRecord($queryStr, array($serial), $row);
	return $ret;*/
}
