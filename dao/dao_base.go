package dao

import (
	"fmt"
	"log"
	"reflect"

	_ "github.com/go-sql-driver/mysql"
)

/*
機能: クエリーを実行し１行取得
	array: 実行クエリー
	params: クエリー埋め込み用パラメータ
	row: Map化したレコード
	err: 終了ステータス
*/
func selectRecord(query string, params ...interface{}) (row map[string]interface{}, err error) {

	var mapRow = make(map[string]interface{})
	rows, err := db.Query(query, params)
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
			mapRow[colNames[i]] = col
		}
		// Do something with the map
		for key, val := range mapRow {
			fmt.Println("Key:", key, "Value Type:", reflect.TypeOf(val))
		}
		break
	}
	fmt.Println(mapRow["al_serial"])
	return mapRow, nil
}
