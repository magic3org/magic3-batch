package dao

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	_ "github.com/go-sql-driver/mysql"
)

// トランザクション状態
const (
	DB_NO_ERROR = 0 // エラーなし
	DB_ERROR    = 1 // エラーあり
)

/*
const DATE_FORMAT = "2006-01-02"                   // 日付フォーマット
const TIMESTAMP_FORMAT = "2006-01-02 15:04:05.999" // 日付時間フォーマット
*/
var _db *sql.DB
var _tx *sql.Tx
var _tranStatus int // トランザクション状態

/*
機能: DBコネクション作成
*/
func Init(host string, dbname string, dbuser string, dbpwd string) error {
	// タイムゾーンを指定してDBに接続
	var err error

	_db, err = sql.Open("mysql", dbuser+":"+dbpwd+"@tcp("+host+")/"+dbname+"?parseTime=true&loc=Asia%2FTokyo")
	//_db, err = sql.Open("mysql", dbuser+":"+dbpwd+"@tcp("+host+")/"+dbname)
	if err != nil {
		return err
	}
	return nil
}

/*
機能: DBコネクション破棄
*/
func Destroy() error {
	_db.Close()
	return nil
}

/*
機能: クエリーを実行しMapで１行取得
	array:	実行クエリー
	params:	クエリー埋め込み用パラメータ
	row:	Map化したレコード
	err:	実行結果(nil=取得できたとき,nil以外=取得できなかったとき)
*/
func _selectRecord(query string, params ...interface{}) (row map[string]string, err error) {
	var mapRow = make(map[string]string)
	rows, err := _db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// カラム名取得
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// データ取得領域確保
	values := make([]sql.RawBytes, len(colNames))

	// データ取得用のパラメータ作成
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// 1レコード取得
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		// 文字列に変換してMapに格納
		var value string
		for i, colValue := range values {
			// Here we can check if the value is nil (NULL value)
			if colValue == nil {
				value = "NULL"
			} else {
				value = string(colValue)
			}
			mapRow[colNames[i]] = value
		}
		break
	}

	// SELECT結果が1行もない場合はErrNoRowsを返す
	if len(mapRow) == 0 {
		return nil, sql.ErrNoRows
	}
	return mapRow, nil
}

/*
機能: クエリーを実行しMapで１行取得
	array:	実行クエリー
	params:	クエリー埋め込み用パラメータ
	row:	Map化したレコード
	err:	実行結果(nil=取得できたとき,nil以外=取得できなかったとき)
*/
func selectRecord(query string, params ...interface{}) (row map[string]interface{}, err error) {
	var mapRow = make(map[string]interface{})
	rows, err := _db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// カラム名取得
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// データ取得領域確保
	//values := make([]sql.RawBytes, len(colNames))
	values := make([]interface{}, len(colNames))

	// データ取得用のパラメータ作成
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// 1レコード取得
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		// 文字列に変換してMapに格納
		//var value string
		for i, colValue := range values {
			// Here we can check if the value is nil (NULL value)
			/*			if colValue == nil {
							value = "NULL"
						} else {
							value = string(colValue)
						}
						mapRow[colNames[i]] = value
			*/
			mapRow[colNames[i]] = colValue
		}
		break
	}

	// SELECT結果が1行もない場合はErrNoRowsを返す
	if len(mapRow) == 0 {
		return nil, sql.ErrNoRows
	}
	return mapRow, nil
}

/*
機能: クエリーを実行しMapで複数行取得
	array:	実行クエリー
	params:	クエリー埋め込み用パラメータ
	rs:		Map化したレコードの配列
	err:	実行結果(nil=取得できたとき,nil以外=取得できなかったとき)
*/
func _selectRecords(query string, params ...interface{}) (rs []map[string]string, err error) {
	var mapRows []map[string]string
	var mapRow = make(map[string]string)
	var copyRow map[string]string
	rows, err := _db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// カラム名取得
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// データ取得領域確保
	values := make([]sql.RawBytes, len(colNames))

	// データ取得用のパラメータ作成
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// 1レコード取得
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		// 文字列に変換してMapに格納
		var value string
		for i, colValue := range values {
			// Here we can check if the value is nil (NULL value)
			if colValue == nil {
				value = "NULL"
			} else {
				value = string(colValue)
			}
			mapRow[colNames[i]] = value
		}

		// Mapを複製して追加
		copyRow = make(map[string]string)
		for key, value := range mapRow {
			copyRow[key] = value
		}
		mapRows = append(mapRows, copyRow)
	}

	// SELECT結果が1行もない場合はErrNoRowsを返す
	if len(mapRows) == 0 {
		return nil, sql.ErrNoRows
	}
	return mapRows, nil
}

/*
機能: クエリーを実行しMapで複数行取得
	array:	実行クエリー
	params:	クエリー埋め込み用パラメータ
	rs:		Map化したレコードの配列
	err:	実行結果(nil=取得できたとき,nil以外=取得できなかったとき)
*/
func selectRecords(query string, params ...interface{}) (rs []map[string]interface{}, err error) {
	var mapRows []map[string]interface{}
	var mapRow = make(map[string]interface{})
	var copyRow map[string]interface{}
	rows, err := _db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// カラム名取得
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// データ取得領域確保
	//values := make([]sql.RawBytes, len(colNames))
	values := make([]interface{}, len(colNames))

	// データ取得用のパラメータ作成
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		// 1レコード取得
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		// 文字列に変換してMapに格納
		//var value string
		for i, colValue := range values {
			// Here we can check if the value is nil (NULL value)
			/*if colValue == nil {
				value = "NULL"
			} else {
				value = string(colValue)
			}
			mapRow[colNames[i]] = value*/
			mapRow[colNames[i]] = colValue
		}

		// Mapを複製して追加
		copyRow = make(map[string]interface{})
		for key, value := range mapRow {
			copyRow[key] = value
		}
		mapRows = append(mapRows, copyRow)
	}

	// SELECT結果が1行もない場合はErrNoRowsを返す
	if len(mapRows) == 0 {
		return nil, sql.ErrNoRows
	}
	return mapRows, nil
}
func selectRecordTest(query string, params ...interface{}) (row map[string]interface{}, err error) {
	var myMap = make(map[string]interface{})
	rows, err := _db.Query(query, params...)
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
	}
	return myMap, nil
}

/*
機能: クエリーを実行
	array:	実行クエリー
	params:	クエリー埋め込み用パラメータ
	err:	実行結果
*/
func execStatement(query string, params ...interface{}) (err error) {
	if _, err := _db.Exec(query, params...); err != nil {
		return err
	}
	return nil
}

/*
機能: トランザクション開始
*/
func StartTransaction() error {
	var err error
	_tranStatus = DB_NO_ERROR
	_tx, err = _db.Begin()
	if err != nil {
		return err
	}
	return nil
}

/*
機能: トランザクション終了
*/
func EndTransaction() error {
	if _tranStatus == DB_NO_ERROR {
		err := _tx.Commit()
		if err != nil {
			return err
		}
	} else {
		err := _tx.Rollback()
		if err != nil {
			return err
		}
	}
	return nil
}

/*
機能: int型変換
	value: 変換元データ
*/
func _toInt(value interface{}) int {
	var val int

	switch value.(type) {
	case int:
		val = value.(int)
	case int8:
		val = int(value.(int8))
	case int16:
		val = int(value.(int16))
	case int32:
		val = int(value.(int32))
	case int64:
		val = int(value.(int64))
	default:
		val = value.(int)
	}
	return val
}

/*
機能: int型変換
	value: 変換元データ
*/
func _toInt64(value interface{}) int64 {
	var val int64

	switch value.(type) {
	case int:
		val = int64(value.(int))
	case int8:
		val = int64(value.(int8))
	case int16:
		val = int64(value.(int16))
	case int32:
		val = int64(value.(int32))
	case int64:
		val = value.(int64)
	default:
		val = int64(value.(int))
	}
	return val
}

/*
機能: 文字列型変換
	value: 変換元データ
*/
func _toString(value interface{}) string {
	var val string

	switch value.(type) {
	case []uint8:
		val = string(value.([]byte))
	}
	return val
}
