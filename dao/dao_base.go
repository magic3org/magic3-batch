package dao

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

/*
機能: DBコネクション作成
*/
func Init(host string, dbname string, dbuser string, dbpwd string) error {
	// DBに接続
	var err error
	db, err = sql.Open("mysql", dbuser+":"+dbpwd+"@tcp("+host+")/"+dbname)
	if err != nil {
		return err
	}
	return nil
}

/*
機能: DBコネクション破棄
*/
func Destroy() error {
	db.Close()
	return nil
}

/*
機能: クエリーを実行しMapで１行取得
	array: 実行クエリー
	params: クエリー埋め込み用パラメータ
	row: Map化したレコード
	err: 終了ステータス(nil=取得できたとき,nil以外=取得できなかったとき)
*/
func selectRecord(query string, params ...interface{}) (row map[string]interface{}, err error) {
	var mapRow = make(map[string]interface{})
	rows, err := db.Query(query, params...)
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
