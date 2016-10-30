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
機能: クエリーを実行し１行取得
	array: 実行クエリー
	params: クエリー埋め込み用パラメータ
	row: Map化したレコード
	err: 終了ステータス
*/
func selectRecord(query string, params ...interface{}) (row map[string]interface{}, err error) {
	var mapRow = make(map[string]interface{})
	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	for rows.Next() {
		err = rows.Scan(colPtrs...)
		if err != nil {
			return nil, err
		}
		for i, col := range cols {
			mapRow[colNames[i]] = col
		}
		break
	}

	if len(mapRow) == 0 {
		return nil, sql.ErrNoRows
	}
	return mapRow, nil
}
