package dao

import (
	"database/sql"

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
	// DBに接続
	var err error
	_db, err = sql.Open("mysql", dbuser+":"+dbpwd+"@tcp("+host+")/"+dbname)
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
