package dao

import _ "github.com/go-sql-driver/mysql"

/*
機能: 最も古いアクセスログを取得
	row:	取得レコード
	err:	取得結果
*/
func GetOldAccessLog() (row map[string]interface{}, err error) {
	// 先頭のアクセスログを取得
	var serial int64
	query := "SELECT min(al_serial) FROM _access_log"
	if err := _db.QueryRow(query).Scan(&serial); err != nil { // レコードなしの場合は終了
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
}

/*
機能: アクセス解析状態を取得
	key:	取得キー
	value:	取得値(値なしの場合は空文字列)
*/
func GetStatus(key string) (value string) {
	var val string
	query := "SELECT as_value FROM _analyze_status "
	query += "WHERE as_id  = ?"
	_db.QueryRow(query).Scan(&val)

	// レコードなしの場合は空文字列が返る
	return val
}
