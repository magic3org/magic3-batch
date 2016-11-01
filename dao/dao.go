package dao

import (
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

/*
機能: 最も古いアクセスログを取得
	row:	取得レコード
	err:	取得結果
*/
func GetOldAccessLog() (row map[string]string, err error) {
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
func GetStatus(key string) (value string, er error) {
	query := "SELECT as_value FROM _analyze_status "
	query += "WHERE as_id  = ?"
	row, err := selectRecord(query, key)
	if err != nil {
		return "", err
	}

	// レコードなしの場合は空文字列が返る
	return row["as_value"], nil
}

/*
機能: アクセス解析状態を更新
	key:	取得キー
	value:	設定値
	error:	実行結果
*/
func UpdateStatus(key string, value string) error {
	now := time.Now()

	// 値を追加または更新
	query := "SELECT as_value FROM _analyze_status "
	query += "WHERE as_id = ?"
	_, err := selectRecord(query, key)
	if err == nil {
		query = "UPDATE _analyze_status "
		query += "SET as_value = ?, "
		query += "as_update_dt = ? "
		query += "WHERE as_id = ? "
		err = execStatement(query, value, now, key)
	} else {
		query = "INSERT INTO _analyze_status ("
		query += "as_id, "
		query += "as_value, "
		query += "as_update_dt "
		query += ") VALUES ("
		query += "?, ?, ?"
		query += ")"
		err = execStatement(query, key, value, now)
	}
	return err
}

/*
機能: 日付指定でアクセス解析の集計を行う
	date:	日付
	er:		実行結果
*/
func CalcDatePv(date time.Time) (er error) {
	var err error
	var query string
	var startDt, endDt time.Time
	var rows []map[string]string
	var row map[string]string
	var rowCount int
	var total int
	var path string
	//var startDtStr, endDtStr string

	// 一旦データをすべて削除
	query = "DELETE FROM _analyze_page_view "
	query += "WHERE ap_date = ? "
	if err = execStatement(query, date); err != nil {
		return err
	}
	query = "DELETE FROM _analyze_daily_count "
	query += "WHERE aa_date = ? "
	if err = execStatement(query, date); err != nil {
		return err
	}

	// 時間単位で集計
	for i := 0; i < 24; i++ {
		// 時間範囲
		startDt = time.Date(date.Year(), date.Month(), date.Day(), i, 0, 0, 0, time.Local)
		if i < 23 {
			endDt = time.Date(date.Year(), date.Month(), date.Day(), i+1, 0, 0, 0, time.Local)
		} else {
			endDt = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local)
			endDt = endDt.AddDate(0, 0, 1)
		}

		query = "SELECT COUNT(*) AS total,al_uri,al_path FROM _access_log "
		query += "WHERE (? <= al_dt AND al_dt < ?) "
		query += "AND al_is_cmd = false "
		query += "GROUP BY al_uri, al_path "
		query += "ORDER BY total DESC"
		rows, err = selectRecords(query, startDt, endDt)
		if err == nil { // レコードが存在する場合
			rowCount = len(rows)
			for j := 0; j < rowCount; j++ {
				row = rows[j]
				total, _ = strconv.Atoi(row["total"])
				path = row["al_path"]
			}
		}

	}
	return nil
}
