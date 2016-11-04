package dao

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const MAX_URL_LENGTH = 180 // URLの長さ最大値
//const TIMESTAMP_FORMAT = "2006-01-02 15:04:05.999" // 日付時間フォーマット

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
func GetStatus(key string) (value string, er error) {
	query := "SELECT as_value FROM _analyze_status "
	query += "WHERE as_id  = ?"
	row, err := selectRecord(query, key)
	if err != nil { // レコードなしの場合は空文字列が返る
		return "", err
	}

	// interface型を文字列型に変換
	val := string(row["as_value"].([]byte))
	return val, nil
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
	var rows []map[string]interface{}
	var row map[string]interface{}
	var rowCount int
	var serial int64
	var total, count int
	var path, uri, url string
	var rowUpdated bool // 更新したかどうか

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
				//total, _ = strconv.Atoi(row["total"])
				//total = int(row["total"].(int64))
				//path = string(row["al_path"].([]byte))
				//uri = string(row["al_uri"].([]byte))
				total = _toInt(row["total"])
				path = _toString(row["al_path"])
				uri = _toString(row["al_uri"])
				rowUpdated = false // 更新したかどうか

				url = makeTruncStr(uri, MAX_URL_LENGTH)
				if url == "" { // URLが空の場合は「/」とみなす
					url = "/"
				}

				if url != uri { // URLが長いときは省略形で登録
					// 既に登録されている場合は更新で登録
					query = "SELECT ap_serial, ap_count FROM _analyze_page_view "
					query += "WHERE ap_type = ? "
					query += "AND ap_url = ? "
					query += "AND ap_date = ? "
					query += "AND ap_hour = ?"
					row, err = selectRecord(query, 0 /*すべてのデータ*/, url, date, i)
					if err == nil { // レコードありの場合
						//serial, _ = strconv.ParseInt(row["ap_serial"], 10, 64)
						//count, _ = strconv.Atoi(row["ap_count"])
						//serial = row["ap_serial"].(int64)
						//count = row["ap_count"].(int)
						serial = _toInt64(row["ap_serial"])
						count = _toInt(row["ap_count"])
						count += total

						query = "UPDATE _analyze_page_view "
						query += "SET ap_count = ? "
						query += "WHERE ap_serial = ? "
						if err = execStatement(query, count, serial); err != nil {
							return err
						}

						rowUpdated = true // 更新完了
					}
				}
				if !rowUpdated { // データ更新していないとき
					query = "INSERT INTO _analyze_page_view ("
					query += "ap_type, "
					query += "ap_url, "
					query += "ap_date, "
					query += "ap_hour, "
					query += "ap_count, "
					query += "ap_path "
					query += ") VALUES ("
					query += "?, ?, ?, ?, ?, ?"
					query += ")"
					if err = execStatement(query, 0 /*すべてのデータ*/, url, date, i, total, path); err != nil {
						return err
					}
				}
			}
		}
	}
	// ##### 訪問数を集計 #####
	// 時間範囲
	startDt = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local)
	endDt = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local)
	endDt = endDt.AddDate(0, 0, 1) // 翌日

	// 一日あたりURLごとの集計
	query = "SELECT COUNT(DISTINCT al_session) AS total,al_uri,al_path FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "GROUP BY al_uri, al_path "
	query += "ORDER BY total DESC"
	rows, err = selectRecords(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		rowCount = len(rows)
		for j := 0; j < rowCount; j++ {
			row = rows[j]
			total = _toInt(row["total"])
			path = _toString(row["al_path"])
			uri = _toString(row["al_uri"])
			rowUpdated = false // 更新したかどうか

			url = makeTruncStr(uri, MAX_URL_LENGTH)
			if url == "" { // URLが空の場合は「/」とみなす
				url = "/"
			}

			if url != uri { // URLが長いときは省略形で登録
				query = "SELECT aa_serial, aa_count FROM _analyze_daily_count "
				query += "WHERE aa_type = ? "
				query += "AND aa_url = ? "
				query += "AND aa_date = ? "
				row, err = selectRecord(query, 0 /*訪問数*/, url, date)
				if err == nil { // レコードありの場合
					serial = _toInt64(row["aa_serial"])
					count = _toInt(row["aa_count"])
					count += total

					query = "UPDATE _analyze_daily_count "
					query += "SET aa_count = ? "
					query += "WHERE aa_serial = ? "
					if err = execStatement(query, count, serial); err != nil {
						return err
					}

					rowUpdated = true // 更新完了
				}
			}
			if !rowUpdated { // データ更新していないとき
				query = "INSERT INTO _analyze_daily_count ("
				query += "aa_type, "
				query += "aa_url, "
				query += "aa_date, "
				query += "aa_count, "
				query += "aa_path "
				query += ") VALUES ("
				query += "?, ?, ?, ?, ?"
				query += ")"
				if err = execStatement(query, 0 /*訪問数*/, url, date, total, path); err != nil {
					return err
				}
			}
		}
	}

	// 一日あたりアクセスポイントごとの集計
	query = "SELECT COUNT(DISTINCT al_session) AS total, al_path FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "GROUP BY al_path "
	query += "ORDER BY total DESC"

	rows, err = selectRecords(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		rowCount = len(rows)
		for j := 0; j < rowCount; j++ {
			row = rows[j]
			total = _toInt(row["total"])
			path = _toString(row["al_path"])

			// アクセスポイント以外のアクセスはカウントしない
			if path != "" {
				query = "INSERT INTO _analyze_daily_count ("
				query += "aa_type, "
				query += "aa_url, "
				query += "aa_date, "
				query += "aa_count, "
				query += "aa_path "
				query += ") VALUES ("
				query += "?, ?, ?, ?, ?"
				query += ")"
				if err = execStatement(query, 0 /*訪問数*/, "" /*アクセスポイント指定*/, date, total, path); err != nil {
					return err
				}
			}
		}
	}

	// 一日あたりすべてのアクセスポイントの集計
	query = "SELECT COUNT(DISTINCT al_session) AS total FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "ORDER BY total DESC"
	row, err = selectRecord(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		total = _toInt(row["total"])

		// 集計データを登録
		if total > 0 { // データありのとき
			query = "INSERT INTO _analyze_daily_count ("
			query += "aa_type, "
			query += "aa_url, "
			query += "aa_date, "
			query += "aa_count, "
			query += "aa_path "
			query += ") VALUES ("
			query += "?, ?, ?, ?, ?"
			query += ")"
			if err = execStatement(query, 0 /*訪問数*/, "" /*アクセスポイント指定*/, date, total, "" /*すべてのアクセスポイント*/); err != nil {
				return err
			}
		}
	}

	// ##### 訪問者数を集計 #####
	// 1日あたりURLごとの集計
	query = "SELECT COUNT(DISTINCT al_cookie_value) AS total,al_uri,al_path FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "GROUP BY al_uri, al_path "
	query += "ORDER BY total DESC"
	rows, err = selectRecords(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		rowCount = len(rows)
		for j := 0; j < rowCount; j++ {
			row = rows[j]
			total = _toInt(row["total"])
			path = _toString(row["al_path"])
			uri = _toString(row["al_uri"])
			rowUpdated = false // 更新したかどうか

			url = makeTruncStr(uri, MAX_URL_LENGTH)
			if url == "" { // URLが空の場合は「/」とみなす
				url = "/"
			}

			if url != uri { // URLが長いときは省略形で登録
				query = "SELECT aa_serial, aa_count FROM _analyze_daily_count "
				query += "WHERE aa_type = ? "
				query += "AND aa_url = ? "
				query += "AND aa_date = ? "
				row, err = selectRecord(query, 1 /*訪問者数*/, url, date)
				if err == nil { // レコードありの場合
					serial = _toInt64(row["aa_serial"])
					count = _toInt(row["aa_count"])
					count += total

					query = "UPDATE _analyze_daily_count "
					query += "SET aa_count = ? "
					query += "WHERE aa_serial = ? "
					if err = execStatement(query, count, serial); err != nil {
						return err
					}

					rowUpdated = true // 更新完了
				}
			}
			if !rowUpdated { // データ更新していないとき
				query = "INSERT INTO _analyze_daily_count ("
				query += "aa_type, "
				query += "aa_url, "
				query += "aa_date, "
				query += "aa_count, "
				query += "aa_path "
				query += ") VALUES ("
				query += "?, ?, ?, ?, ?"
				query += ")"
				if err = execStatement(query, 1 /*訪問者数*/, url, date, total, path); err != nil {
					return err
				}
			}
		}
	}
	// 1日あたりアクセスポイントごとの集計
	query = "SELECT COUNT(DISTINCT al_cookie_value) AS total, al_path FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "GROUP BY al_path "
	query += "ORDER BY total DESC"

	rows, err = selectRecords(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		// 集計データを登録
		rowCount = len(rows)
		for j := 0; j < rowCount; j++ {
			row = rows[j]
			total = _toInt(row["total"])
			path = _toString(row["al_path"])

			// アクセスポイント以外のアクセスはカウントしない
			if path != "" {

				query = "INSERT INTO _analyze_daily_count ("
				query += "aa_type, "
				query += "aa_url, "
				query += "aa_date, "
				query += "aa_count, "
				query += "aa_path "
				query += ") VALUES ("
				query += "?, ?, ?, ?, ?"
				query += ")"
				if err = execStatement(query, 1 /*訪問者数*/, "" /*アクセスポイント指定*/, date, total, path); err != nil {
					return err
				}
			}
		}
	}

	// 1日あたりすべてのアクセスの集計
	query = "SELECT COUNT(DISTINCT al_cookie_value) AS total FROM _access_log "
	query += "WHERE (? <= al_dt AND al_dt < ?) "
	query += "AND al_is_cmd = false "
	query += "ORDER BY total DESC"
	row, err = selectRecord(query, startDt, endDt)
	if err == nil { // レコードが存在する場合
		total = _toInt(row["total"])

		// 集計データを登録
		if total > 0 { // データありのとき
			query = "INSERT INTO _analyze_daily_count ("
			query += "aa_type, "
			query += "aa_url, "
			query += "aa_date, "
			query += "aa_count, "
			query += "aa_path "
			query += ") VALUES ("
			query += "?, ?, ?, ?, ?"
			query += ")"
			if err = execStatement(query, 1 /*訪問者数*/, "" /*アクセスポイント指定*/, date, total, "" /*すべてのアクセスポイント*/); err != nil {
				return err
			}
		}
	}
	return nil
}

/*
機能: 省略文字列を作成
	str:	変換元文字列
	length:	文字列長
	str:	作成した文字列
*/
func makeTruncStr(str string, length int) string {
	var destStr string

	if len(str) > length {
		destStr = str[0:length] + "..."
	} else {
		destStr = str
	}
	return destStr
}
