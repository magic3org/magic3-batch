package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/magic3org/magic3-batch/dao"
)

const DATE_LAYOUT = "2006-01-02" // 日付フォーマット
const TIMESTAMP_LAYOUT = "2006-01-02 15:04:05.999"
const CF_LAST_DATE_CALC_PV = "last_date_calc_pv" // ページビュー集計の最終更新日

func main() {
	// コマンドライン定義
	var defFilePath string
	flag.StringVar(&defFilePath, "path", "", "file path of siteDef.php")

	// パラメータエラーチェック
	flag.Parse()
	if len(defFilePath) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// サイト定義ファイルから定義値取得
	host, dbname, dbuser, dbpwd, err := parseDefFile(defFilePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// DBに接続
	err = dao.Init(host, dbname, dbuser, dbpwd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer dao.Destroy() // DBコネクション破棄

	err = updateDb()
}

/*
機能: Magic3のサイト定義ファイル(siteDef.php)から定義値を取得
*/
func parseDefFile(path string) (host string, dbname string, dbuser string, dbpwd string, err error) {
	// ファイル読み込み
	fp, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return "", "", "", "", fmt.Errorf("siteDef.php open failed: %s", path)
	}
	defer fp.Close()

	// define定義を取得
	defs := map[string]string{}
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		entry := scanner.Text()
		expstr := `^[ \t]*define\([ \t]*["'](.*)["'][ \t]*,[ \t]*["'](.*)["'][ \t]*\)`
		assigned := regexp.MustCompile(expstr)
		matched := assigned.FindStringSubmatch(entry)
		if len(matched) == 3 {
			defs[matched[1]] = matched[2]
		}
	}

	// DB接続情報取得
	dsn := map[string]string{}
	dbdef := defs["M3_DB_CONNECT_DSN"]

	// 先頭のmysql文字列除く
	if !strings.HasPrefix(dbdef, "mysql:") {
		return "", "", "", "", fmt.Errorf("not mysql dsn: %s", dbdef)
	}
	dbdef = strings.Trim(dbdef, "mysql:")
	for _, attrs := range strings.Split(dbdef, ";") {
		attr := strings.Split(attrs, "=")
		key := strings.TrimSpace(attr[0])
		value := strings.TrimSpace(attr[1])
		if key == "" {
			continue
		}
		dsn[key] = value
	}

	host = dsn["host"]
	dbname = dsn["dbname"]
	dbuser = defs["M3_DB_CONNECT_USER"]
	dbpwd = defs["M3_DB_CONNECT_PASSWORD"]
	pos := strings.Index(host, ":")
	if pos == -1 {
		host += ":3306"
	}
	return host, dbname, dbuser, dbpwd, nil
}

/*
機能: アクセス解析処理
*/
func updateDb() error {
	var maxDayCount int
	maxDayCount = 100 // 最大集計日数

	row, err := dao.GetOldAccessLog()
	if err != nil { // アクセスログがない場合は終了
		return err
	}

	// 集計日付範囲取得
	var date time.Time
	//date, _ = time.Parse(TIMESTAMP_LAYOUT, row["al_dt"]) // DB格納値をTime型に変換
	date = row["al_dt"].(time.Time)
	startDate := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local)

	// 集計完了日を取得
	lastDateStr, _ := dao.GetStatus(CF_LAST_DATE_CALC_PV)
	if lastDateStr != "" {
		date, _ = time.Parse(DATE_LAYOUT, lastDateStr)
		startDate = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, 1) // 集計完了日の翌日から集計
	}

	// 集計は本日の前日まで行う
	date = time.Now()
	endDate := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, -1)

	var dayCount int
	date = startDate
	for {
		// 指定範囲を越えた場合は終了
		if date.After(endDate) {
			fmt.Println("集計完了しました")
			break
		}

		// トランザクションスタート
		dao.StartTransaction()

		// 集計処理
		err = dao.CalcDatePv(date)
		if err == nil {
			// 集計完了日付を更新
			dao.UpdateStatus(CF_LAST_DATE_CALC_PV, date.Format(DATE_LAYOUT))
		} else {
			fmt.Println(err)
		}
		// トランザクション終了
		dao.EndTransaction()

		dayCount++
		if dayCount >= maxDayCount {
			break
		}
		date = date.AddDate(0, 0, 1)
	}
	return nil
}
