package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const DAY_LAYOUT = "2006-01-02" // 日付フォーマット

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
	db, err := sql.Open("mysql", dbuser+":"+dbpwd+"@tcp("+host+")/"+dbname)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close() // 関数がリターンする直前に呼び出される

	err = updateDb(db)
	/*
		rows, err := db.Query("SELECT * FROM _login_user") //
		if err != nil {
			panic(err.Error())
		}

		columns, err := rows.Columns() // カラム名を取得
		if err != nil {
			panic(err.Error())
		}

		values := make([]sql.RawBytes, len(columns))

		//  rows.Scan は引数に `[]interface{}`が必要.

		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				panic(err.Error())
			}

			var value string
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				fmt.Println(columns[i], ": ", value)
			}
			fmt.Println("-----------------------------------")
		}
	*/
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
func updateDb(db *sql.DB) error {
	// 先頭のアクセスログを取得
	var serial int64
	query := "SELECT min(al_serial) FROM _access_log"
	if err := db.QueryRow(query).Scan(&serial); err != nil {
		return err
	}
	fmt.Println(serial)

	// 集計日付範囲取得
	//day := time.Now()
	//endData := day.Format(DAY_LAYOUT)

	query = "DELETE FROM _analyze_page_view "
	query += "WHERE ap_date = ? "

	if _, err := db.Exec(query, "2016/10/01"); err != nil {

	}
	/*query := "DELETE FROM user WHERE id=?"

	  if _, err := db.Exec(query, id); err != nil {
	      log.Fatal("delete error: ", err)
	  } else {
	      fmt.Println("delete complete! id =  ", id)
	  }*/
	fmt.Println("-----------------------------------")
return nil
}
