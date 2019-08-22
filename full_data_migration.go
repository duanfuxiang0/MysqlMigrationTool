package main

import (
	"database/sql"
	"fmt"
	
	_ "github.com/go-sql-driver/mysql"
	
	"github.com/fuxiangduan/MysqlMigrationTool/core"
)

var (
	SlaveDb *sql.DB
	
    startKey int64
	endKey int64
	pageSize int64
)

func init() {
	var err error
	
	// init mysql
	SlaveDb, err = sql.Open("mysql", "test:test@tcp(10.60.82.115:3306)/liebao_game_user_account?parseTime=true&loc=Local&charset=utf8mb4")
	if err != nil {
		fmt.Printf("mysql open error: %v\n)", err)
	}
	
	if err := SlaveDb.Ping(); err != nil {
		fmt.Println(err)
	}
	
	startKey = 793896608835043328
	endKey = 115966021812945715
	pageSize = 100
}

func selectBatch(from int64, limit int64) (*sql.Rows, error) {
	query := `select id,avatar,name,gender,birthday,
              email,address,ctime,mtime
              from user where uid > ? order by id limit ?;`
	res, err := SlaveDb.Query(query, from, limit)
	print(res)
	return res, err
}

func multiInsert(rows *sql.Rows) (lastUid int64, err error) {
	defer rows.Close()
	
	sqlStr := `INSERT IGNORE INTO user_new(id,avatar,name,gender,birthday,
              email,address,ctime,mtime) VALUES `
	var values []interface{}
	for rows.Next() {
		sqlStr += `(?,?,?,?,?,?,?,?,?),`
		ub := &core.User{}
		if err := rows.Scan(ub.Id, ub.Avatar, ub.Name, ub.Gender,
			ub.Birthday, ub.Email, ub.Address,ub.Ctime, ub.Mtime); err != nil {
			if err == sql.ErrNoRows {
				lastUid = ub.Id
			} else {
				fmt.Printf("rows.Scan error: %v", err)
			}
		}
		values = append(values, ub.Id, core.NullBytes2PtrBytes(ub.Avatar), ub.Name,
			ub.Gender, core.NullTime2DateFmt(ub.Birthday), ub.Email, ub.Address,
			ub.Ctime, ub.Mtime)
	}
	sqlStr = sqlStr[0:len(sqlStr)-2] // trim the last ,
	if _, err = core.MasterDb.Exec(sqlStr, values...); err != nil {
		return 0, err
	}
	return lastUid, err
}

func main() {
	fmt.Println("begin ...")
	
	from := startKey
	for from < endKey {
		rows, err := selectBatch(from, pageSize)
		if err != nil {
			fmt.Printf("multi select error : %v\n", err)
		}
		from, err = multiInsert(rows)
		if err != nil {
			fmt.Printf("multi insert error : %v\n", err)
		}
	}
}
