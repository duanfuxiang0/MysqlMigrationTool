package core

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

const SecretKey = "key"

var MasterDb *sql.DB

func init() {
	var err error

	// init mysql
	MasterDb, err = sql.Open("mysql", "root:123@tcp(127.0.0.1:3306)/testdb?"+
		"parseTime=true&loc=Local&charset=utf8")
	if err != nil {
		fmt.Printf("mysql open error: %v\n)", err)
	}
	MasterDb.SetMaxOpenConns(100)
	MasterDb.SetMaxIdleConns(50)
	MasterDb.SetConnMaxLifetime(time.Minute)
}

type NullBytes struct {
	Bytes []byte
	Valid bool // Valid is true if []byte is not NULL
}

type User struct {
	Id       int64          `json:"id"`
	Avatar   NullBytes      `json:"avatar"`
	Name     sql.NullString `json:"name"`
	Gender   sql.NullInt64  `json:"gender"`
	Birthday mysql.NullTime `json:"birthday"`
	Email    sql.NullString `json:"email"`
	Location sql.NullString `json:"location"`
	Address  sql.NullString `json:"address"`
	Ctime    time.Time      `json:"ctime"`
	Mtime    time.Time      `json:"mtime"`
}

func (ub User) OldDbTable() string {
	return "testdb.user"
}

func (ub *User) InsertToNew() error {
	query := `INSERT IGNORE INTO user_new(
              id,avatar,name,gender,birthday,email,address,ctime,mtime)
              VALUES(?,?,?,?,?,?,?,?,?);`
	res, err := MasterDb.Exec(query,
		ub.Id, NullBytes2PtrBytes(ub.Avatar), ub.Name, ub.Gender,
		NullTime2DateFmt(ub.Birthday), ub.Email, ub.Address,
		ub.Ctime, ub.Mtime)
	fmt.Printf("user InsertToNew res: %v\n", res)
	return err
}

func (ub *User) UpdateToNew() error {
	query := `UPDATE user_new SET
              id=?,avatar=?,name=?,gender=?,birthday=?,
              email=?,address=?,ctime=?,mtime=?;`
	res, err := MasterDb.Exec(query, ub.Id, NullBytes2PtrBytes(ub.Avatar), ub.Name, ub.Gender,
		NullTime2DateFmt(ub.Birthday), ub.Email, ub.Address,
		ub.Ctime, ub.Mtime)
	fmt.Printf("user UpdateToNew res: %v\n", res)
	return err
}

func (ub *User) DeleteFromNew() error {
	query := "DELETE FROM user_new WHERE id=?"

	res, err := MasterDb.Exec(query, ub.Id)
	fmt.Printf("user DeleteFromNew res: %v\n", res)
	return err
}

type Email struct {
	Id   int64          `json:"id"`
	Email sql.NullString `json:"email"`
	Ctime time.Time      `json:"ctime"`
	Mtime time.Time      `json:"mtime"`
}

func (ue Email) OldDbTable() string {
	return "testdb.user_email"
}

func (ue *Email) InsertToNew() error {

	query := `INSERT IGNORE INTO user_email_new(
              id,email,ctime,mtime)
              VALUES(?,?,?,?)`
	res, err := MasterDb.Exec(query,
		ue.Id, ue.Email, ue.Ctime, ue.Mtime)
	fmt.Printf("user_email_new InsertToNew res: %v\n", res)
	return err
}

func (ue *Email) UpdateToNew() error {

	query := `UPDATE user_email_new SET
              email=?,ctime=?,mtime=?
              WHERE id=?`
	res, err := MasterDb.Exec(query,
		ue.Email, ue.Ctime, ue.Mtime, ue.Id)
	fmt.Printf("user_email_new UpdateToNew res: %v\n", res)
	return err
}

func (ue *Email) DeleteToNew() error {
	query := "DELETE FROM user_email_new WHERE id=?"

	res, err := MasterDb.Exec(query, ue.Id)
	fmt.Printf("user_email_new DeleteFromNew res: %v\n", res)
	return err
}

type Mobile struct {
	Id    int64          `json:"id"`
	Mobile sql.NullString `json:"mobile"`
	Ctime  time.Time      `json:"ctime"`
	Mtime  time.Time      `json:"mtime"`
}

func (um Mobile) OldDbTable() string {
	return "testdb.user_mobile"
}

func (um *Mobile) InsertToNew() error {

	query := `INSERT IGNORE INTO user_mobile_new(
              id,mobile,ctime,mtime)
              VALUES(?,?,?,?)`
	res, err := MasterDb.Exec(query,
		um.Id, um.Mobile, um.Ctime, um.Mtime)
	fmt.Printf("user_mobile_new InsertToNew res: %v\n", res)
	return err
}

func (um *Mobile) UpdateToNew() error {

	query := `UPDATE user_mobile_new SET
              mobile=?,ctime=?,mtime=?
              WHERE id=?`
	res, err := MasterDb.Exec(query,
		um.Mobile, um.Ctime, um.Mtime, um.Id)
	fmt.Printf("user_mobile_new UpdateToNew res: %v\n", res)
	return err
}

func (um *Mobile) DeleteFromNew() error {

	query := "DELETE FROM user_mobile_new WHERE id=?"
	res, err := MasterDb.Exec(query, um.Id)
	fmt.Printf("user_mobile_new DeleteFromNew res: %v\n", res)
	return err
}

// help function
func NullTime2DateFmt(t mysql.NullTime) sql.NullString {

	var str string
	var valid bool
	if t.Valid {
		str = t.Time.Format("2006-01-02")
		valid = true
	}
	return sql.NullString{String: str, Valid: valid}
}

func NullBytes2PtrBytes(nb NullBytes) *[]byte {
	if nb.Valid {
		return &nb.Bytes
	} else {
		return nil
	}
}
