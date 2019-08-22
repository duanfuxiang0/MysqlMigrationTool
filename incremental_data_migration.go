package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	driverMysql "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/schema"

	"github.com/fuxiangduan/MysqlMigrationTool/core"
)

var (
	MyCanal *canal.Canal

	NilString = sql.NullString{Valid: false}
	NilInt64  = sql.NullInt64{Valid: false}
)

func init() {
	var err error
	// init canal
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", 3306)
	cfg.User = "root"
	cfg.Password = "123"
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""
	MyCanal, err = canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create new canal error: %v\n", err)
	}
}

// --------- binlog parser ----------

type BinlogParser struct{}

// get row event value by parse binlog
func (bp *BinlogParser) ParseBinLogRow(dest interface{}, rowsEvent *canal.RowsEvent, whichRow int) error {

	destVal := reflect.ValueOf(dest)
	realDest := reflect.Indirect(destVal)
	destModel := realDest.Type()
	num := destModel.NumField()
	for i := 0; i < num; i++ {
		parsedTag := parseTagSetting(destModel.Field(i).Tag)
		var (
			columnName string
			ok         bool
		)
		if columnName, ok = parsedTag["COLUMN"]; !ok || columnName == "COLUMN" {
			continue // not set column name
		}

		fieldTypeName := realDest.Field(i).Type().Name()
		switch fieldTypeName {
		case "bool":
			field := bp.boolHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetBool(field)
		case "string":
			field := bp.stringHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetString(field)
		case "NullString":
			field := bp.nullStringHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).Set(reflect.ValueOf(field))
		case "int":
			field := bp.intHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetInt(field)
		case "in8":
			field := bp.intHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetInt(field)
		case "int16":
			field := bp.intHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetInt(field)
		case "int32":
			field := bp.intHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetInt(field)
		case "int64":
			field := bp.intHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).SetInt(field)
		case "NullInt64":
			field := bp.nullInt64Helper(rowsEvent, whichRow, columnName)
			realDest.Field(i).Set(reflect.ValueOf(field))
		case "NullBytes":
			field := bp.nullBytesHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).Set(reflect.ValueOf(field))
		case "Time":
			timeField := bp.timeHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).Set(reflect.ValueOf(timeField))
		case "NullTime":
			timeField := bp.nullTimeHelper(rowsEvent, whichRow, columnName)
			realDest.Field(i).Set(reflect.ValueOf(timeField))
		}
	}
	return nil
}

func (bp *BinlogParser) timeHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) time.Time {

	columnId := bp.getColIdByName(rowsEvent, columnName)
	if rowsEvent.Table.Columns[columnId].Type != schema.TYPE_TIMESTAMP {
		panic("Not dateTime type")
	}
	t, _ := time.ParseInLocation("2006-01-02 15:04:05",
		rowsEvent.Rows[whichRow][columnId].(string),
		time.Local)
	return t
}

func (bp *BinlogParser) nullTimeHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) driverMysql.NullTime {

	columnId := bp.getColIdByName(rowsEvent, columnName)

	var (
		timeVal  time.Time
		validVal = true
		err      error
	)
	if rowsEvent.Table.Columns[columnId].Type != schema.TYPE_DATE {
		validVal = false
	}
	switch rowsEvent.Rows[whichRow][columnId].(type) {
	case string:
		timeVal, err = time.ParseInLocation("2006-01-02",
			rowsEvent.Rows[whichRow][columnId].(string),
			time.Local)
		if err != nil {
			validVal = false
		}
	default:
		validVal = false
	}
	return driverMysql.NullTime{Time: timeVal, Valid: validVal}
}

func (bp *BinlogParser) intHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) int64 {

	columnId := bp.getColIdByName(rowsEvent, columnName)
	if rowsEvent.Table.Columns[columnId].Type != schema.TYPE_NUMBER {
		return 0
	}

	switch rowsEvent.Rows[whichRow][columnId].(type) {
	case int8:
		return int64(rowsEvent.Rows[whichRow][columnId].(int8))
	case int32:
		return int64(rowsEvent.Rows[whichRow][columnId].(int32))
	case int64:
		return rowsEvent.Rows[whichRow][columnId].(int64)
	case int:
		return int64(rowsEvent.Rows[whichRow][columnId].(int))
	case uint8:
		return int64(rowsEvent.Rows[whichRow][columnId].(uint8))
	case uint16:
		return int64(rowsEvent.Rows[whichRow][columnId].(uint16))
	case uint32:
		return int64(rowsEvent.Rows[whichRow][columnId].(uint32))
	case uint64:
		return int64(rowsEvent.Rows[whichRow][columnId].(uint64))
	case uint:
		return int64(rowsEvent.Rows[whichRow][columnId].(uint))
	}
	return 0
}

func (bp *BinlogParser) nullInt64Helper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) sql.NullInt64 {

	columnId := bp.getColIdByName(rowsEvent, columnName)
	if rowsEvent.Table.Columns[columnId].Type != schema.TYPE_NUMBER {
		return NilInt64
	}

	var int64Val int64
	var validVal = true
	switch rowsEvent.Rows[whichRow][columnId].(type) {
	case int8:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(int8))
	case int32:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(int32))
	case int64:
		int64Val = rowsEvent.Rows[whichRow][columnId].(int64)
	case int:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(int))
	case uint8:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(uint8))
	case uint16:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(uint16))
	case uint32:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(uint32))
	case uint64:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(uint64))
	case uint:
		int64Val = int64(rowsEvent.Rows[whichRow][columnId].(uint))
	default:
		validVal = false
	}
	return sql.NullInt64{Int64: int64Val, Valid: validVal}
}

func (bp *BinlogParser) boolHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) bool {

	val := bp.intHelper(rowsEvent, whichRow, columnName)
	if val == 1 {
		return true
	}
	return false
}

func (bp *BinlogParser) stringHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) string {

	columnId := bp.getColIdByName(rowsEvent, columnName)
	if rowsEvent.Table.Columns[columnId].Type == schema.TYPE_ENUM {
		values := rowsEvent.Table.Columns[columnId].EnumValues
		if len(values) == 0 {
			return ""
		}
		if rowsEvent.Rows[whichRow][columnId] == nil {
			// If there's an empty line in the enum
			return ""
		}

		return values[rowsEvent.Rows[whichRow][columnId].(int64)-1]
	}

	value := rowsEvent.Rows[whichRow][columnId]

	switch value := value.(type) {
	case []byte:
		return string(value)
	case string:
		return value
	}
	return ""
}

func (bp *BinlogParser) nullStringHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) sql.NullString {

	columnId := bp.getColIdByName(rowsEvent, columnName)
	if rowsEvent.Table.Columns[columnId].Type == schema.TYPE_ENUM {
		values := rowsEvent.Table.Columns[columnId].EnumValues
		if len(values) == 0 {
			return NilString
		}
		if rowsEvent.Rows[whichRow][columnId] == nil {
			// If there's an empty line in the enum
			return NilString
		}
		value := values[rowsEvent.Rows[whichRow][columnId].(int64)-1]
		return sql.NullString{String: value, Valid: true}
	}

	value := rowsEvent.Rows[whichRow][columnId]

	switch value := value.(type) {
	case []byte:
		return sql.NullString{String: string(value), Valid: true}
	case string:
		return sql.NullString{String: value, Valid: true}
	}
	return NilString
}

func (bp *BinlogParser) nullBytesHelper(rowsEvent *canal.RowsEvent, whichRow int, columnName string) core.NullBytes {

	var bytes []byte
	var valid = true
	columnId := bp.getColIdByName(rowsEvent, columnName)
	value := rowsEvent.Rows[whichRow][columnId]

	switch value := value.(type) {
	case []byte:
		bytes = value
	case string:
		bytes = []byte(value)
	default:
		valid = false
	}
	return core.NullBytes{Bytes: bytes, Valid: valid}
}

func (bp *BinlogParser) getColIdByName(rowsEvent *canal.RowsEvent, columnName string) int {
	for id, value := range rowsEvent.Table.Columns {
		if value.Name == columnName {
			return id
		}
	}
	panic(fmt.Sprintf("There is no column %s in table %s.%s", columnName, rowsEvent.Table.Schema, rowsEvent.Table.Name))
}

// parse model tags, [copy from gorm source]
func parseTagSetting(tags reflect.StructTag) map[string]string {
	settings := map[string]string{}
	for _, str := range []string{tags.Get("sql"), tags.Get("orm")} {
		tags := strings.Split(str, ";")
		for _, value := range tags {
			v := strings.Split(value, ":")
			k := strings.TrimSpace(strings.ToUpper(v[0]))
			if len(v) >= 2 {
				settings[k] = strings.Join(v[1:], ":")
			} else {
				settings[k] = k
			}
		}
	}
	return settings
}

// --------- event handler ----------

type RowEventHandler struct {
	canal.DummyEventHandler
	BinlogParser
}

func (r *RowEventHandler) OnRow(rowsEvent *canal.RowsEvent) error {
	defer func() {
		if r := recover(); r != nil {
			fmt.Print(r, " ", string(debug.Stack()))
		}
	}()

	var start = 0
	var step = 1

	if rowsEvent.Action == canal.UpdateAction {
		start = 1
		step = 2
	}

	for i := start; i < len(rowsEvent.Rows); i += step {

		dbTable := rowsEvent.Table.Schema + "." + rowsEvent.Table.Name

		switch dbTable {
		case core.User{}.OldDbTable():
			user := &core.User{}
			if err := r.ParseBinLogRow(user, rowsEvent, i); err != nil {
				fmt.Printf("parse user err: %v\n", err)
				break
			}
			switch rowsEvent.Action {
			case canal.UpdateAction:
				oldUser := &core.User{}
				if err := r.ParseBinLogRow(oldUser, rowsEvent, i-1); err != nil {
					fmt.Printf("parse old user err: %v\n", err)
					break
				}
				fmt.Printf("UserBase %d changed from user: %v to user: %v\n", user.Id, oldUser, user)
				if err := user.UpdateToNew(); err != nil {
					fmt.Printf("exec user.UpdateToNew error: %v\n", err)
					break
				}
				fmt.Println("UserBase is changed")
			case canal.InsertAction:
				fmt.Printf("UserBase %d is created user: %v\n", user.Id, user)
				if err := user.InsertToNew(); err != nil {
					fmt.Printf("exec user.InsertToNew error: %v\n", err)
					break
				}
				fmt.Println("UserBase is created")
			case canal.DeleteAction:
				fmt.Printf("UserBase %d is deleted user: %v\n", user.Id, user)
				if err := user.DeleteFromNew(); err != nil {
					fmt.Printf("exec user.DeleteFromNew error: %v\n", err)
					break
				}
				fmt.Println("UserBase is deleted")
			}
		case core.Email{}.OldDbTable():
			email := &core.Email{}
			if err := r.ParseBinLogRow(email, rowsEvent, i); err != nil {
				fmt.Printf("parse email err: %v\n", err)
				break
			}
			switch rowsEvent.Action {
			case canal.UpdateAction:
				oldEmail := &core.Email{}
				if err := r.ParseBinLogRow(oldEmail, rowsEvent, i-1); err != nil {
					fmt.Printf("parse old email err: %v\n", err)
					break
				}
				fmt.Printf("Email %d changed from email: %v to mobile: %v\n", email.Id, oldEmail, email)
				if err := email.UpdateToNew(); err != nil {
					fmt.Printf("exec email.UpdateToNew error: %v\n", err)
					break
				}
				fmt.Println("Email is changed")
			case canal.InsertAction:
				fmt.Printf("Email %d is created email: %v\n", email.Id, email)
				if err := email.InsertToNew(); err != nil {
					fmt.Printf("exec email.InsertToNew err: %v\n", err)
					break
				}
				fmt.Println("Email is created")
			case canal.DeleteAction:
				fmt.Printf("Email %d is deleted email: %v\n", email.Id, email)
				if err := email.DeleteToNew(); err != nil {
					fmt.Printf("exec email.DeleteFromNew err: %v\n", err)
					break
				}
				fmt.Println("Email is deleted")
			}
		case core.Mobile{}.OldDbTable():
			mobile := &core.Mobile{}
			if err := r.ParseBinLogRow(mobile, rowsEvent, i); err != nil {
				fmt.Printf("parse mobile err: %v\n", err)
				break
			}
			switch rowsEvent.Action {
			case canal.UpdateAction:
				oldMobile := &core.Mobile{}
				if err := r.ParseBinLogRow(oldMobile, rowsEvent, i-1); err != nil {
					fmt.Printf("parse old mobile err: %v\n", err)
					break
				}
				fmt.Printf("Mobile %d changed from mobile: %v to mobile: %v\n", mobile.Id, oldMobile, mobile)
				if err := mobile.UpdateToNew(); err != nil {
					fmt.Printf("exec mobile.UpdateToNew error: %v\n", err)
					break
				}
				fmt.Println("Mobile is changed")
			case canal.InsertAction:
				fmt.Printf("Mobile %d is created mobile: %v\n", mobile.Id, mobile)
				if err := mobile.InsertToNew(); err != nil {
					fmt.Printf("exec mobile.InsertToNew error: %v\n", err)
					break
				}
				fmt.Println("Mobile is created")
			case canal.DeleteAction:
				fmt.Printf("Mobile %d is deleted mobile: %v\n", mobile.Id, mobile)
				if err := mobile.DeleteFromNew(); err != nil {
					fmt.Printf("exec mobile.DeleteFromNew error: %v\n", err)
					break
				}
				fmt.Println("Mobile is deleted")
			}
		}
	}
	return nil
}

func (r *RowEventHandler) String() string {
	return "RowEventHandler"
}

// ---------     main      ----------

func main() {
	defer func() {
		MyCanal.Close()
		_ = core.MasterDb.Close()
		fmt.Println("exit ...")
	}()
	go func() {
		MyCanal.SetEventHandler(&RowEventHandler{})
		pos := mysql.Position{Name: "mysql-bin.000002", Pos: 0} // 设置binlog的解析位置
		if err := MyCanal.RunFrom(pos); err != nil {
			fmt.Printf("binlog canal run from pos: %v\n", pos)
		}
	}()
	fmt.Printf("Ctrl-C to exit...")
	for {
		fmt.Printf("now time: %v \n", time.Now())
		time.Sleep(10 * time.Minute)
	}
}
