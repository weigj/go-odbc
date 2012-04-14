// Copyright (c) 2011, Wei guangjing <vcc.163@gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

/*
#cgo darwin LDFLAGS: -lodbc
#cgo freebsd LDFLAGS: -lodbc
#cgo linux LDFLAGS: -lodbc
#cgo windows LDFLAGS: -lodbc32

#include <stdio.h>
#include <stdlib.h>

#ifdef __MINGW32__
  #include <windef.h>
#else
  typedef void* HANDLE;
#endif

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

SQLRETURN _SQLColAttribute (
	SQLHSTMT        StatementHandle,
	SQLUSMALLINT    ColumnNumber,
	SQLUSMALLINT    FieldIdentifier,
	SQLPOINTER      CharacterAttributePtr,
	SQLSMALLINT     BufferLength,
	SQLSMALLINT *   StringLengthPtr,
	void *        NumericAttributePtr) {
		return SQLColAttribute(StatementHandle,
			ColumnNumber,
			FieldIdentifier,
			CharacterAttributePtr,
			BufferLength,
			StringLengthPtr,
			NumericAttributePtr);
}

*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

const (
	BUFFER_SIZE     = 10 * 1024
	INFO_BUFFER_LEN = 256
)

var (
	Genv C.SQLHANDLE
)

type Connection struct {
	Dbc       C.SQLHANDLE
	connected bool
}

type Statement struct {
	executed   bool
	prepared   bool
	scrollable bool

	handle C.SQLHANDLE
}

type ODBCError struct {
	SQLState     string
	NativeError  int
	ErrorMessage string
}

func (e *ODBCError) Error() string {
	return e.String()
}

func (e *ODBCError) String() string {
	if e != nil {
		return e.SQLState + " " + e.ErrorMessage
	}
	return ""
}

func initEnv() (err *ODBCError) {
	ret := C.SQLAllocHandle(C.SQL_HANDLE_ENV, nil, &Genv)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_ENV, Genv)
		return err
	}
	ret = C.SQLSetEnvAttr(C.SQLHENV(Genv), C.SQL_ATTR_ODBC_VERSION, C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_OV_ODBC3))), C.SQLINTEGER(0))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_ENV, Genv)
		return err
	}
	return nil
}

func Connect(dsn string, params ...interface{}) (conn *Connection, err *ODBCError) {
	var h C.SQLHANDLE
	ret := C.SQLAllocHandle(C.SQL_HANDLE_DBC, Genv, &h)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, h)
		return nil, err
	}

	var stringLength2 C.SQLSMALLINT
	outBuf := make([]byte, BUFFER_SIZE*2)
	outConnectionString := (*C.SQLWCHAR)(unsafe.Pointer(&outBuf[0]))

	ret = C.SQLDriverConnectW(C.SQLHDBC(h),
		C.SQLHWND(unsafe.Pointer(uintptr(0))),
		(*C.SQLWCHAR)(unsafe.Pointer(StringToUTF16Ptr(dsn))),
		C.SQL_NTS,
		outConnectionString,
		BUFFER_SIZE,
		&stringLength2,
		C.SQL_DRIVER_NOPROMPT)

	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, h)
		return nil, err
	}
	return &Connection{Dbc: h, connected: true}, nil
}

func (conn *Connection) ExecDirect(sql string) (stmt *Statement, err *ODBCError) {
	if stmt, err = conn.newStmt(); err != nil {
		return nil, err
	}
	wsql := StringToUTF16Ptr(sql)
	ret := C.SQLExecDirectW(C.SQLHSTMT(stmt.handle), (*C.SQLWCHAR)(unsafe.Pointer(wsql)), C.SQL_NTS)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		stmt.Close()
		return nil, err
	}
	stmt.executed = true
	return stmt, nil
}

func (conn *Connection) newStmt() (*Statement, *ODBCError) {
	stmt := &Statement{}

	ret := C.SQLAllocHandle(C.SQL_HANDLE_STMT, conn.Dbc, &stmt.handle)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return nil, err
	}
	return stmt, nil
}

func (conn *Connection) Prepare(sql string, params ...interface{}) (*Statement, *ODBCError) {
	wsql := StringToUTF16Ptr(sql)
	stmt, err := conn.newStmt()
	if err != nil {
		return nil, err
	}
	ret := C.SQLPrepareW(C.SQLHSTMT(stmt.handle), (*C.SQLWCHAR)(unsafe.Pointer(wsql)), C.SQLINTEGER(len(sql)))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		stmt.Close()
		return nil, err
	}
	stmt.prepared = true
	return stmt, nil
}

func (conn *Connection) Commit() (err *ODBCError) {
	ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_COMMIT)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return
}

func (conn *Connection) AutoCommit(b bool) (err *ODBCError) {
	var n C.int
	if b {
		n = C.SQL_AUTOCOMMIT_ON
	} else {
		n = C.SQL_AUTOCOMMIT_OFF
	}
	ret := C.SQLSetConnectAttr(C.SQLHDBC(conn.Dbc), C.SQL_ATTR_AUTOCOMMIT, C.SQLPOINTER(unsafe.Pointer(uintptr(n))), C.SQL_IS_UINTEGER)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return
}

func (conn *Connection) BeginTransaction() (err *ODBCError) {
	ret := C.SQLSetConnectAttr(C.SQLHDBC(conn.Dbc), C.SQL_ATTR_AUTOCOMMIT, C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_AUTOCOMMIT_OFF))), C.SQL_IS_UINTEGER)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return
}

func (conn *Connection) Rollback() (err *ODBCError) {
	ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_ROLLBACK)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
	}
	return
}

func (conn *Connection) ServerInfo() (string, string, string, *ODBCError) {
	var info_len C.SQLSMALLINT
	p := make([]byte, INFO_BUFFER_LEN)
	ret := C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DATABASE_NAME, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return "", "", "", err
	}
	db := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DBMS_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return db, "", "", err
	}
	ver := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_SERVER_NAME, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return db, ver, "", err
	}
	server := string(p[0:info_len])
	return db, ver, server, nil
}

func (conn *Connection) ClientInfo() (string, string, string, *ODBCError) {
	var info_len C.SQLSMALLINT
	p := make([]byte, INFO_BUFFER_LEN)
	ret := C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DRIVER_NAME, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return "", "", "", err
	}
	drv_name := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DRIVER_ODBC_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return "", "", "", err
	}
	drv_odbc_ver := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DRIVER_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
		return "", "", "", err
	}
	drv_ver := string(p[0:info_len])
	return drv_name, drv_odbc_ver, drv_ver, nil
}

func (conn *Connection) Close() *ODBCError {
	if conn.connected {
		ret := C.SQLDisconnect(C.SQLHDBC(conn.Dbc))
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
			return err
		}
		ret = C.SQLFreeHandle(C.SQL_HANDLE_DBC, conn.Dbc)
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc)
			return err
		}
		conn.connected = false
	}
	return nil
}

func (stmt *Statement) RowsAffected() (int, *ODBCError) {
	var nor C.SQLLEN
	ret := C.SQLRowCount(C.SQLHSTMT(stmt.handle), &nor)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return -1, err
	}
	return int(nor), nil
}

func (stmt *Statement) Cancel() *ODBCError {
	ret := C.SQLCancel(C.SQLHSTMT(stmt.handle))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}
	return nil
}

func (stmt *Statement) NumParams() int {
	var cParams C.SQLSMALLINT
	ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams)
	if !Success(ret) {
		return -1
	}
	return int(cParams)
}

func (stmt *Statement) Execute(params ...interface{}) *ODBCError {
	if params != nil {
		var cParams C.SQLSMALLINT
		ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams)
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := C.SQLExecute(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == C.SQL_NO_DATA {
		// Execute NO DATA
	} else if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Execute2(params []driver.Value) *ODBCError {
	if params != nil {
		var cParams C.SQLSMALLINT
		ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams)
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := C.SQLExecute(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NEED_DATA {
		// TODO
		//		send_data(stmt)
	} else if ret == C.SQL_NO_DATA {
		// Execute NO DATA
	} else if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}
	stmt.executed = true
	return nil
}

func (stmt *Statement) Fetch() (bool, *ODBCError) {
	ret := C.SQLFetch(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NO_DATA {
		return false, nil
	}
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return false, err
	}
	return true, nil
}

type Row struct {
	Data []interface{}
}

// Get(Columnindex)
// TODO Get(ColumnName)
func (r *Row) Get(a interface{}) interface{} {
	value := reflect.ValueOf(a)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return r.Data[f.Int()]
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return r.Data[f.Uint()]
		//	case *reflect.StringValue:
		//		i := r.Meta[f.Get()]
		//		return r.Data[i]
	}
	return nil
}

func (r *Row) GetInt(a interface{}) (ret int64) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ret = int64(f.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		ret = int64(f.Uint())
	}
	return
}

func (r *Row) GetFloat(a interface{}) (ret float64) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.Float32, reflect.Float64:
		ret = float64(f.Float())
	}
	return
}

func (r *Row) GetString(a interface{}) (ret string) {
	v := r.Get(a)
	value := reflect.ValueOf(v)
	switch f := value; f.Kind() {
	case reflect.String:
		ret = f.String()
	}
	return
}

func (r *Row) Length() int {
	return len(r.Data)
}

func (stmt *Statement) FetchAll() (rows []*Row, err *ODBCError) {
	for {
		row, err := stmt.FetchOne()
		if err != nil || row == nil {
			break
		}
		rows = append(rows, row)
	}

	return rows, err
}

func (stmt *Statement) FetchOne() (*Row, *ODBCError) {
	ok, err := stmt.Fetch()
	if !ok {
		return nil, err
	}
	n, _ := stmt.NumFields()
	row := new(Row)
	row.Data = make([]interface{}, n)
	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row.Data[i] = v
	}
	return row, nil
}

func (stmt *Statement) FetchOne2(row []driver.Value) (eof bool, err *ODBCError) {
	ok, err := stmt.Fetch()
	if !ok && err == nil {
		return !ok, nil
	} else if err != nil {
		return false, err
	}
	n, _ := stmt.NumFields()
	for i := 0; i < n; i++ {
		v, _, _, _ := stmt.GetField(i)
		row[i] = v
	}
	return false, nil
}

func (stmt *Statement) GetField(field_index int) (v interface{}, ftype int, flen int, err *ODBCError) {
	var field_type C.int
	var field_len C.SQLLEN
	var ll C.SQLSMALLINT
	ret := C._SQLColAttribute(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_DESC_CONCISE_TYPE, C.SQLPOINTER(unsafe.Pointer(uintptr(0))), C.SQLSMALLINT(0), &ll, unsafe.Pointer(&field_type))
	if !Success(ret) {
		// TODO return err
	}
	ret = C._SQLColAttribute(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_DESC_LENGTH, C.SQLPOINTER(unsafe.Pointer(uintptr(0))), C.SQLSMALLINT(0), &ll, unsafe.Pointer(&field_len))
	if !Success(ret) {
		// TODO return err
	}
	var fl C.SQLLEN = C.SQLLEN(field_len)
	switch int(field_type) {
	case C.SQL_BIT:
		var value C.BYTE
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_BIT, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = byte(value)
		}
	case C.SQL_INTEGER, C.SQL_SMALLINT, C.SQL_TINYINT:
		var value C.long
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_LONG, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int(value)
		}
	case C.SQL_BIGINT:
		var value C.longlong
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_SBIGINT, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = int64(value)
		}
	case C.SQL_FLOAT, C.SQL_REAL, C.SQL_DOUBLE:
		var value C.double
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_DOUBLE, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = float64(value)
		}
	case C.SQL_CHAR, C.SQL_VARCHAR, C.SQL_LONGVARCHAR, C.SQL_WCHAR, C.SQL_WVARCHAR, C.SQL_WLONGVARCHAR:
		value := make([]uint16, int(field_len)+8)
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_WCHAR, C.SQLPOINTER(unsafe.Pointer(&value[0])), field_len+4, &fl)
		s := UTF16ToString(value)
		v = s
	case C.SQL_TYPE_TIMESTAMP, C.SQL_TYPE_DATE, C.SQL_TYPE_TIME, C.SQL_DATETIME:
		var value C.TIMESTAMP_STRUCT
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_TYPE_TIMESTAMP, C.SQLPOINTER(unsafe.Pointer(&value)), C.SQLLEN(unsafe.Sizeof(value)), &fl)
		if fl == -1 {
			v = nil
		} else {
			v = time.Date(int(value.year), time.Month(value.month), int(value.day), int(value.hour), int(value.minute), int(value.second), int(value.fraction), time.UTC)
		}
	case C.SQL_BINARY, C.SQL_VARBINARY, C.SQL_LONGVARBINARY:
		var vv int
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_BINARY, C.SQLPOINTER(unsafe.Pointer(&vv)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			value := make([]byte, fl)
			ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_BINARY, C.SQLPOINTER(unsafe.Pointer(&value[0])), C.SQLLEN(fl), &fl)
			v = value
		}
	default:
		value := make([]byte, field_len)
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_BINARY, C.SQLPOINTER(unsafe.Pointer(&value[0])), field_len, &fl)
		v = value
	}
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_STMT, stmt.handle)
	}
	return v, int(field_type), int(fl), err
}

func (stmt *Statement) NumFields() (int, *ODBCError) {
	var NOC C.SQLSMALLINT
	ret := C.SQLNumResultCols(C.SQLHSTMT(stmt.handle), &NOC)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return -1, err
	}
	return int(NOC), nil
}

func (stmt *Statement) GetParamType(index int) (int, int, int, int, *ODBCError) {
	var data_type, dec_ptr, null_ptr C.SQLSMALLINT
	var size_ptr C.SQLULEN
	ret := C.SQLDescribeParam(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(index), &data_type, &size_ptr, &dec_ptr, &null_ptr)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return -1, -1, -1, -1, err
	}
	return int(data_type), int(size_ptr), int(dec_ptr), int(null_ptr), nil
}

func (stmt *Statement) BindParam(index int, param interface{}) *ODBCError {
	var ValueType C.SQLSMALLINT
	var ParameterType C.SQLSMALLINT
	var ColumnSize C.SQLULEN
	var DecimalDigits C.SQLSMALLINT
	var ParameterValuePtr C.SQLPOINTER
	var BufferLength C.SQLLEN
	var StrLen_or_IndPt C.SQLLEN
	v := reflect.ValueOf(param)
	if param == nil {
		ft, _, _, _, err := stmt.GetParamType(index)
		if err != nil {
			return err
		}
		ParameterType = C.SQLSMALLINT(ft)
		if ParameterType == C.SQL_UNKNOWN_TYPE {
			ParameterType = C.SQL_VARCHAR
		}
		ValueType = C.SQL_C_DEFAULT
		StrLen_or_IndPt = C.SQL_NULL_DATA
		ColumnSize = 1
	} else {
		switch v.Kind() {
		case reflect.Bool:
			ParameterType = C.SQL_BIT
			ValueType = C.SQL_C_BIT
			var b [1]byte
			if v.Bool() {
				b[0] = 1
			} else {
				b[0] = 0
			}
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&b[0]))
			BufferLength = 1
			StrLen_or_IndPt = 0
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch v.Type().Kind() {
			case reflect.Int:
			case reflect.Int8, reflect.Int16, reflect.Int32:
				ParameterType = C.SQL_INTEGER
				ValueType = C.SQL_C_LONG
				var l C.long = C.long(v.Int())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&l))
				BufferLength = 4
				StrLen_or_IndPt = 0
			case reflect.Int64:
				ParameterType = C.SQL_BIGINT
				ValueType = C.SQL_C_SBIGINT
				var ll C.longlong = C.longlong(v.Int())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&ll))
				BufferLength = 8
				StrLen_or_IndPt = 0
			}
		case reflect.Float32, reflect.Float64:
			ParameterType = C.SQL_DOUBLE
			ValueType = C.SQL_C_DOUBLE
			var d C.double = C.double(v.Float())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&d))
			BufferLength = 8
			StrLen_or_IndPt = 0
		case reflect.Complex64, reflect.Complex128:
		case reflect.String:
			var slen C.SQLUINTEGER = C.SQLUINTEGER(len(v.String()))
			ParameterType = C.SQL_VARCHAR
			ValueType = C.SQL_C_CHAR
			s := []byte(v.String())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&s[0]))
			ColumnSize = C.SQLULEN(slen)
			BufferLength = C.SQLLEN(slen + 1)
			StrLen_or_IndPt = C.SQLLEN(slen)
		default:
			fmt.Println("Not support type", v)
		}
	}
	ret := C.SQLBindParameter(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(index), C.SQL_PARAM_INPUT, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_IndPt)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return err
	}

	return nil
}

func (stmt *Statement) NextResult() bool {
	ret := C.SQLMoreResults(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NO_DATA {
		return false
	}
	return true
}

func (stmt *Statement) NumRows() (int, *ODBCError) {
	var NOR C.SQLLEN
	ret := C.SQLRowCount(C.SQLHSTMT(stmt.handle), &NOR)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return -1, err
	}
	return int(NOR), nil
}

func (stmt *Statement) HasRows() bool {
	n, _ := stmt.NumRows()
	return n > 0
}

type Field struct {
	Name          string
	Type          int
	Size          int
	DecimalDigits int
	Nullable      int
}

func (stmt *Statement) FieldMetadata(col int) (*Field, *ODBCError) {
	var BufferLength C.SQLSMALLINT = INFO_BUFFER_LEN
	var NameLength C.SQLSMALLINT
	var DataType C.SQLSMALLINT
	var ColumnSize C.SQLULEN
	var DecimalDigits C.SQLSMALLINT
	var Nullable C.SQLSMALLINT
	ColumnName := make([]byte, INFO_BUFFER_LEN)
	ret := C.SQLDescribeCol(C.SQLHSTMT(stmt.handle),
		C.SQLUSMALLINT(col),
		(*C.SQLCHAR)(unsafe.Pointer(&ColumnName[0])),
		BufferLength,
		&NameLength,
		&DataType,
		&ColumnSize,
		&DecimalDigits,
		&Nullable)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle)
		return nil, err
	}
	field := &Field{string(ColumnName[0:NameLength]), int(DataType), int(ColumnSize), int(DecimalDigits), int(Nullable)}
	return field, nil
}

func (stmt *Statement) free() {
	C.SQLFreeHandle(C.SQL_HANDLE_STMT, stmt.handle)
}

func (stmt *Statement) Close() {
	stmt.free()
}

func Success(ret C.SQLRETURN) bool {
	return int(ret) == C.SQL_SUCCESS || int(ret) == C.SQL_SUCCESS_WITH_INFO
}

func FormatError(ht C.SQLSMALLINT, h C.SQLHANDLE) (err *ODBCError) {
	sqlState := make([]uint16, 6)
	var nativeError C.SQLINTEGER
	messageText := make([]uint16, C.SQL_MAX_MESSAGE_LENGTH)
	var textLength C.SQLSMALLINT
	err = &ODBCError{}
	i := 0
	for {
		i++
		ret := C.SQLGetDiagRecW(C.SQLSMALLINT(ht),
			h,
			C.SQLSMALLINT(i),
			(*C.SQLWCHAR)(unsafe.Pointer(&sqlState[0])),
			&nativeError,
			(*C.SQLWCHAR)(unsafe.Pointer(&messageText[0])),
			C.SQL_MAX_MESSAGE_LENGTH,
			&textLength)
		if ret == C.SQL_INVALID_HANDLE || ret == C.SQL_NO_DATA {
			break
		}
		if i == 1 { // first error message save the SQLSTATE.
			err.SQLState = UTF16ToString(sqlState)
			err.NativeError = int(nativeError)
		}
		err.ErrorMessage += UTF16ToString(messageText)
	}

	return err
}

func init() {
	if err := initEnv(); err != nil {
		panic("odbc init env error!" + err.String())
	}
}
