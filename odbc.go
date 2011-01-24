// Copyright (c) 2011, Wei guangjing <vcc.163@gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

/*
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

*/
import "C"
import (
	"unsafe"
	"fmt"
	"reflect"
)

const (
	BUFFER_SIZE     = 10 * 1024
	INFO_BUFFER_LEN = 256
)

var (
	Genv            C.SQLHANDLE
	SQL_NULL_HANDLE C.SQLHANDLE
	Debug           bool = false
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

func initEnv() (err *ODBCError) {
	ret := C.SQLAllocHandle(C.SQL_HANDLE_ENV, SQL_NULL_HANDLE, &Genv)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_ENV, Genv, int(ret))
		debugPrint("ERROR:", err)
		return err
	}
	ret = C.SQLSetEnvAttr(C.SQLHENV(Genv), C.SQL_ATTR_ODBC_VERSION, C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_OV_ODBC3))), C.SQLINTEGER(0))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_ENV, Genv, int(ret))
		debugPrint("ERROR:", err)
		return err
	}
	return nil
}

func Connect(dsn string, params ...interface{}) (conn *Connection, err *ODBCError) {
	var h C.SQLHANDLE
	ret := C.SQLAllocHandle(C.SQL_HANDLE_DBC, Genv, &h)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, h, int(ret))
		debugPrint("SQLAllocHandle ERROR=", err)
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
		err := FormatError(C.SQL_HANDLE_DBC, h, int(ret))
		debugPrint("SQLDriverConnect ERROR=", err)
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
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("SQLExecDirectW Error", err)
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
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("Commit Error", err)
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
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("Prepare Error", err)
		stmt.Close()
		return nil, err
	}
	stmt.prepared = true
	return stmt, nil
}

//func (conn *Connection) Query(sql string, params ...interface{}) (stmt *Statement, err *ODBCError) {
//	//TODO 
//}

func (conn *Connection) Commit() (err *ODBCError) {
	ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_COMMIT)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("Commit Error", err)
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
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("Commit Error", err)
	}
	return
}

func (conn *Connection) BeginTransaction() (err *ODBCError) {
	ret := C.SQLSetConnectAttr(C.SQLHDBC(conn.Dbc), C.SQL_ATTR_AUTOCOMMIT, C.SQLPOINTER(unsafe.Pointer(uintptr(C.SQL_AUTOCOMMIT_OFF))), C.SQL_IS_UINTEGER)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("BeginTransaction Error", err)
	}
	return
}

func (conn *Connection) Rollback() (err *ODBCError) {
	ret := C.SQLEndTran(C.SQL_HANDLE_DBC, conn.Dbc, C.SQL_ROLLBACK)
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLEndTran Error", err)
	}
	return
}

func (conn *Connection) ServerInfo() (string, string, string, *ODBCError) {
	var info_len C.SQLSMALLINT
	p := make([]byte, INFO_BUFFER_LEN)
	ret := C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DATABASE_NAME, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
		return "", "", "", err
	}
	db := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DBMS_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
		return db, "", "", err
	}
	ver := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_SERVER_NAME, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
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
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
		return "", "", "", err
	}
	drv_name := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DRIVER_ODBC_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
		return "", "", "", err
	}
	drv_odbc_ver := string(p[0:info_len])
	ret = C.SQLGetInfo(C.SQLHDBC(conn.Dbc), C.SQL_DRIVER_VER, C.SQLPOINTER(unsafe.Pointer(&p[0])), INFO_BUFFER_LEN, &info_len)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
		debugPrint("SQLGetInfo ERROR=", err)
		return "", "", "", err
	}
	drv_ver := string(p[0:info_len])
	return drv_name, drv_odbc_ver, drv_ver, nil
}

func (conn *Connection) Close() *ODBCError {
	if conn.connected {
		ret := C.SQLDisconnect(C.SQLHDBC(conn.Dbc))
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_DBC, conn.Dbc, int(ret))
			debugPrint("SQLDisconnect Error=", err)
			return err
		}
		ret = C.SQLFreeHandle(C.SQL_HANDLE_DBC, conn.Dbc)
		if !Success(ret) {
			debugPrint("SQLFreeHandle Error=", ret)
		}
		conn.connected = false
	}
	return nil
}

func (stmt *Statement) RowsAffected() (int, *ODBCError) {
	var nor C.SQLINTEGER
	ret := C.SQLRowCount(C.SQLHSTMT(stmt.handle), &nor)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("SQLRowCount Error", err)
		return -1, err
	}
	return int(nor), nil
}

func (stmt *Statement) Cancel() *ODBCError {
	ret := C.SQLCancel(C.SQLHSTMT(stmt.handle))
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("Cancel Error", err)
		return err
	}
	return nil
}

func (stmt *Statement) Execute(params ...interface{}) *ODBCError {
	if params != nil {
		var cParams C.SQLSMALLINT
		ret := C.SQLNumParams(C.SQLHSTMT(stmt.handle), &cParams)
		if !Success(ret) {
			err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
			debugPrint("SQLNumParams Error", err)
			return err
		}
		for i := 0; i < int(cParams); i++ {
			stmt.BindParam(i+1, params[i])
		}
	}
	ret := C.SQLExecute(C.SQLHSTMT(stmt.handle))
	if ret == C.SQL_NEED_DATA {
		debugPrint("Execute NEED DATA")
		// TODO
		//		send_data(stmt)
	} else if ret == C.SQL_NO_DATA {
		debugPrint("Execute NO DATA")
	} else if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("Execute Error", ret, err)
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
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("Fetch Error", err)
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
	value := reflect.NewValue(a)
	switch f := value.(type) {
	case *reflect.IntValue:
		return r.Data[f.Get()]
	case *reflect.UintValue:
		return r.Data[f.Get()]
		//	case *reflect.StringValue:
		//		i := r.Meta[f.Get()]
		//		return r.Data[i]
	}
	return nil
}

func (r *Row) GetInt(a interface{}) (ret int64) {
	v := r.Get(a)
	value := reflect.NewValue(v)
	switch f := value.(type) {
	case *reflect.IntValue:
		ret = int64(f.Get())
	case *reflect.UintValue:
		ret = int64(f.Get())
	}
	return
}

func (r *Row) GetFloat(a interface{}) (ret float64) {
	v := r.Get(a)
	value := reflect.NewValue(v)
	switch f := value.(type) {
	case *reflect.FloatValue:
		ret = float64(f.Get())
	}
	return
}

func (r *Row) GetComplex(a interface{}) (ret complex) {
	v := r.Get(a)
	value := reflect.NewValue(v)
	switch f := value.(type) {
	case *reflect.ComplexValue:
		ret = complex(f.Get())
	}
	return
}

func (r *Row) GetString(a interface{}) (ret string) {
	v := r.Get(a)
	value := reflect.NewValue(v)
	switch f := value.(type) {
	case *reflect.StringValue:
		ret = f.Get()
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

func (stmt *Statement) GetField(field_index int) (v interface{}, ftype int, flen int, err *ODBCError) {
	var field_type C.int
	var field_len C.int
	var ll C.SQLSMALLINT
	ret := C.SQLColAttribute(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_DESC_CONCISE_TYPE, C.SQLPOINTER(unsafe.Pointer(uintptr(0))), C.SQLSMALLINT(0), &ll, C.SQLPOINTER(unsafe.Pointer(&field_type)))
	if !Success(ret) {
		debugPrint("GetFiled type Error")
	}
	ret = C.SQLColAttribute(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_DESC_LENGTH, C.SQLPOINTER(unsafe.Pointer(uintptr(0))), C.SQLSMALLINT(0), &ll, C.SQLPOINTER(unsafe.Pointer(&field_len)))
	if !Success(ret) {
		debugPrint("GetFiled len Error")
	}
	var fl C.SQLLEN = C.SQLLEN(field_len)
	switch int(field_type) {
	case C.SQL_BIT, C.SQL_INTEGER, C.SQL_SMALLINT, C.SQL_TINYINT:
		var value C.HANDLE
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_LONG, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = value
		}
	case C.SQL_FLOAT, C.SQL_REAL:
		var value C.double
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_DOUBLE, C.SQLPOINTER(unsafe.Pointer(&value)), 0, &fl)
		if fl == -1 {
			v = nil
		} else {
			v = value
		}
	case C.SQL_WCHAR, C.SQL_WVARCHAR, C.SQL_WLONGVARCHAR:
		value := make([]uint16, int(field_len)+8)
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_WCHAR, C.SQLPOINTER(unsafe.Pointer(&value[0])), C.SQLINTEGER(int(field_len)+4), &fl)
		s := UTF16ToString(value)
		v = s
	default:
		value := make([]byte, int(fl)+2)
		ret = C.SQLGetData(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(field_index+1), C.SQL_C_CHAR, C.SQLPOINTER(unsafe.Pointer(&value[0])), C.SQLINTEGER(int(field_len)+4), &fl)
		s := string(value[0:])
		v = s
		debugPrint("default type", value, fl, s)
	}
	if !Success(ret) {
		err = FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("GetFiled Data Error", err)
	}
	return v, int(field_type), int(fl), err
}

func (stmt *Statement) NumFields() (int, *ODBCError) {
	var NOC C.SQLSMALLINT
	ret := C.SQLNumResultCols(C.SQLHSTMT(stmt.handle), &NOC)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("NumFields Error", err)
		return -1, err
	}
	return int(NOC), nil
}

func (stmt *Statement) GetParamType(index int) (int, int, int, int, *ODBCError) {
	var data_type, dec_ptr, null_ptr C.SQLSMALLINT
	var size_ptr C.SQLULEN
	ret := C.SQLDescribeParam(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(index), &data_type, &size_ptr, &dec_ptr, &null_ptr)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("GetParamType Error", err)
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
	v := reflect.NewValue(param)
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
		switch v := v.(type) {
		case *reflect.BoolValue:
			ParameterType = C.SQL_BIT
			ValueType = C.SQL_C_BIT
			var b [1]byte
			if v.Get() {
				b[0] = 1
			} else {
				b[0] = 0
			}
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&b[0]))
			BufferLength = 1
			StrLen_or_IndPt = 0
		case *reflect.IntValue:
			switch v.Type().Kind() {
			case reflect.Int:
			case reflect.Int8, reflect.Int16, reflect.Int32:
				ParameterType = C.SQL_INTEGER
				ValueType = C.SQL_C_LONG
				var l C.long = C.long(v.Get())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&l))
				BufferLength = 4
				StrLen_or_IndPt = 0
			case reflect.Int64:
				ParameterType = C.SQL_BIGINT
				ValueType = C.SQL_C_SBIGINT
				var ll C.longlong = C.longlong(v.Get())
				ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&ll))
				BufferLength = 8
				StrLen_or_IndPt = 0
			}
		case *reflect.FloatValue:
			ParameterType = C.SQL_DOUBLE
			ValueType = C.SQL_C_DOUBLE
			var d C.double = C.double(v.Get())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&d))
			BufferLength = 8
			StrLen_or_IndPt = 0
		case *reflect.ComplexValue:
		case *reflect.StringValue:
			var slen C.SQLUINTEGER = C.SQLUINTEGER(len(v.Get()))
			ParameterType = C.SQL_VARCHAR
			ValueType = C.SQL_C_CHAR
			s := []byte(v.Get())
			ParameterValuePtr = C.SQLPOINTER(unsafe.Pointer(&s[0]))
			ColumnSize = slen
			BufferLength = C.SQLINTEGER(slen + 1)
			StrLen_or_IndPt = C.SQLINTEGER(slen)
		default:
			debugPrint("Not support type", v)
		}
	}
	ret := C.SQLBindParameter(C.SQLHSTMT(stmt.handle), C.SQLUSMALLINT(index), C.SQL_PARAM_INPUT, ValueType, ParameterType, ColumnSize, DecimalDigits, ParameterValuePtr, BufferLength, &StrLen_or_IndPt)
	if !Success(ret) {
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("SQLBindParameter Error", err)
		return err
	}

	return nil
}

//func (stmt *Statement) BindCol(col int, data interface{}, buff_indicator int) *ODBCError {
//	// TODO
//	switch f := data.(type) {
//	case bool: //1 SQL_BIT
//	case int: //4
//	case string:
//	case float: //4
//	case float64: //8
//	case complex: //8
//	default:
//	}
//	//ret := C.SQLBindCol(C.SQLHSTMT(stmt.handle), col, C.SQL_C_CHAR, &data, len(data), &buff_indicator)
//	return nil
//}

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
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("SQLRowCount Error", err)
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
		err := FormatError(C.SQL_HANDLE_STMT, stmt.handle, int(ret))
		debugPrint("SQLDescribeCol Error", err)
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

func (err *ODBCError) String() string {
	if err != nil {
		return err.SQLState + " " + string(err.NativeError) + " " + err.ErrorMessage
	}
	return ""
}

func FormatError(ht C.SQLSMALLINT, h C.SQLHANDLE, val_ret int) (err *ODBCError) {
	sqlState := make([]byte, 6)
	var nativeError C.SQLINTEGER
	messageText := make([]byte, BUFFER_SIZE*10)
	var textLength C.SQLSMALLINT
	debugPrint("FormatError", val_ret)
	for {
		ret := C.SQLGetDiagRec(C.SQLSMALLINT(ht),
			h,
			C.SQLSMALLINT(val_ret),
			(*C.SQLCHAR)(unsafe.Pointer(&sqlState[0])),
			&nativeError,
			(*C.SQLCHAR)(unsafe.Pointer(&messageText[0])),
			BUFFER_SIZE*2,
			&textLength)
		switch ret {
		case C.SQL_INVALID_HANDLE:
			debugPrint("SQL_INVALID_HANDLE")
			err = &ODBCError{SQLState: string(sqlState), NativeError: int(nativeError), ErrorMessage: "SQL_INVALID_HANDLE"}
		case C.SQL_NO_DATA:
		case C.SQL_SUCCESS:
			debugPrint("SQL_SUCCESS")
			err = &ODBCError{SQLState: string(sqlState), NativeError: int(nativeError), ErrorMessage: string(messageText)}
			continue
		case C.SQL_SUCCESS_WITH_INFO:
			debugPrint("SQLGetDiagRec => SQL_SUCCESS_WITH_INFO")
			err = &ODBCError{SQLState: string(sqlState), NativeError: int(nativeError), ErrorMessage: "SQL_SUCCESS_WITH_INFO"}
		case C.SQL_ERROR:
			debugPrint("SQLGetDiagRec => SQL_ERROR", sqlState, nativeError, messageText)
			err = &ODBCError{SQLState: string(sqlState), NativeError: int(nativeError), ErrorMessage: "SQL_ERROR"}
		default:
			debugPrint("SQLGetDiagRec => OTHER_ERROR")
			err = &ODBCError{SQLState: string(sqlState), NativeError: int(nativeError), ErrorMessage: "OTHER_SQL_ERROR"}
		}
		break
	}

	return err
}

func debugPrint(a ...interface{}) {
	if Debug {
		fmt.Println(a...)
	}
}

func init() {
	if err := initEnv(); err != nil {
		panic("odbc init env error!" + err.String())
	}
}
