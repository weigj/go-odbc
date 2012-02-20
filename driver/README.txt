Driver for database/sql

Install:
	go install

Example:

package main

import (
   "database/sql" 
   _ "odbc/driver"
   "fmt"
)

func main() {
   db, err := sql.Open("odbc", "DSN=test;")
   defer db.Close()

   stmt, err :=	db.Prepare("select name from table")
   defer stmt.Close()

   rows, err :=	stmt.Query()
   defer rows.Close()

   for rows.Next() {
       var name string

       _ = rows.Scan(&name)
       fmt.Println(name)
   }
}