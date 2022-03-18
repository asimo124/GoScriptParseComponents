package main

import (
	"encoding/csv"
	"fmt"
	"os"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type CsvLine struct {
	id 				string
	user_id 		string
	ip 				string
	event 			string
	event_details 	string
	created_at 		string
	updated_at 		string
}

func main() {
	copyEventLog()
}
func copyEventLog() {
	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print("Error 2: " + err.Error())
	}
	defer db.Close()
	fileName := "event_log_csv_20220318.csv"
	file := "/Users/alexhawley/Documents/tmp/prod_import/" + fileName
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}
	db.Exec("TRUNCATE event_log")
	for i, line := range lines {
		if (i > 0) {
			data := CsvLine{
				id:            line[0],
				user_id:       line[1],
				ip:            line[2],
				event:         line[3],
				event_details: line[4],
				created_at:    line[5],
				updated_at:    line[6],
			}
			db.Exec("INSERT INTO event_log " +
				"(    id,              user_id,               ip,                event,  " +
				"		  event_details,               created_at,         updated_at) VALUES " +
				"(" + data.id + ", " + data.user_id + ", '" + data.ip + "', '" + data.event + "', " +
				"	 '" + data.event_details + "', " + data.created_at + ", " + data.updated_at + ") ")
		}
	}
	fmt.Println("Script completed")
}