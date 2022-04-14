package main

import "fmt"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type District struct {
	id   int    `json:"id"`
	title string `json:"name"`
}

func main() {j

	// Open up our database connection.
	db, err := sql.Open("mysql", "root:eStud10@/e2lyii_new2")

	// if there is an error opening the connection, handle it
	if err != nil {
		fmt.Print(err.Error())
	}
	defer db.Close()

	// Execute the query
	results, err := db.Query("SELECT id, title FROM district")
	if err != nil {
		fmt.Print(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var district District
		// for each row, scan the result into our district composite object
		err = results.Scan(&district.id, &district.title)
		if err != nil {
			fmt.Print(err.Error()) // proper error handling instead of panic in your app
		}
		// and then print out the district's Name attribute
		fmt.Printf(district.title, "\n")
	}
}
