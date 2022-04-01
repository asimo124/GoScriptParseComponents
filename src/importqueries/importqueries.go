package importqueries

import (
	"fmt"
	"strings"
)
import _ "github.com/go-sql-driver/mysql"



var Basic int

func ImportQueries() (allowance int, deduction int) {

	Basic = 2
	return
}

func convertStrToDate(str string) string {
	strArr := strings.Split(str, "-")
	if (len(strArr) > 2) {
		year := strArr[0][len(strArr[0])-2:]
		return strArr[1] + "/" + strArr[2] + " 06:00:00AM '" + year + " -0600"
	}
	fmt.Println("Date format must be in YYYY-mm-dd")
	return ""
}
func test(num int) {

}