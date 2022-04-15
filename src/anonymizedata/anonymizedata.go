package main

import (
	"encoding/csv"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"log"
	"os"
	"strconv"
	"strings"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import _ "github.com/brianvoe/gofakeit"

func main() {
	if len(os.Args) < 6 {
		fmt.Println("")
		fmt.Println("Invalid usage:")
		fmt.Println("CORRECT Usage: anonymizedata {fileName} {districtColumnIndexes} {campusColumnIndexes} {emailColumnIndexes} {firstNameColumnIndexes} {lastNameColumnIndexes} {fullNameColumnIndexes} {columnCountIndex}")
		fmt.Println("")
		fmt.Println("{fileName} - name of source file, which should be in /Users/alexhawley/Documents/tmp/go_enrich_data")
		fmt.Println("{districtColumnIndexes} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{campusColumnIndexes} - comma-separated list of column indexes that need to have an anonymized campus name")
		fmt.Println("{emailColumnIndexes} - comma-separated list of column indexes that need to have an anonymized email (will use the same one for all columns)")
		fmt.Println("{firstNameColumnIndexes} - comma-separated list of column indexes that need to have an anonymized first name")
		fmt.Println("{lastNameColumnIndexes} - comma-separated list of column indexes that need to have an anonymized last name")
		fmt.Println("{fullNameColumnIndexes} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{columnCount} - count of valid columns")
		fmt.Println("")
		fmt.Println("")
	}

	fileName := os.Args[1]
	districtColumnIndexes := os.Args[2]
	campusColumnIndexes := os.Args[3]
	emailColumnIndexes := os.Args[4]
	firstNameColumnIndexes := os.Args[5]
	lastNameColumnIndexes := os.Args[6]
	fullNameColumnIndexes := os.Args[7]
	columnCountStr := os.Args[8]

	columnCount64, err := strconv.ParseInt(columnCountStr, 10, 64)
	if err != nil {
		fmt.Print("Error 1.5: " + err.Error())
		os.Exit(1)
	}
	columnCount := int(columnCount64)

	fmt.Println("Name: ", gofakeit.Name())
	os.Exit(0)

	anonymizeData(fileName, districtColumnIndexes, campusColumnIndexes, emailColumnIndexes, firstNameColumnIndexes,
		lastNameColumnIndexes, fullNameColumnIndexes, columnCount)
}
func test(num int) {

}
func anonymizeData(fileName string, districtColumnIndexes string, campusColumnIndexes string, emailColumnIndexes string,
	firstNameColumnIndexes string, lastNameColumnIndexes string, fullNameColumnIndexes string, columnCount int) {
	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print("Error 2: " + err.Error())
	}
	defer db.Close()

	fileNameOutput := strings.Replace(fileName, ".csv", "_output.csv", 1)
	filename := "/Users/alexhawley/Documents/tmp/go_enrich_data/" + fileName
	filenameOutput := "/Users/alexhawley/Documents/tmp/go_enrich_data/" + fileNameOutput
	f, err := os.Open(filename)

	if err != nil {
		panic(err)
	}
	defer f.Close()
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}
	csvFile, err := os.Create(filenameOutput)
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)

	for i, line := range lines {

		/*/
		boyEndDateStr := line[boyEndDateTimeStampIndex]
		value := convertStrToDateStandard(boyEndDateStr)
		t, _ := time.Parse(layout, value)
		boyEndDate := strconv.FormatInt(t.Unix(), 10)
		//*/

		/**
		 * Query all User Information as well as Coaching Touches - "Current Year"
		 */

		//if line[emailColumnIndex] != "" { // if email IS provided

		if i == 0 { // first line

			var records2 []string
			j := 0
			test(j)
			for i := 0; i < columnCount; i++ {
				records2 = append(records2, line[i])
				j = i
			}

			records2 = append(records2, "email")
			records2 = append(records2, "total_coaching_touches")
			records2 = append(records2, "user_id")
			records2 = append(records2, "district_id")
			records2 = append(records2, "distrrict")
			records2 = append(records2, "campus_id")
			records2 = append(records2, "campus")
			records2 = append(records2, "first_name")
			records2 = append(records2, "last_name")
			records2 = append(records2, "user")
			records2 = append(records2, "user_type")
			records2 = append(records2, "title")
			records2 = append(records2, "last_login")
			records2 = append(records2, "assigned_coach_user_id")
			records2 = append(records2, "coaching_source")
			records2 = append(records2, "assigned_coach")
			records2 = append(records2, "coaching_assignment")

			// BOY - current year
			records2 = append(records2, "total_badges_earned_boy_current_year")
			records2 = append(records2, "level1_badges_earned_count_boy_current_year")
			records2 = append(records2, "level2_badges_earned_count_boy_current_year")
			records2 = append(records2, "level3_badges_earned_count_boy_current_year")
			records2 = append(records2, "level4_badges_earned_count_boy_current_year")
			// BOY - cumulative
			records2 = append(records2, "total_badges_earned_boy_cumulative")
			records2 = append(records2, "level1_badges_earned_count_boy_cumulative")
			records2 = append(records2, "level2_badges_earned_count_boy_cumulative")
			records2 = append(records2, "level3_badges_earned_count_boy_cumulative")
			records2 = append(records2, "level4_badges_earned_count_boy_cumulative")

			// MOY - current year
			records2 = append(records2, "total_badges_earned_moy_current_year")
			records2 = append(records2, "level1_badges_earned_count_moy_current_year")
			records2 = append(records2, "level2_badges_earned_count_moy_current_year")
			records2 = append(records2, "level3_badges_earned_count_moy_current_year")
			records2 = append(records2, "level4_badges_earned_count_moy_current_year")
			// MOY - cumulative
			records2 = append(records2, "total_badges_earned_moy_cumulative")
			records2 = append(records2, "level1_badges_earned_count_moy_cumulative")
			records2 = append(records2, "level2_badges_earned_count_moy_cumulative")
			records2 = append(records2, "level3_badges_earned_count_moy_cumulative")
			records2 = append(records2, "level4_badges_earned_count_moy_cumulative")
			_ = csvwriter.Write(records2)
			csvwriter.Flush()

		} else { // after first line

			/*for results.Next() {
				var records2 []string
				j := 0
				for i := 0; i < columnCount; i++ {
					records2 = append(records2, line[i])
					j = i
				}
				test(j)

				records2 = append(records2, userInfo.email)
				records2 = append(records2, strconv.Itoa(userInfo.TotalCoachingTouches))
				records2 = append(records2, strconv.Itoa(userInfo.user_id))
				records2 = append(records2, strconv.Itoa(userInfo.district_id))
				records2 = append(records2, userInfo.district)
				records2 = append(records2, strconv.Itoa(userInfo.campus_id))
				records2 = append(records2, userInfo.campus)
				records2 = append(records2, userInfo.first_name)
				records2 = append(records2, userInfo.last_name)
				records2 = append(records2, userInfo.user)
				records2 = append(records2, userInfo.user_type)
				records2 = append(records2, userInfo.title)
				records2 = append(records2, strconv.Itoa(userInfo.last_login))
				records2 = append(records2, strconv.Itoa(userInfo.assigned_coach_user_id))
				records2 = append(records2, userInfo.coaching_source)
				records2 = append(records2, userInfo.assigned_coach)
				records2 = append(records2, userInfo.coaching_assignment)

				_ = csvwriter.Write(records2)
			}*/
		}

		//} else { // if no email provided

		/*/
			var records2 []string
			j := 0
			for i := 0; i < columnCount; i++ {
				records2 = append(records2, line[i])
				j = i
			}
			_ = csvwriter.Write(records2)
			for i := j; i < 38; i++ {
				records2 = append(records2, "")
			}
			_ = csvwriter.Write(records2)
		//*/

		//}
		/*/
		if i > 30 {
			break
		}
		//*/
	}
	csvwriter.Flush()
	csvFile.Close()
	fmt.Println("Script completed")
}
