package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type UserInfo struct {
	email                  string `json:"name"`
	TotalCoachingTouches   int    `json:"id"`
	user_id                int    `json:"id"`
	district_id            int    `json:"id"`
	district               string `json:"name"`
	campus_id              int    `json:"id"`
	campus                 string `json:"name"`
	first_name             string `json:"name"`
	last_name              string `json:"name"`
	user                   string `json:"name"`
	user_type              string `json:"name"`
	title                  string `json:"name"`
	last_login             int    `json:"id"`
	assigned_coach_user_id int    `json:"id"`
	coaching_source        string `json:"name"`
	assigned_coach         string `json:"name"`
	coaching_assignment    string `json:"name"`
}
type UserBadges struct {
	email                      string `json:"name"`
	TotalBadgesEarned          int    `json:"id"`
	level1_badges_earned_count int    `json:"id"`
	level2_badges_earned_count int    `json:"id"`
	level3_badges_earned_count int    `json:"id"`
	level4_badges_earned_count int    `json:"id"`
}

func main() {
	if (len(os.Args) < 6) {
		fmt.Println("")
		fmt.Println("Invalid usage:")
		fmt.Println("CORRECT Usage: enrichdatav2_merged_static_EOY {fileName} {startDateStr} {boyEndDate} {moyEndDate}, {eoyEndDate} {emailColumnIndex} {columnCountIndex}")
		fmt.Println("")
		fmt.Println("{fileName} - name of source file, which should be in /Users/alexhawley/Documents/tmp/go_enrich_data")
		fmt.Println("{startDateStr} - start of date range - (YYYY-mm-dd)")
		fmt.Println("{boyEndDate} - BOY end of date range - (YYYY-mm-dd)")
		fmt.Println("{moyEndDate} - MOY end of date range - (YYYY-mm-dd)")
		fmt.Println("{eoyEndDate} - EOY end of date range - (YYYY-mm-dd)")
		fmt.Println("{emailColumnIndex} - the index of which column (0 based) that contains the email")
		fmt.Println("{columnCountIndex} - the number of valid columns in the source file")
		fmt.Println("")
		fmt.Println("")
	}

	layout := "01/02 03:04:05PM '06 -0700"
	fileName := os.Args[1]
	startDateStr := os.Args[2]
	boyEndDateStr := os.Args[3]
	moyEndDateStr := os.Args[4]
	eoyEndDateStr := os.Args[5]
	emailColumnIndexStr := os.Args[6]
	columnCountStr := os.Args[7]


	emailColumnIndex, err := strconv.ParseInt(emailColumnIndexStr, 10, 64)
	if err != nil {
		fmt.Print("Error 1: " + err.Error())
		os.Exit(1)
	}
	columnCount64, err := strconv.ParseInt(columnCountStr, 10, 64)
	if err != nil {
		fmt.Print("Error 1.5: " + err.Error())
		os.Exit(1)
	}
	columnCount := int(columnCount64)

	value := convertStrToDate(startDateStr)
	t, _ := time.Parse(layout, value)
	startDate:= strconv.FormatInt(t.Unix(), 10)

	value2 := convertStrToDate(boyEndDateStr)
	t2, _ := time.Parse(layout, value2)
	boyEndDate:= strconv.FormatInt(t2.Unix(), 10)

	value3 := convertStrToDate(moyEndDateStr)
	t3, _ := time.Parse(layout, value3)
	moyEndDate:= strconv.FormatInt(t3.Unix(), 10)

	value4 := convertStrToDate(eoyEndDateStr)
	t4, _ := time.Parse(layout, value4)
	eoyEndDate:= strconv.FormatInt(t4.Unix(), 10)

	enrichData(fileName, startDate, boyEndDate, moyEndDate, eoyEndDate, emailColumnIndex, columnCount)
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

func convertStrToDateStandard(str string) string {
	strArr := strings.Split(str, "/")
	if (len(strArr) > 2) {
		year := strArr[2]

		month := strArr[0]
		if len(month) == 1 {
			month = "0" + month
		}
		day := strArr[1]
		if len(day) == 1 {
			day = "0" + day
		}
		retStr := month + "/" + day + " 06:00:00AM '" + year + " -0600"
		return retStr
	}
	//fmt.Println("Date format must be in YYYY-mm-dd")
	return ""
}

func test(num int) {

}
func enrichData(fileName string, startDate string, boyEndDate string, moyEndDate string, eoyEndDate string,
	emailColumnIndex int64, columnCount int) {

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

		startDateCumulative := "1496275200" // 2017-06-01 (start of valid esuite logs/gis)
		//startDateCumulativeInt := 1496275200

		TotalBadgesEarnedBoy := 0
		level1_badges_earned_countBoy := 0
		level2_badges_earned_countBoy := 0
		level3_badges_earned_countBoy := 0
		level4_badges_earned_countBoy := 0

		TotalBadgesEarnedBoyCumulative := 0
		level1_badges_earned_countBoyCumulative := 0
		level2_badges_earned_countBoyCumulative := 0
		level3_badges_earned_countBoyCumulative := 0
		level4_badges_earned_countBoyCumulative := 0

		TotalBadgesEarnedMoy := 0
		level1_badges_earned_countMoy := 0
		level2_badges_earned_countMoy := 0
		level3_badges_earned_countMoy := 0
		level4_badges_earned_countMoy := 0

		TotalBadgesEarnedMoyCumulative := 0
		level1_badges_earned_countMoyCumulative := 0
		level2_badges_earned_countMoyCumulative := 0
		level3_badges_earned_countMoyCumulative := 0
		level4_badges_earned_countMoyCumulative := 0

		TotalBadgesEarnedEoy := 0
		level1_badges_earned_countEoy := 0
		level2_badges_earned_countEoy := 0
		level3_badges_earned_countEoy := 0
		level4_badges_earned_countEoy := 0

		TotalBadgesEarnedEoyCumulative := 0
		level1_badges_earned_countEoyCumulative := 0
		level2_badges_earned_countEoyCumulative := 0
		level3_badges_earned_countEoyCumulative := 0
		level4_badges_earned_countEoyCumulative := 0

		test(TotalBadgesEarnedMoy)
		test(level1_badges_earned_countMoy)
		test(level2_badges_earned_countMoy)
		test(level3_badges_earned_countMoy)
		test(level4_badges_earned_countMoy)

		test(TotalBadgesEarnedMoyCumulative)
		test(level1_badges_earned_countMoyCumulative)
		test(level2_badges_earned_countMoyCumulative)
		test(level3_badges_earned_countMoyCumulative)
		test(level4_badges_earned_countMoyCumulative)

		test(TotalBadgesEarnedEoy)
		test(level1_badges_earned_countEoy)
		test(level2_badges_earned_countEoy)
		test(level3_badges_earned_countEoy)
		test(level4_badges_earned_countEoy)

		test(TotalBadgesEarnedEoyCumulative)
		test(level1_badges_earned_countEoyCumulative)
		test(level2_badges_earned_countEoyCumulative)
		test(level3_badges_earned_countEoyCumulative)
		test(level4_badges_earned_countEoyCumulative)

		if i > 0 {
			if line[emailColumnIndex] != "" {

				/**
				 * BOY
				 */
				// current year
				badgesItem := searchBadges(db, startDate, boyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedBoy = badgesItem["TotalBadgesEarned"]
				level1_badges_earned_countBoy = badgesItem["level1_badges_earned_count"]
				level2_badges_earned_countBoy = badgesItem["level2_badges_earned_count"]
				level3_badges_earned_countBoy = badgesItem["level3_badges_earned_count"]
				level4_badges_earned_countBoy = badgesItem["level4_badges_earned_count"]

				// cumulative
				badgesItem2 := searchBadges(db, startDateCumulative, boyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedBoyCumulative = badgesItem2["TotalBadgesEarned"]
				level1_badges_earned_countBoyCumulative = badgesItem2["level1_badges_earned_count"]
				level2_badges_earned_countBoyCumulative = badgesItem2["level2_badges_earned_count"]
				level3_badges_earned_countBoyCumulative = badgesItem2["level3_badges_earned_count"]
				level4_badges_earned_countBoyCumulative = badgesItem2["level4_badges_earned_count"]

				/**
				 * MOY
				 */
				// current year
				badgesItem3 := searchBadges(db, startDate, moyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedMoy = badgesItem3["TotalBadgesEarned"]
				level1_badges_earned_countMoy = badgesItem3["level1_badges_earned_count"]
				level2_badges_earned_countMoy = badgesItem3["level2_badges_earned_count"]
				level3_badges_earned_countMoy = badgesItem3["level3_badges_earned_count"]
				level4_badges_earned_countMoy = badgesItem3["level4_badges_earned_count"]

				// cumulative
				badgesItem4 := searchBadges(db, startDateCumulative, moyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedMoyCumulative = badgesItem4["TotalBadgesEarned"]
				level1_badges_earned_countMoyCumulative = badgesItem4["level1_badges_earned_count"]
				level2_badges_earned_countMoyCumulative = badgesItem4["level2_badges_earned_count"]
				level3_badges_earned_countMoyCumulative = badgesItem4["level3_badges_earned_count"]
				level4_badges_earned_countMoyCumulative = badgesItem4["level4_badges_earned_count"]

				/**
				 * EOY
				 */
				// current year
				badgesItem5 := searchBadges(db, startDate, eoyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedEoy = badgesItem5["TotalBadgesEarned"]
				level1_badges_earned_countEoy = badgesItem5["level1_badges_earned_count"]
				level2_badges_earned_countEoy = badgesItem5["level2_badges_earned_count"]
				level3_badges_earned_countEoy = badgesItem5["level3_badges_earned_count"]
				level4_badges_earned_countEoy = badgesItem5["level4_badges_earned_count"]

				// cumulative
				badgesItem6 := searchBadges(db, startDateCumulative, eoyEndDate, line[emailColumnIndex])
				TotalBadgesEarnedEoyCumulative = badgesItem6["TotalBadgesEarned"]
				level1_badges_earned_countEoyCumulative = badgesItem6["level1_badges_earned_count"]
				level2_badges_earned_countEoyCumulative = badgesItem6["level2_badges_earned_count"]
				level3_badges_earned_countEoyCumulative = badgesItem6["level3_badges_earned_count"]
				level4_badges_earned_countEoyCumulative = badgesItem6["level4_badges_earned_count"]
			}
		}
		/**
		 * Query all User Information as well as Coaching Touches - "Current Year"
		 */

		if line[emailColumnIndex] != "" {  // if email IS provided

			if (i == 0) { // first line

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

				// EOY - current year
				records2 = append(records2, "total_badges_earned_eoy_current_year")
				records2 = append(records2, "level1_badges_earned_count_eoy_current_year")
				records2 = append(records2, "level2_badges_earned_count_eoy_current_year")
				records2 = append(records2, "level3_badges_earned_count_eoy_current_year")
				records2 = append(records2, "level4_badges_earned_count_eoy_current_year")
				// EOY - cumulative
				records2 = append(records2, "total_badges_earned_eoy_cumulative")
				records2 = append(records2, "level1_badges_earned_count_eoy_cumulative")
				records2 = append(records2, "level2_badges_earned_count_eoy_cumulative")
				records2 = append(records2, "level3_badges_earned_count_eoy_cumulative")
				records2 = append(records2, "level4_badges_earned_count_eoy_cumulative")


				_ = csvwriter.Write(records2)
				csvwriter.Flush()

			} else { // after first line

				results, err := db.Query("SELECT ifnull(u.email, '') as email, " +
					"ifnull(COUNT(cl.id), 0) as TotalCoachingTouches, ifnull(u.id, 0) as user_id, " +
					"ifnull(u.district_id, 0), ifnull(d.title, '') as 'district', ifnull(u.school_id, 0) as campus_id, " +
					"ifnull(s.title, '') as campus, ifnull(up.first_name, '') as first_name, " +
					"ifnull(up.last_name, '') as last_name, " +
					"ifnull(CONCAT(up.first_name, ' ', up.last_name), '') as 'user', " +
					"ifnull(CASE " +
					"WHEN u.coachee_type = 'teacher' THEN 'Teacher' " +
					"WHEN u.coachee_type = 'librarian' THEN 'Librarian' " +
					"WHEN u.coachee_type = 'coach' THEN 'Coach' " +
					"WHEN u.coachee_type = 'campus-admin' THEN 'Campus Admin' " +
					"WHEN u.coachee_type = 'district-admin' " +
					"THEN 'District Admin' ELSE u.coachee_type END, '') AS user_type, " +
					"ifnull(up.title, '') as title, ifnull(u.last_login, 0), " +
					"ifnull(uc.coach_id, 0) as 'assigned_coach_user_id', " +
					"ifnull(CASE WHEN ( " +
					"SELECT COUNT(cl.id) " +
					"FROM egrowe_coachlog cl " +
					"INNER JOIN egrowe_coachlog_attendee cla " +
					"ON cl.id = cla.egrowe_coachlog_id " +
					"AND cl.start_datetime BETWEEN " + startDate + " AND " + moyEndDate + " " +
					"INNER JOIN egrowe_coachlog_type clt " +
					"ON cl.egrowe_coachlog_type_id = clt.id " +
					"INNER JOIN `user` cu " +
					"ON cl.user_id = cu.id " +
					"WHERE 1 AND cl.is_deleted = 0 AND cl.is_practice = 0 AND cla.present = 1 AND clt.is_coaching = 1 " +
					"AND cla.user_id = u.id AND cu.district_id = 2 " +
					") > 0 THEN 'engage2learn' ELSE 'District' END, '') as coaching_source, " +
					"ifnull(CONCAT(uccp.first_name, ' ', uccp.last_name), 0) as assigned_coach, " +
					"IF(ucc.district_id = 2, 'engage2learn', " +
					"if(ucc.district_id is null, 'Unassigned', 'District')) as coaching_assignment " +
					"FROM `user` u " +
					"LEFT JOIN `user_profile` up " +
					"ON u.id = up.user_id " +
					"LEFT JOIN `district` d " +
					"ON u.district_id = d.id " +
					"LEFT JOIN `school` s " +
					"ON u.school_id = s.id " +
					"LEFT JOIN `user_coach` uc " +
					"ON u.id = uc.coachee_id " +
					"AND uc.is_current = 1 " +
					"LEFT JOIN `user` ucc " +
					"ON uc.coach_id = ucc.id " +
					"LEFT JOIN user_profile uccp " +
					"ON ucc.id = uccp.user_id " +
					"LEFT JOIN `egrowe_coachlog_attendee` cla1 " +
					"ON u.id = cla1.user_id AND cla1.present = 1 " +
					"LEFT JOIN egrowe_coachlog cl " +
					"ON cla1.egrowe_coachlog_id = cl.id " +
					"AND cl.is_practice = 0 " +
					"AND cl.is_deleted = 0 " +
					"LEFT JOIN egrowe_coachlog_type clt " +
					"ON cl.egrowe_coachlog_type_id = clt.id " +
					"AND clt.is_coaching = 1 " +
					"WHERE 1 " +
					"AND u.email = '" + line[emailColumnIndex] + "' ")
				if err != nil {
					fmt.Print(err.Error())
				}
				var userInfo UserInfo
				for results.Next() {
					var records2 []string
					j := 0
					for i := 0; i < columnCount; i++ {
						records2 = append(records2, line[i])
						j = i
					}
					test(j)

					err = results.Scan(&userInfo.email, &userInfo.TotalCoachingTouches, &userInfo.user_id, &userInfo.district_id,
						&userInfo.district, &userInfo.campus_id, &userInfo.campus, &userInfo.first_name, &userInfo.last_name,
						&userInfo.user, &userInfo.user_type, &userInfo.title, &userInfo.last_login,
						&userInfo.assigned_coach_user_id, &userInfo.coaching_source, &userInfo.assigned_coach,
						&userInfo.coaching_assignment)
					if err != nil {  // if NO records found

						fmt.Println("err: ", err)

						for i := j; i < 23; i++ {
							records2 = append(records2, "")
						}
						_ = csvwriter.Write(records2)

					} else {  // if find records

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

						// BOY - Current Year
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedBoy))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countBoy))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countBoy))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countBoy))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countBoy))
						// BOY - Cumulative
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedBoyCumulative))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countBoyCumulative))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countBoyCumulative))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countBoyCumulative))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countBoyCumulative))

						// MOY - Current Year
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedMoy))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countMoy))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countMoy))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countMoy))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countMoy))
						// MOY - Cumulative
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedMoyCumulative))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countMoyCumulative))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countMoyCumulative))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countMoyCumulative))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countMoyCumulative))

						// EOY - Current Year
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedEoy))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countEoy))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countEoy))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countEoy))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countEoy))
						// EOY - Cumulative
						records2 = append(records2, strconv.Itoa(TotalBadgesEarnedEoyCumulative))
						records2 = append(records2, strconv.Itoa(level1_badges_earned_countEoyCumulative))
						records2 = append(records2, strconv.Itoa(level2_badges_earned_countEoyCumulative))
						records2 = append(records2, strconv.Itoa(level3_badges_earned_countEoyCumulative))
						records2 = append(records2, strconv.Itoa(level4_badges_earned_countEoyCumulative))
						_ = csvwriter.Write(records2)
					}
				}
			}

		 } else {  // if no email provided

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
		}
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

func searchBadges(db *sql.DB, startDate string, endDate string, email string) map[string]int {

	retVal := map[string]int{
		"TotalBadgesEarned": 0,
		"level1_badges_earned_count": 0,
		"level2_badges_earned_count": 0,
		"level3_badges_earned_count": 0,
	}

	results3, err3 := db.Query("SELECT ifnull(bu.email, '') " +
		", ifnull(count(eb.id), 0) as TotalBadgesEarned" +
		", ifnull(SUM(" +
		"CASE WHEN b.`level` = 1 THEN 1 ELSE 0 END" +
		"), 0) AS level1_badges_earned_count" +
		", ifnull(SUM(" +
		"CASE WHEN b.`level` = 2 THEN 1 ELSE 0 END" +
		"), 0) AS level2_badges_earned_count" +
		", ifnull(SUM(" +
		"CASE WHEN b.`level` = 3 THEN 1 ELSE 0 END" +
		"), 0) AS level3_badges_earned_count" +
		", ifnull(SUM(" +
		"CASE WHEN b.`level` = 4 THEN 1 ELSE 0 END" +
		"), 0) AS level4_badges_earned_count " +
		"FROM `user` bu " +
		"LEFT JOIN egrowe_user_badge eb " +
		"ON eb.user_id = bu.id " +
		"LEFT JOIN egrowe_badge b " +
		"oN eb.egrowe_badge_id = b.id " +
		"WHERE 1 " +
		"AND b.sub_goal_type = 'level'" +
		"AND eb.created_at BETWEEN " + startDate + " AND " + endDate + " " +
		"AND bu.email = '" + email + "' ")
	if err3 != nil {
		fmt.Println("Error 3: " + err3.Error())
	}
	var userBadges UserBadges
	for results3.Next() {
		err3 = results3.Scan(&userBadges.email, &userBadges.TotalBadgesEarned,
			&userBadges.level1_badges_earned_count, &userBadges.level2_badges_earned_count,
			&userBadges.level3_badges_earned_count, &userBadges.level4_badges_earned_count)
		if err3 != nil {
			fmt.Println("err3: ", err3)
		} else {
			retVal["TotalBadgesEarned"] = userBadges.TotalBadgesEarned
			retVal["level1_badges_earned_count"] = userBadges.level1_badges_earned_count
			retVal["level2_badges_earned_count"] = userBadges.level2_badges_earned_count
			retVal["level3_badges_earned_count"] = userBadges.level3_badges_earned_count
			retVal["level4_badges_earned_count"] = userBadges.level4_badges_earned_count
		}
	}
	return retVal
}