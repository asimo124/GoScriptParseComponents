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
		fmt.Println("CORRECT Usage: enrichdata {fileName} {startDateStr} {endDateStr} {emailColumnIndex} {columnCountIndex} {endDateStrMOY} {timeOfYearColumnIndex}")
		fmt.Println("")
		fmt.Println("{fileName} - name of source file, which should be in /Users/alexhawley/Documents/tmp/go_enrich_data")
		fmt.Println("{startDateStr} - start of date range - (YYYY-mm-dd)")
		fmt.Println("{endDateStr} - end of date range - (YYYY-mm-dd)")
		fmt.Println("{emailColumnIndex} - the index of which column (0 based) that contains the email")
		fmt.Println("{columnCountIndex} - the number of valid columns in the source file")
		fmt.Println("{endDateStrMOY} - end of date range for MOY (as compared to BOY)")
		fmt.Println("{timeOfYearColumnIndex} - Index of 'TimeOfYear' column (0 based) that determines BOY or MOY")
		fmt.Println("")
		fmt.Println("")
	}
	endDateMoyStr := "01/31 11:59:00PM '69 -0000"
	timeOfYearColumnIndexStr := "-1"
	layout := "01/02 03:04:05PM '06 -0700"
	fileName := os.Args[1]
	startDateStr := os.Args[2]
	endDateStr := os.Args[3]
	emailColumnIndexStr := os.Args[4]
	columnCountStr := os.Args[5]
	if (len(os.Args) > 6) {
		endDateMoyStr = os.Args[6]
	}
	if (len(os.Args) > 7) {
		timeOfYearColumnIndexStr = os.Args[7]
	}

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

	timeOfYearColumnIndex64, err := strconv.ParseInt(timeOfYearColumnIndexStr, 10, 64)
	if err != nil {
		fmt.Print("Error 1.6: " + err.Error())
		os.Exit(1)
	}
	timeOfYearColumnIndex:= int(timeOfYearColumnIndex64)

	value := convertStrToDate(startDateStr)
	t, _ := time.Parse(layout, value)
	startDate:= strconv.FormatInt(t.Unix(), 10)

	value2 := convertStrToDate(endDateStr)
	t2, _ := time.Parse(layout, value2)
	endDate:= strconv.FormatInt(t2.Unix(), 10)

	value3 := convertStrToDate(endDateMoyStr)
	t3, _ := time.Parse(layout, value3)
	endDateMoy := strconv.FormatInt(t3.Unix(), 10)

	enrichData(fileName, startDate, endDate, emailColumnIndex, columnCount, endDateMoy, timeOfYearColumnIndex)
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
func enrichData(fileName string, startDate string, endDate string, emailColumnIndex int64, columnCount int,
	endDateMoy string, timeOfYearColumnIndex int) {
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

		useEndDate := endDate
		if (timeOfYearColumnIndex > -1) {
			if (line[timeOfYearColumnIndex] == "MOY") {
				useEndDate = endDateMoy
			}
		}

		results3, err3 := db.Query("SELECT bu.email, count(eb.id) as TotalBadgesEarned" +
			", SUM(" +
			"CASE WHEN b.sub_goal_type = 'level' AND b.`level` = 1 THEN 1 ELSE 0 END" +
			") AS level1_badges_earned_count" +
			", SUM(" +
			"CASE WHEN b.sub_goal_type = 'level' AND b.`level` = 2 THEN 1 ELSE 0 END" +
			") AS level2_badges_earned_count" +
			", SUM(" +
			"CASE WHEN b.sub_goal_type = 'level' AND b.`level` = 3 THEN 1 ELSE 0 END" +
			") AS level3_badges_earned_count" +
			", SUM(" +
			"CASE WHEN b.sub_goal_type = 'level' AND b.`level` = 4 THEN 1 ELSE 0 END" +
			") AS level4_badges_earned_count " +
			"FROM `user` bu " +
			"LEFT JOIN egrowe_user_badge eb " +
			"ON eb.user_id = bu.id " +
			"LEFT JOIN egrowe_badge b " +
			"oN eb.egrowe_badge_id = b.id " +
			"WHERE 1 " +
			"AND eb.created_at BETWEEN " + startDate + " AND " + useEndDate + " " +
			"AND bu.email = '" + line[emailColumnIndex] + "' ")
		if err3 != nil {
			fmt.Println("Error 3: " + err3.Error())
		}
		var userBadges UserBadges
		for results3.Next() {
			err3 = results3.Scan(&userBadges.email, &userBadges.TotalBadgesEarned,
				&userBadges.level1_badges_earned_count, &userBadges.level2_badges_earned_count,
				&userBadges.level3_badges_earned_count, &userBadges.level4_badges_earned_count)
			if err3 != nil {

			} else {

			}
		}
		results, err := db.Query("SELECT u.email as email, COUNT(cl.id) as TotalCoachingTouches, " +
			"u.id as user_id, u.district_id, d.title as 'district', u.school_id as campus_id, s.title as campus, " +
			"up.first_name, up.last_name, CONCAT(up.first_name, ' ', up.last_name) as 'user', " +
			"ifnull(CASE " +
			"WHEN u.coachee_type = 'teacher' THEN 'Teacher' " +
			"WHEN u.coachee_type = 'librarian' THEN 'Librarian' " +
			"WHEN u.coachee_type = 'coach' THEN 'Coach' " +
			"WHEN u.coachee_type = 'campus-admin' THEN 'Campus Admin' " +
			"WHEN u.coachee_type = 'district-admin' " +
			"THEN 'District Admin' ELSE u.coachee_type END, '') AS user_type, up.title, ifnull(u.last_login, 0), " +
			"ifnull(uc.coach_id, 0) as 'assigned_coach_user_id', " +
			"ifnull(CASE WHEN ( " +
				"SELECT COUNT(cl.id) " +
				"FROM egrowe_coachlog cl " +
				"INNER JOIN egrowe_coachlog_attendee cla " +
					"ON cl.id = cla.egrowe_coachlog_id " +
					"AND cl.start_datetime BETWEEN " + startDate + " AND " + useEndDate + " " +
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
			for i := 0; i < columnCount - 1; i++ {
				records2 = append(records2, line[i])
				j = i
			}
			test(j)
			if (i == 0) {
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
				records2 = append(records2, "total_badges_earned")
				records2 = append(records2, "leve1l_badges_earned_count")
				records2 = append(records2, "leve2l_badges_earned_count")
				records2 = append(records2, "leve3l_badges_earned_count")
				records2 = append(records2, "leve4l_badges_earned_count")
				_ = csvwriter.Write(records2)
				csvwriter.Flush()
			} else {  // after first line
				var records2 []string
				j := 0
				for i := 0; i < columnCount - 1; i++ {
					records2 = append(records2, line[i])
					j = i
				}
				err = results.Scan(&userInfo.email, &userInfo.TotalCoachingTouches, &userInfo.user_id, &userInfo.district_id,
					&userInfo.district, &userInfo.campus_id, &userInfo.campus, &userInfo.first_name, &userInfo.last_name,
					&userInfo.user, &userInfo.user_type, &userInfo.title, &userInfo.last_login,
					&userInfo.assigned_coach_user_id, &userInfo.coaching_source, &userInfo.assigned_coach,
					&userInfo.coaching_assignment)
				if err != nil {
					for i := j; i < 23; i++ {
						records2 = append(records2, "")
					}
					_ = csvwriter.Write(records2)
				} else {
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
					records2 = append(records2, strconv.Itoa(userBadges.TotalBadgesEarned))
					records2 = append(records2, strconv.Itoa(userBadges.level1_badges_earned_count))
					records2 = append(records2, strconv.Itoa(userBadges.level2_badges_earned_count))
					records2 = append(records2, strconv.Itoa(userBadges.level3_badges_earned_count))
					records2 = append(records2, strconv.Itoa(userBadges.level4_badges_earned_count))
					_ = csvwriter.Write(records2)
				}
			}
		}
	}
	csvwriter.Flush()
	csvFile.Close()
	fmt.Println("Script completed")
}