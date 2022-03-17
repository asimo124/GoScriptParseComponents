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

type CsvLine struct {
	District          string
	Campus            string
	Teacher           string
	Email             string
	LID               string
	Student           string
	Grade             string
	Course            string
	Subject           string
	PercentGoal       string
	CA1PercentCorrect string
	CA1GoalMet        string
	ACPPercentCorrect string
	ACPGoalMet        string
}

func main() {
	if (len(os.Args) < 3) {
		fmt.Println("ERROR: Did not provide start date and end date")
	}
	layout := "01/02 03:04:05PM '06 -0700"
	startDateStr := os.Args[1]
	endDateStr := os.Args[2]
	value := convertStrToDate(startDateStr)
	t, _ := time.Parse(layout, value)
	startDate:= strconv.FormatInt(t.Unix(), 10)
	value2 := convertStrToDate(endDateStr)
	t2, _ := time.Parse(layout, value2)
	endDate:= strconv.FormatInt(t2.Unix(), 10)
	enrichData(startDate, endDate)
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

func enrichData(startDate string, endDate string) {
	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print(err.Error())
	}
	defer db.Close()
	filename := "/Users/alexhawley/Documents/tmp/go_enrich_data/Dallas_ISD_AIM_Data_Import_Form_ACP_v2.csv"
	filenameOutput := "/Users/alexhawley/Documents/tmp/go_enrich_data/Dallas_ISD_AIM_Data Import_Form_ACP_output.csv"
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
	records := []string{
		"district",
		"campus",
		"teacher",
		"email2",
		"lid",
		"student",
		"grade",
		"course",
		"subject",
		"ca1_percent_goal",
		"ca1_percent_correct",
		"ca1_goal_met",
		"acp_percent_correct",
		"acp_goal_met",
		"email",
		"total_coaching_touches",
		"user_id",
		"district_id",
		"distrrict",
		"campus_id",
		"campus",
		"first_name",
		"last_name",
		"user",
		"user_type",
		"title",
		"last_login",
		"assigned_coach_user_id",
		"coaching_source",
		"assigned_coach",
		"coaching_assignment",
		"total_badges_earned",
		"leve1l_badges_earned_count",
		"leve2l_badges_earned_count",
		"leve3l_badges_earned_count",
		"leve4l_badges_earned_count",
	}
	_ = csvwriter.Write(records)
	csvwriter.Flush()
	for i, line := range lines {
		data := CsvLine{
			District:          line[0],
			Campus:            line[1],
			Teacher:           line[2],
			Email:             line[3],
			LID:               line[4],
			Student:           line[5],
			Grade:             line[6],
			Course:            line[7],
			Subject:           line[8],
			PercentGoal:       line[9],
			CA1PercentCorrect: line[10],
			CA1GoalMet:        line[11],
			ACPPercentCorrect: line[12],
			ACPGoalMet:        line[13],
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
			"AND eb.created_at BETWEEN " + startDate + " AND " + endDate + " " +
			"AND bu.email = '" + data.Email + "' ")
		if err3 != nil {
			fmt.Println(err3.Error())
		}
		var userBadges UserBadges
		for results3.Next() {
			err3 = results3.Scan(&userBadges.email, &userBadges.TotalBadgesEarned,
				&userBadges.level1_badges_earned_count, &userBadges.level2_badges_earned_count,
				&userBadges.level3_badges_earned_count, &userBadges.level4_badges_earned_count)
			if err3 != nil {
				/*/
				fmt.Println(err3.Error())
				//*/
				//os.Exit(1)
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
					"AND cl.start_datetime BETWEEN " + startDate + " AND " + endDate + " " +
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
			"AND u.email = '" + data.Email + "' ")

		if err != nil {
			fmt.Print(err.Error())
		}
		var userInfo UserInfo
		for results.Next() {
			err = results.Scan(&userInfo.email, &userInfo.TotalCoachingTouches, &userInfo.user_id, &userInfo.district_id,
				&userInfo.district, &userInfo.campus_id, &userInfo.campus, &userInfo.first_name, &userInfo.last_name,
				&userInfo.user, &userInfo.user_type, &userInfo.title, &userInfo.last_login,
				&userInfo.assigned_coach_user_id, &userInfo.coaching_source, &userInfo.assigned_coach,
				&userInfo.coaching_assignment)
			if err != nil {
				/*/
				fmt.Println(err.Error())
				//*/
				//os.Exit(1)
				if (i > 0) {
					records2 := []string{
						data.District,
						data.Campus,
						data.Teacher,
						data.Email,
						data.LID,
						data.Student,
						data.Grade,
						data.Course,
						data.Subject,
						data.PercentGoal,
						data.CA1PercentCorrect,
						data.CA1GoalMet,
						data.ACPPercentCorrect,
						data.ACPGoalMet,
						"",
						"0",
						"0",
						"0",
						"",
						"0",
						"",
						"",
						"",
						"",
						"",
						"",
						"0",
						"0",
						"",
						"",
						"",
						"0",
						"0",
						"0",
						"0",
						"0"}
					_ = csvwriter.Write(records2)
				}
			} else {
				if (i > 0) {
					records2 := []string{
						data.District,
						data.Campus,
						data.Teacher,
						data.Email,
						data.LID,
						data.Student,
						data.Grade,
						data.Course,
						data.Subject,
						data.PercentGoal,
						data.CA1PercentCorrect,
						data.CA1GoalMet,
						data.ACPPercentCorrect,
						data.ACPGoalMet,
						userInfo.email,
						strconv.Itoa(userInfo.TotalCoachingTouches),
						strconv.Itoa(userInfo.user_id),
						strconv.Itoa(userInfo.district_id),
						userInfo.district,
						strconv.Itoa(userInfo.campus_id),
						userInfo.campus,
						userInfo.first_name,
						userInfo.last_name,
						userInfo.user,
						userInfo.user_type,
						userInfo.title,
						strconv.Itoa(userInfo.last_login),
						strconv.Itoa(userInfo.assigned_coach_user_id),
						userInfo.coaching_source,
						userInfo.assigned_coach,
						userInfo.coaching_assignment,
						strconv.Itoa(userBadges.TotalBadgesEarned),
						strconv.Itoa(userBadges.level1_badges_earned_count),
						strconv.Itoa(userBadges.level2_badges_earned_count),
						strconv.Itoa(userBadges.level3_badges_earned_count),
						strconv.Itoa(userBadges.level4_badges_earned_count),
					}
					_ = csvwriter.Write(records2)
				}
			}
		}
	}
	csvwriter.Flush()
	csvFile.Close()
	fmt.Println("Script completed")
}