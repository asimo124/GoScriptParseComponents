package main

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type TableInfo struct {
	DistrictName                                string `json:"name"`
	Campus_2                                    string `json:"name"`
	StudentLastName                             string `json:"name"`
	StudentFirstName                            string `json:"name"`
	StudentMI                                   string `json:"name"`
	Helper_Column                               string `json:"name"`
	StudentID                                   string `json:"name"`
	Course_Teacher                              string `json:"name"`
	email                                       string `json:"name"`
	Grade                                       string `json:"name"`
	Subject                                     string `json:"name"`
	TestName                                    string `json:"name"`
	BOY_Test_Date                               string `json:"name"`
	BOYTestRITScore                             string `json:"name"`
	TestPercentile                              string `json:"name"`
	AchievementQuintile                         string `json:"name"`
	FallToFallObservedGrowth                    string `json:"name"`
	FallToFallMetProjectedGrowth                string `json:"name"`
	FallToFallConditionalGrowthIndex            string `json:"name"`
	FallToFallConditionalGrowthPercentile       string `json:"name"`
	FallToFallGrowthQuintile                    string `json:"name"`
	ProjectedProficiencyStudy4                  string `json:"name"`
	ProjectedProficiencyLevel4                  string `json:"name"`
	SheetTab                                    string `json:"name"`
	MOY_Test_Date                               string `json:"name"`
	MOY_RIT_Score                               string `json:"name"`
	FalltoWinterMetProjectedGrowth              string `json:"name"`
	EOY_Test_Date                               string `json:"name"`
	EOY_RIT_Score                               string `json:"name"`
	FalltoSpringMetProjectedGrowth              string `json:"name"`
	total_current_year_coaching_conversations   string `json:"name"`
	total_current_year_coaching_logs            string `json:"name"`
	total_cumulative_coaching_conversations     string `json:"name"`
	total_cumulative_coaching_logs              string `json:"name"`
	user_id                                     string `json:"name"`
	district_id                                 string `json:"name"`
	distrrict                                   string `json:"name"`
	campus_id                                   string `json:"name"`
	campus                                      string `json:"name"`
	first_name                                  string `json:"name"`
	last_name                                   string `json:"name"`
	user                                        string `json:"name"`
	user_type                                   string `json:"name"`
	title                                       string `json:"name"`
	last_login                                  string `json:"name"`
	assigned_coach_user_id                      string `json:"name"`
	coaching_source                             string `json:"name"`
	assigned_coach                              string `json:"name"`
	coaching_assignment                         string `json:"name"`
	total_badges_earned_boy_current_year        string `json:"name"`
	level1_badges_earned_count_boy_current_year string `json:"name"`
	level2_badges_earned_count_boy_current_year string `json:"name"`
	level3_badges_earned_count_boy_current_year string `json:"name"`
	level4_badges_earned_count_boy_current_year string `json:"name"`
	total_badges_earned_boy_cumulative          string `json:"name"`
	level1_badges_earned_count_boy_cumulative   string `json:"name"`
	level2_badges_earned_count_boy_cumulative   string `json:"name"`
	level3_badges_earned_count_boy_cumulative   string `json:"name"`
	level4_badges_earned_count_boy_cumulative   string `json:"name"`
	total_badges_earned_moy_current_year        string `json:"name"`
	level1_badges_earned_count_moy_current_year string `json:"name"`
	level2_badges_earned_count_moy_current_year string `json:"name"`
	level3_badges_earned_count_moy_current_year string `json:"name"`
	level4_badges_earned_count_moy_current_year string `json:"name"`
	total_badges_earned_moy_cumulative          string `json:"name"`
	level1_badges_earned_count_moy_cumulative   string `json:"name"`
	level2_badges_earned_count_moy_cumulative   string `json:"name"`
	level3_badges_earned_count_moy_cumulative   string `json:"name"`
	level4_badges_earned_count_moy_cumulative   string `json:"name"`
	total_badges_earned_eoy_current_year        string `json:"name"`
	level1_badges_earned_count_eoy_current_year string `json:"name"`
	level2_badges_earned_count_eoy_current_year string `json:"name"`
	level3_badges_earned_count_eoy_current_year string `json:"name"`
	level4_badges_earned_count_eoy_current_year string `json:"name"`
	total_badges_earned_eoy_cumulative          string `json:"name"`
	level1_badges_earned_count_eoy_cumulative   string `json:"name"`
	level2_badges_earned_count_eoy_cumulative   string `json:"name"`
	level3_badges_earned_count_eoy_cumulative   string `json:"name"`
	level4_badges_earned_count_eoy_cumulative   string `json:"name"`
}

func main() {
	if len(os.Args) < 22 {
		fmt.Println("")
		fmt.Println("Invalid usage:")
		fmt.Println("CORRECT Usage: import_master_data {tableName} {schoolYear} {district} {districtId} " +
			"{fileName} {spreadsheetDate} {boyDateFormattedColumnName} {moyDateFormattedColumnName} " +
			"{eoyDateFormattedColumnName} {studentLidColumnName} {studentColumnNameColumnName} {studentFirstColumnNameColumnName} " +
			"{studentLastColumnNameColumnName} {gradeColumnName} {subjectColumnName} {scoreBoyColumnName} {scoreMoyColumnName} {scoreEoyColumnName} " +
			"{performanceLevelBoyColumnName} {performanceLevelMoyColumnName} {performanceLevelEoyColumnName")
		fmt.Println("")
		fmt.Println("{tableName} - table to import from")
		fmt.Println("{schoolYear} - school year")
		fmt.Println("{district} - district name")
		fmt.Println("{districtId} - district id")
		fmt.Println("{fileName} - file name of the file this came from")
		fmt.Println("{spreadsheetDate} - date of file this came from ")
		fmt.Println("{boyDateFormattedColumnName} - name of column in temp table ")
		fmt.Println("{moyDateFormattedColumnName} - name of column in temp table ")
		fmt.Println("{eoyDateFormattedColumnName} - name of column in temp table ")
		fmt.Println("{studentLidColumnName} - name of column in temp table ")
		fmt.Println("{studentColumnNameColumnName} - name of column in temp table ")
		fmt.Println("{studentFirstColumnNameColumnName}  - name of column in temp table ")
		fmt.Println("{studentLastColumnNameColumnName} - name of column in temp table ")
		fmt.Println("{gradeColumnName} - name of column in temp table ")
		fmt.Println("{subjectColumnName} - name of column in temp table ")
		fmt.Println("{scoreBoyColumnName} - name of column in temp table ")
		fmt.Println("{scoreMoyColumnName} - name of column in temp table ")
		fmt.Println("{scoreEoyColumnName}  - name of column in temp table ")
		fmt.Println("{performanceLevelBoyColumnName} - name of column in temp table ")
		fmt.Println("{performanceLevelMoyColumnName} - name of column in temp table ")
		fmt.Println("{performanceLevelEoyColumnName} - name of column in temp table ")
		fmt.Println("")
		fmt.Println("")
	}

	tableName := os.Args[1]
	schoolYear := os.Args[2]
	district := os.Args[3]
	districtId := os.Args[4]
	fileName := os.Args[5]
	spreadsheetDate := os.Args[6]
	boyDateColumnFormattedName := os.Args[7]
	moyDateColumnFormattedName := os.Args[8]
	eoyDateColumnFormattedName := os.Args[9]
	studentLidName := os.Args[10]
	studentNameColumnName := os.Args[11]
	studentFirstNameColumnName := os.Args[12]
	studentLastNameColumnName := os.Args[13]
	gradeName := os.Args[14]
	subjectName := os.Args[15]
	scoreBoyName := os.Args[16]
	scoreMoyName := os.Args[17]
	scoreEoyName := os.Args[18]
	performanceLevelBoyName := os.Args[19]
	performanceLevelMoyName := os.Args[20]
	performanceLevelEoyName := os.Args[21]

	importMasterData(tableName, schoolYear, district, districtId, fileName, spreadsheetDate, boyDateColumnFormattedName,
		moyDateColumnFormattedName, eoyDateColumnFormattedName, studentLidName, studentNameColumnName,
		studentFirstNameColumnName, studentLastNameColumnName, gradeName, subjectName, scoreBoyName, scoreMoyName,
		scoreEoyName, performanceLevelBoyName, performanceLevelMoyName, performanceLevelEoyName)
}

func convertStrToDate(str string) string {
	strArr := strings.Split(str, "-")
	if len(strArr) > 2 {
		year := strArr[0][len(strArr[0])-2:]
		return strArr[1] + "/" + strArr[2] + " 06:00:00AM '" + year + " -0600"
	}
	fmt.Println("Date format must be in YYYY-mm-dd")
	return ""
}

func convertStrToDateStandard(str string) string {
	strArr := strings.Split(str, "/")
	if len(strArr) > 2 {
		year := strArr[2][len(strArr[2])-2:]

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

func getTimeStampStringFromDateString(dateStr string, useStandard bool) string {
	value := ""
	layout := "01/02 03:04:05PM '06 -0700"
	if useStandard == true {
		value = convertStrToDateStandard(dateStr)
	} else {
		value = convertStrToDate(dateStr)
	}
	t, _ := time.Parse(layout, value)
	date2 := strconv.FormatInt(t.Unix(), 10)
	return date2
}

func test(num int) {

}
func testStr(val2 string) {

}
func testBadge(obj map[string]int) {

}
func testBool(val2 bool) {

}
func convertInt64ToString(num2 int64) string {
	num2Int := int(num2)
	return strconv.Itoa(num2Int)
}

func getFieldInt(v *TableInfo, field string) int {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return int(f.Int())
}

func getFieldString(v *TableInfo, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.String()
}

func convetStrtoTimestampStandard(value string) string {

	value = convertStrToDateStandard(value)
	layout := "01/02 03:04:05PM '06 -0700"
	t, _ := time.Parse(layout, value)
	return strconv.FormatInt(t.Unix(), 10)
}

func convertStandardDateToDate(str string) string {

	strArr := strings.Split(str, "/")
	if len(strArr) > 2 {
		year := strArr[2][len(strArr[2])-2:]
		if len(year) == 2 {
			year = "20" + year
		}

		month := strArr[0]
		if len(month) == 1 {
			month = "0" + month
		}
		day := strArr[1]
		if len(day) == 1 {
			day = "0" + day
		}
		retStr := year + "-" + month + "-" + day
		return retStr
	}
	return ""
}

func importMasterData(tableName string, schoolYear string, district string, districtId string, fileName string,
	spreadsheetDate string, boyDateColumnFormattedName string, moyDateColumnFormattedName string,
	eoyDateColumnFormattedName string, studentLidName string, studentNameColumnName string,
	studentFirstNameColumnName string, studentLastNameColumnName string, gradeName string, subjectName string,
	scoreBoyName string, scoreMoyName string, scoreEoyName string, performanceLevelBoyName string,
	performanceLevelMoyName string, performanceLevelEoyName string) {

	tableName = strings.Replace(tableName, "'", "", -1)
	schoolYear = strings.Replace(schoolYear, "'", "", -1)
	district = strings.Replace(district, "'", "", -1)
	districtId = strings.Replace(districtId, "'", "", -1)
	fileName = strings.Replace(fileName, "'", "", -1)
	spreadsheetDate = strings.Replace(spreadsheetDate, "'", "", -1)

	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print("Error 2: " + err.Error())
	}
	defer db.Close()

	spreadsheetDateFormatted := ""

	/**
	 * Insert into Master Import Table
	 */
	sql := "INSERT INTO domo_master_import " +
		"(filename,           spreadsheet_type, spreadsheet_date,          spreadsheet_date_formatted, " +
		"	created_at,           updated_at,           dashboard_id,  widget_title, import_date, " +
		"import_date_formatted) VALUES " +
		"('" + fileName + "', '',               '" + spreadsheetDate + "', '" + spreadsheetDateFormatted + "'," +
		"	UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW()), 0,             '',           UNIX_TIMESTAMP(NOW())," +
		"NOW())"

	res, err := db.Exec(sql)

	if err != nil {
		panic("err1.5: " + err.Error())
	}

	lastId, err := res.LastInsertId()
	lastIdStr := convertInt64ToString(lastId)

	sql = "SELECT * FROM " + tableName

	results, err := db.Query(sql)

	testStr(boyDateColumnFormattedName)
	testStr(moyDateColumnFormattedName)
	testStr(eoyDateColumnFormattedName)
	testStr(studentLidName)
	testStr(studentNameColumnName)
	testStr(studentFirstNameColumnName)
	testStr(studentLastNameColumnName)
	testStr(gradeName)
	testStr(subjectName)

	testStr(scoreBoyName)
	testStr(scoreMoyName)
	testStr(scoreEoyName)

	testStr(performanceLevelBoyName)
	testStr(performanceLevelMoyName)
	testStr(performanceLevelEoyName)

	// which creates

	if err != nil {
		fmt.Print("err1.3: " + err.Error())
	}
	var tableInfo TableInfo
	for results.Next() {

		err = results.Scan(&tableInfo.email,
			&tableInfo.DistrictName, &tableInfo.Campus_2, &tableInfo.StudentLastName, &tableInfo.StudentFirstName,
			&tableInfo.StudentMI, &tableInfo.Helper_Column, &tableInfo.StudentID, &tableInfo.Course_Teacher,
			&tableInfo.email, &tableInfo.Grade, &tableInfo.Subject, &tableInfo.TestName, &tableInfo.BOY_Test_Date,
			&tableInfo.BOYTestRITScore, &tableInfo.TestPercentile, &tableInfo.AchievementQuintile,
			&tableInfo.FallToFallObservedGrowth, &tableInfo.FallToFallMetProjectedGrowth,
			&tableInfo.FallToFallConditionalGrowthIndex, &tableInfo.FallToFallConditionalGrowthPercentile,
			&tableInfo.FallToFallGrowthQuintile, &tableInfo.ProjectedProficiencyStudy4,
			&tableInfo.ProjectedProficiencyLevel4, &tableInfo.SheetTab, &tableInfo.MOY_Test_Date,
			&tableInfo.MOY_RIT_Score, &tableInfo.FalltoWinterMetProjectedGrowth, &tableInfo.EOY_Test_Date,
			&tableInfo.EOY_RIT_Score, &tableInfo.FalltoSpringMetProjectedGrowth,
			&tableInfo.total_current_year_coaching_conversations, &tableInfo.total_current_year_coaching_logs,
			&tableInfo.total_cumulative_coaching_conversations, &tableInfo.total_cumulative_coaching_logs,
			&tableInfo.user_id, &tableInfo.district_id, &tableInfo.distrrict, &tableInfo.campus_id, &tableInfo.campus,
			&tableInfo.first_name, &tableInfo.last_name, &tableInfo.user, &tableInfo.user_type, &tableInfo.title,
			&tableInfo.last_login, &tableInfo.assigned_coach_user_id, &tableInfo.coaching_source,
			&tableInfo.assigned_coach, &tableInfo.coaching_assignment, &tableInfo.total_badges_earned_boy_current_year,
			&tableInfo.level1_badges_earned_count_boy_current_year,
			&tableInfo.level2_badges_earned_count_boy_current_year,
			&tableInfo.level3_badges_earned_count_boy_current_year,
			&tableInfo.level4_badges_earned_count_boy_current_year, &tableInfo.total_badges_earned_boy_cumulative,
			&tableInfo.level1_badges_earned_count_boy_cumulative, &tableInfo.level2_badges_earned_count_boy_cumulative,
			&tableInfo.level3_badges_earned_count_boy_cumulative, &tableInfo.level4_badges_earned_count_boy_cumulative,
			&tableInfo.total_badges_earned_moy_current_year, &tableInfo.level1_badges_earned_count_moy_current_year,
			&tableInfo.level2_badges_earned_count_moy_current_year,
			&tableInfo.level3_badges_earned_count_moy_current_year,
			&tableInfo.level4_badges_earned_count_moy_current_year, &tableInfo.total_badges_earned_moy_cumulative,
			&tableInfo.level1_badges_earned_count_moy_cumulative, &tableInfo.level2_badges_earned_count_moy_cumulative,
			&tableInfo.level3_badges_earned_count_moy_cumulative, &tableInfo.level4_badges_earned_count_moy_cumulative,
			&tableInfo.total_badges_earned_eoy_current_year, &tableInfo.level1_badges_earned_count_eoy_current_year,
			&tableInfo.level2_badges_earned_count_eoy_current_year,
			&tableInfo.level3_badges_earned_count_eoy_current_year,
			&tableInfo.level4_badges_earned_count_eoy_current_year, &tableInfo.total_badges_earned_eoy_cumulative,
			&tableInfo.level1_badges_earned_count_eoy_cumulative, &tableInfo.level2_badges_earned_count_eoy_cumulative,
			&tableInfo.level3_badges_earned_count_eoy_cumulative, &tableInfo.level4_badges_earned_count_eoy_cumulative)

		tableInfo.DistrictName = strings.Replace(tableInfo.DistrictName, "'", "''", -1)
		tableInfo.Campus_2 = strings.Replace(tableInfo.Campus_2, "'", "''", -1)
		tableInfo.StudentLastName = strings.Replace(tableInfo.StudentLastName, "'", "''", -1)
		tableInfo.StudentFirstName = strings.Replace(tableInfo.StudentFirstName, "'", "''", -1)
		tableInfo.StudentMI = strings.Replace(tableInfo.StudentMI, "'", "''", -1)
		tableInfo.Helper_Column = strings.Replace(tableInfo.Helper_Column, "'", "''", -1)
		tableInfo.StudentID = strings.Replace(tableInfo.StudentID, "'", "''", -1)
		tableInfo.Course_Teacher = strings.Replace(tableInfo.Course_Teacher, "'", "''", -1)
		tableInfo.email = strings.Replace(tableInfo.email, "'", "''", -1)
		tableInfo.Grade = strings.Replace(tableInfo.Grade, "'", "''", -1)
		tableInfo.Subject = strings.Replace(tableInfo.Subject, "'", "''", -1)
		tableInfo.TestName = strings.Replace(tableInfo.TestName, "'", "''", -1)
		tableInfo.BOY_Test_Date = strings.Replace(tableInfo.BOY_Test_Date, "'", "''", -1)
		tableInfo.BOYTestRITScore = strings.Replace(tableInfo.BOYTestRITScore, "'", "''", -1)
		tableInfo.TestPercentile = strings.Replace(tableInfo.TestPercentile, "'", "''", -1)
		tableInfo.AchievementQuintile = strings.Replace(tableInfo.AchievementQuintile, "'", "''", -1)
		tableInfo.FallToFallObservedGrowth = strings.Replace(tableInfo.FallToFallObservedGrowth, "'", "''", -1)
		tableInfo.FallToFallMetProjectedGrowth = strings.Replace(tableInfo.FallToFallMetProjectedGrowth, "'", "''", -1)
		tableInfo.FallToFallConditionalGrowthIndex = strings.Replace(tableInfo.FallToFallConditionalGrowthIndex, "'", "''", -1)
		tableInfo.FallToFallConditionalGrowthPercentile = strings.Replace(tableInfo.FallToFallConditionalGrowthPercentile, "'", "''", -1)
		tableInfo.FallToFallGrowthQuintile = strings.Replace(tableInfo.FallToFallGrowthQuintile, "'", "''", -1)
		tableInfo.ProjectedProficiencyStudy4 = strings.Replace(tableInfo.ProjectedProficiencyStudy4, "'", "''", -1)
		tableInfo.ProjectedProficiencyLevel4 = strings.Replace(tableInfo.ProjectedProficiencyLevel4, "'", "''", -1)
		tableInfo.SheetTab = strings.Replace(tableInfo.SheetTab, "'", "''", -1)
		tableInfo.MOY_Test_Date = strings.Replace(tableInfo.MOY_Test_Date, "'", "''", -1)
		tableInfo.MOY_RIT_Score = strings.Replace(tableInfo.MOY_RIT_Score, "'", "''", -1)
		tableInfo.FalltoWinterMetProjectedGrowth = strings.Replace(tableInfo.FalltoWinterMetProjectedGrowth, "'", "''", -1)
		tableInfo.EOY_Test_Date = strings.Replace(tableInfo.EOY_Test_Date, "'", "''", -1)
		tableInfo.EOY_RIT_Score = strings.Replace(tableInfo.EOY_RIT_Score, "'", "''", -1)
		tableInfo.FalltoSpringMetProjectedGrowth = strings.Replace(tableInfo.FalltoSpringMetProjectedGrowth, "'", "''", -1)
		tableInfo.total_current_year_coaching_conversations = strings.Replace(tableInfo.total_current_year_coaching_conversations, "'", "''", -1)
		tableInfo.total_current_year_coaching_logs = strings.Replace(tableInfo.total_current_year_coaching_logs, "'", "''", -1)
		tableInfo.total_cumulative_coaching_conversations = strings.Replace(tableInfo.total_cumulative_coaching_conversations, "'", "''", -1)
		tableInfo.total_cumulative_coaching_logs = strings.Replace(tableInfo.total_cumulative_coaching_logs, "'", "''", -1)
		tableInfo.user_id = strings.Replace(tableInfo.user_id, "'", "''", -1)
		tableInfo.district_id = strings.Replace(tableInfo.district_id, "'", "''", -1)
		tableInfo.distrrict = strings.Replace(tableInfo.distrrict, "'", "''", -1)
		tableInfo.campus_id = strings.Replace(tableInfo.campus_id, "'", "''", -1)
		tableInfo.campus = strings.Replace(tableInfo.campus, "'", "''", -1)
		tableInfo.first_name = strings.Replace(tableInfo.first_name, "'", "''", -1)
		tableInfo.last_name = strings.Replace(tableInfo.last_name, "'", "''", -1)
		tableInfo.user = strings.Replace(tableInfo.user, "'", "''", -1)
		tableInfo.user_type = strings.Replace(tableInfo.user_type, "'", "''", -1)
		tableInfo.title = strings.Replace(tableInfo.title, "'", "''", -1)
		tableInfo.last_login = strings.Replace(tableInfo.last_login, "'", "''", -1)
		tableInfo.assigned_coach_user_id = strings.Replace(tableInfo.assigned_coach_user_id, "'", "''", -1)
		tableInfo.coaching_source = strings.Replace(tableInfo.coaching_source, "'", "''", -1)
		tableInfo.assigned_coach = strings.Replace(tableInfo.assigned_coach, "'", "''", -1)
		tableInfo.coaching_assignment = strings.Replace(tableInfo.coaching_assignment, "'", "''", -1)
		tableInfo.total_badges_earned_boy_current_year = strings.Replace(tableInfo.total_badges_earned_boy_current_year, "'", "''", -1)
		tableInfo.level1_badges_earned_count_boy_current_year = strings.Replace(tableInfo.level1_badges_earned_count_boy_current_year, "'", "''", -1)
		tableInfo.level2_badges_earned_count_boy_current_year = strings.Replace(tableInfo.level2_badges_earned_count_boy_current_year, "'", "''", -1)
		tableInfo.level3_badges_earned_count_boy_current_year = strings.Replace(tableInfo.level3_badges_earned_count_boy_current_year, "'", "''", -1)
		tableInfo.level4_badges_earned_count_boy_current_year = strings.Replace(tableInfo.level4_badges_earned_count_boy_current_year, "'", "''", -1)
		tableInfo.total_badges_earned_boy_cumulative = strings.Replace(tableInfo.total_badges_earned_boy_cumulative, "'", "''", -1)
		tableInfo.level1_badges_earned_count_boy_cumulative = strings.Replace(tableInfo.level1_badges_earned_count_boy_cumulative, "'", "''", -1)
		tableInfo.level2_badges_earned_count_boy_cumulative = strings.Replace(tableInfo.level2_badges_earned_count_boy_cumulative, "'", "''", -1)
		tableInfo.level3_badges_earned_count_boy_cumulative = strings.Replace(tableInfo.level3_badges_earned_count_boy_cumulative, "'", "''", -1)
		tableInfo.level4_badges_earned_count_boy_cumulative = strings.Replace(tableInfo.level4_badges_earned_count_boy_cumulative, "'", "''", -1)
		tableInfo.total_badges_earned_moy_current_year = strings.Replace(tableInfo.total_badges_earned_moy_current_year, "'", "''", -1)
		tableInfo.level1_badges_earned_count_moy_current_year = strings.Replace(tableInfo.level1_badges_earned_count_moy_current_year, "'", "''", -1)
		tableInfo.level2_badges_earned_count_moy_current_year = strings.Replace(tableInfo.level2_badges_earned_count_moy_current_year, "'", "''", -1)
		tableInfo.level3_badges_earned_count_moy_current_year = strings.Replace(tableInfo.level3_badges_earned_count_moy_current_year, "'", "''", -1)
		tableInfo.level4_badges_earned_count_moy_current_year = strings.Replace(tableInfo.level4_badges_earned_count_moy_current_year, "'", "''", -1)
		tableInfo.total_badges_earned_moy_cumulative = strings.Replace(tableInfo.total_badges_earned_moy_cumulative, "'", "''", -1)
		tableInfo.level1_badges_earned_count_moy_cumulative = strings.Replace(tableInfo.level1_badges_earned_count_moy_cumulative, "'", "''", -1)
		tableInfo.level2_badges_earned_count_moy_cumulative = strings.Replace(tableInfo.level2_badges_earned_count_moy_cumulative, "'", "''", -1)
		tableInfo.level3_badges_earned_count_moy_cumulative = strings.Replace(tableInfo.level3_badges_earned_count_moy_cumulative, "'", "''", -1)
		tableInfo.level4_badges_earned_count_moy_cumulative = strings.Replace(tableInfo.level4_badges_earned_count_moy_cumulative, "'", "''", -1)
		tableInfo.total_badges_earned_eoy_current_year = strings.Replace(tableInfo.total_badges_earned_eoy_current_year, "'", "''", -1)
		tableInfo.level1_badges_earned_count_eoy_current_year = strings.Replace(tableInfo.level1_badges_earned_count_eoy_current_year, "'", "''", -1)
		tableInfo.level2_badges_earned_count_eoy_current_year = strings.Replace(tableInfo.level2_badges_earned_count_eoy_current_year, "'", "''", -1)
		tableInfo.level3_badges_earned_count_eoy_current_year = strings.Replace(tableInfo.level3_badges_earned_count_eoy_current_year, "'", "''", -1)
		tableInfo.level4_badges_earned_count_eoy_current_year = strings.Replace(tableInfo.level4_badges_earned_count_eoy_current_year, "'", "''", -1)
		tableInfo.total_badges_earned_eoy_cumulative = strings.Replace(tableInfo.total_badges_earned_eoy_cumulative, "'", "''", -1)
		tableInfo.level1_badges_earned_count_eoy_cumulative = strings.Replace(tableInfo.level1_badges_earned_count_eoy_cumulative, "'", "''", -1)
		tableInfo.level2_badges_earned_count_eoy_cumulative = strings.Replace(tableInfo.level2_badges_earned_count_eoy_cumulative, "'", "''", -1)
		tableInfo.level3_badges_earned_count_eoy_cumulative = strings.Replace(tableInfo.level3_badges_earned_count_eoy_cumulative, "'", "''", -1)
		tableInfo.level4_badges_earned_count_eoy_cumulative = strings.Replace(tableInfo.level4_badges_earned_count_eoy_cumulative, "'", "''", -1)

		tableInfo2 := &tableInfo

		boyDateFormatted := getFieldString(tableInfo2, boyDateColumnFormattedName)
		boyDate := convetStrtoTimestampStandard(boyDateFormatted)
		boyDateFormatted = convertStandardDateToDate(boyDateFormatted)

		moyDateFormatted := getFieldString(tableInfo2, moyDateColumnFormattedName)
		moyDate := convetStrtoTimestampStandard(moyDateFormatted)
		moyDateFormatted = convertStandardDateToDate(moyDateFormatted)

		eoyDateFormatted := getFieldString(tableInfo2, eoyDateColumnFormattedName)
		eoyDate := convetStrtoTimestampStandard(eoyDateFormatted)
		eoyDateFormatted = convertStandardDateToDate(eoyDateFormatted)

		studentLid := ""
		if studentLidName != "blank" {
			studentLid = getFieldString(tableInfo2, eoyDateColumnFormattedName)
		}
		studentName := ""
		if studentNameColumnName != "blank" {
			studentName = getFieldString(tableInfo2, studentNameColumnName)
		}
		studentFirstName := ""
		if studentFirstNameColumnName != "blank" {
			studentFirstName = getFieldString(tableInfo2, studentFirstNameColumnName)
		}
		studentLastName := ""
		if studentLastNameColumnName != "blank" {
			studentLastName = getFieldString(tableInfo2, studentLastNameColumnName)
		}
		grade := ""
		if gradeName != "blank" {
			grade = getFieldString(tableInfo2, gradeName)
		}
		subject := ""
		if subjectName != "blank" {
			subject = getFieldString(tableInfo2, subjectName)
		}

		scoreBoy := ""
		if scoreBoyName != "blank" {
			scoreBoy = getFieldString(tableInfo2, scoreBoyName)
		}
		scoreMoy := ""
		if scoreMoyName != "blank" {
			scoreMoy = getFieldString(tableInfo2, scoreMoyName)
		}
		scoreEoy := ""
		if scoreEoyName != "blank" {
			scoreEoy = getFieldString(tableInfo2, scoreEoyName)
		}

		performanceLevelBoy := ""
		if performanceLevelBoyName != "blank" {
			performanceLevelBoy = getFieldString(tableInfo2, performanceLevelBoyName)
		}
		performanceLevelMoy := ""
		if performanceLevelMoyName != "blank" {
			performanceLevelMoy = getFieldString(tableInfo2, performanceLevelMoyName)
		}
		performanceLevelEoy := ""
		if performanceLevelEoyName != "blank" {
			performanceLevelEoy = getFieldString(tableInfo2, performanceLevelEoyName)
		}

		/**
		 * Insert into Master Import Table
		 */
		sql := "INSERT INTO domo_master_student_data" +
			"(import_id,        email,                     date,               date_formatted,          " +
			"   date_moy,        date_moy_formatted,         date_eoy,         date_eoy_formatted,      " +
			"school_year,           district, " +
			"district_id,          campus,                     campus_id,  user,              " +
			"	user_id,  					assigned_coach,  " +
			"coaching_assignment, 					coaching_source, " +
			"	user_type, 					total_current_year_coaching_conversations,  " +
			"total_current_year_coaching_logs," +
			"total_cumulative_coaching_conversations,  " +
			"total_cumulative_coaching_logs, " +
			"total_badges_earned_boy_current_year, " +
			"level1_badges_earned_count_boy_current_year, " +
			" level2_badges_earned_count_boy_current_year, " +
			"level3_badges_earned_count_boy_current_year,  " +
			"level4_badges_earned_count_boy_current_year, " +
			"total_badges_earned_boy_cumulative,  " +
			"level1_badges_earned_count_boy_cumulative, " +
			"level2_badges_earned_count_boy_cumulative,  " +
			"level3_badges_earned_count_boy_cumulative, " +
			"level4_badges_earned_count_boy_cumulative,  " +
			"total_badges_earned_moy_current_year, " +
			"level1_badges_earned_count_moy_current_year,  " +
			"level2_badges_earned_count_moy_current_year, " +
			"level3_badges_earned_count_moy_current_year,  " +
			"level4_badges_earned_count_moy_current_year, " +
			"total_badges_earned_moy_cumulative,  " +
			"level1_badges_earned_count_moy_cumulative, " +
			"level2_badges_earned_count_moy_cumulative,  " +
			"level3_badges_earned_count_moy_cumulative, " +
			"level4_badges_earned_count_moy_cumulative, " +
			"total_badges_earned_eoy_current_year, " +
			"level1_badges_earned_count_eoy_current_year,  " +
			"level2_badges_earned_count_eoy_current_year, " +
			"level3_badges_earned_count_eoy_current_year,  " +
			"level4_badges_earned_count_eoy_current_year, " +
			"total_badges_earned_eoy_cumulative,  " +
			"level1_badges_earned_count_eoy_cumulative, " +
			"level2_badges_earned_count_eoy_cumulative,  " +
			"level3_badges_earned_count_eoy_cumulative, " +
			"level4_badges_earned_count_eoy_cumulative, " +
			"original_campus,  original_date,  student_lid,      student_name,       " +
			"	student_first_name,       student_last_name,        grade,         subject,  " +
			"score_boy,        score_moy,           score_eoy, " +
			"	performance_level_boy,           performance_level_moy,           performance_level_eoy, " +
			"created_at,  updated_at) VALUES " +
			"" +
			"(" + lastIdStr + ", '" + tableInfo.email + "', " + boyDate + ", '" + boyDateFormatted + "', " +
			"	" + moyDate + ", '" + moyDateFormatted + "', " + eoyDate + ", '" + eoyDateFormatted + "', " +
			"'" + schoolYear + "', '" + district + "', " +
			" " + districtId + ", '" + tableInfo.campus + "', '" + tableInfo.campus_id + "', '" + tableInfo.user + "'," +
			" " + tableInfo.user_id + ", '" + tableInfo.assigned_coach + "'," +
			" 	'" + tableInfo.coaching_assignment + "', '" + tableInfo.coaching_source + "', " +
			"	'" + tableInfo.user_type + "', " + tableInfo.total_current_year_coaching_conversations + ", " +
			" " + tableInfo.total_current_year_coaching_logs + ", " +
			" " + tableInfo.total_cumulative_coaching_conversations + "," +
			" " + tableInfo.total_cumulative_coaching_logs + ", " +
			" " + tableInfo.total_badges_earned_boy_current_year + ", " +
			" " + tableInfo.level1_badges_earned_count_boy_current_year + ", " +
			" " + tableInfo.level2_badges_earned_count_boy_current_year + ", " +
			" " + tableInfo.level3_badges_earned_count_boy_current_year + ", " +
			" " + tableInfo.level4_badges_earned_count_boy_current_year + ", " +
			" " + tableInfo.total_badges_earned_boy_cumulative + ", " +
			" " + tableInfo.level1_badges_earned_count_boy_cumulative + ", " +
			" " + tableInfo.level2_badges_earned_count_boy_cumulative + ", " +
			" " + tableInfo.level3_badges_earned_count_boy_cumulative + ", " +
			" " + tableInfo.level4_badges_earned_count_boy_cumulative + ", " +
			" " + tableInfo.total_badges_earned_moy_current_year + ", " +
			" " + tableInfo.level1_badges_earned_count_moy_current_year + ", " +
			" " + tableInfo.level2_badges_earned_count_moy_current_year + ", " +
			" " + tableInfo.level3_badges_earned_count_moy_current_year + ", " +
			" " + tableInfo.level4_badges_earned_count_moy_current_year + ", " +
			" " + tableInfo.total_badges_earned_moy_cumulative + ", " +
			" " + tableInfo.level1_badges_earned_count_moy_cumulative + ", " +
			" " + tableInfo.level2_badges_earned_count_moy_cumulative + ", " +
			" " + tableInfo.level3_badges_earned_count_moy_cumulative + ", " +
			" " + tableInfo.level4_badges_earned_count_moy_cumulative + ", " +
			" " + tableInfo.total_badges_earned_eoy_current_year + ", " +
			" " + tableInfo.level1_badges_earned_count_eoy_current_year + ", " +
			" " + tableInfo.level2_badges_earned_count_eoy_current_year + ", " +
			" " + tableInfo.level3_badges_earned_count_eoy_current_year + ", " +
			" " + tableInfo.level4_badges_earned_count_eoy_current_year + ", " +
			" " + tableInfo.total_badges_earned_eoy_cumulative + ", " +
			" " + tableInfo.level1_badges_earned_count_eoy_cumulative + ", " +
			" " + tableInfo.level2_badges_earned_count_eoy_cumulative + ", " +
			" " + tableInfo.level3_badges_earned_count_eoy_cumulative + ", " +
			" " + tableInfo.level4_badges_earned_count_eoy_cumulative + ", " +
			" '',            '',              '" + studentLid + "', '" + studentName + "', " +
			"	'" + studentFirstName + "', '" + studentLastName + "', '" + grade + "', '" + subject + "', " +
			"'" + scoreBoy + "', '" + scoreMoy + "', '" + scoreEoy + "', " +
			"	'" + performanceLevelBoy + "', '" + performanceLevelMoy + "', '" + performanceLevelEoy + "'," +
			"UNIX_TIMESTAMP(NOW()), UNIX_TIMESTAMP(NOW())) "

		_, err := db.Exec(sql)

		if err != nil {
			fmt.Println("sql: ", sql)
			panic("err 1.6: " + err.Error())
		}
	}
	fmt.Println("Script completed")
}
