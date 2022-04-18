package main

import (
	"encoding/csv"
	"fmt"
	"github.com/brianvoe/gofakeit"
	"log"
	"math/rand"
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
		fmt.Println("CORRECT Usage: anonymizedata {fileName} {districtColumnIndexes} {campusColumnIndexes} {emailColumnIndexes} {columnCountIndex} {firstNameColumnIndexes1} {lastNameColumnIndexes1} {fullNameColumnIndexes1} {firstNameColumnIndexes2} {lastNameColumnIndexes2} {fullNameColumnIndexes2} {firstNameColumnIndexes3} {lastNameColumnIndexes3} {fullNameColumnIndexes3} {firstNameColumnIndexes4} {lastNameColumnIndexes4} {fullNameColumnIndexes4} ")
		fmt.Println("")
		fmt.Println("{fileName} - name of source file, which should be in /Users/alexhawley/Documents/tmp/go_enrich_data")
		fmt.Println("{districtColumnIndexes} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{campusColumnIndexes} - comma-separated list of column indexes that need to have an anonymized campus name")
		fmt.Println("{emailColumnIndexes} - comma-separated list of column indexes that need to have an anonymized email (will use the same one for all columns)")
		fmt.Println("{columnCount} - count of valid columns")
		fmt.Println("{firstNameColumnIndexes1} - comma-separated list of column indexes that need to have an anonymized first name")
		fmt.Println("{middleNameColumnIndexes1} - comma-separated list of column indexes that need to have an anonymized middle name")
		fmt.Println("{lastNameColumnIndexes1} - comma-separated list of column indexes that need to have an anonymized last name")
		fmt.Println("{fullNameColumnIndexes1} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{firstNameColumnIndexes2} - comma-separated list of column indexes that need to have an anonymized first name")
		fmt.Println("{middleNameColumnIndexes2} - comma-separated list of column indexes that need to have an anonymized middle name")
		fmt.Println("{lastNameColumnIndexes2} - comma-separated list of column indexes that need to have an anonymized last name")
		fmt.Println("{fullNameColumnIndexes2} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{firstNameColumnIndexes3} - comma-separated list of column indexes that need to have an anonymized first name")
		fmt.Println("{middleNameColumnIndexes3} - comma-separated list of column indexes that need to have an anonymized middle name")
		fmt.Println("{lastNameColumnIndexes3} - comma-separated list of column indexes that need to have an anonymized last name")
		fmt.Println("{fullNameColumnIndexes3} - comma-separated list of column indexes that need to have an anonymized district name")
		fmt.Println("{firstNameColumnIndexes4} - comma-separated list of column indexes that need to have an anonymized first name")
		fmt.Println("{middleNameColumnIndexes4} - comma-separated list of column indexes that need to have an anonymized middle name")
		fmt.Println("{lastNameColumnIndexes4} - comma-separated list of column indexes that need to have an anonymized last name")
		fmt.Println("{fullNameColumnIndexes4} - comma-separated list of column indexes that need to have an anonymized district name")

		fmt.Println("")
		fmt.Println("")
	}

	fileName := os.Args[1]
	districtColumnIndexes := os.Args[2]
	campusColumnIndexes := os.Args[3]
	emailColumnIndexes := os.Args[4]
	columnCountStr := os.Args[5]
	firstNameColumnIndexes1 := os.Args[6]
	middleNameColumnIndexes1 := os.Args[7]
	lastNameColumnIndexes1 := os.Args[8]
	fullNameColumnIndexes1 := os.Args[9]
	firstNameColumnIndexes2 := ""
	middleNameColumnIndexes2 := ""
	lastNameColumnIndexes2 := ""
	fullNameColumnIndexes2 := ""
	if len(os.Args) > 13 {
		firstNameColumnIndexes2 = os.Args[10]
		middleNameColumnIndexes2 = os.Args[11]
		lastNameColumnIndexes2 = os.Args[12]
		fullNameColumnIndexes2 = os.Args[13]
	}
	firstNameColumnIndexes3 := ""
	middleNameColumnIndexes3 := ""
	lastNameColumnIndexes3 := ""
	fullNameColumnIndexes3 := ""
	if len(os.Args) > 17 {
		firstNameColumnIndexes3 = os.Args[14]
		middleNameColumnIndexes3 = os.Args[15]
		lastNameColumnIndexes3 = os.Args[16]
		fullNameColumnIndexes3 = os.Args[17]
	}
	firstNameColumnIndexes4 := ""
	middleNameColumnIndexes4 := ""
	lastNameColumnIndexes4 := ""
	fullNameColumnIndexes4 := ""
	if len(os.Args) > 21 {
		firstNameColumnIndexes4 = os.Args[18]
		middleNameColumnIndexes4 = os.Args[19]
		lastNameColumnIndexes4 = os.Args[20]
		fullNameColumnIndexes4 = os.Args[21]
	}

	fmt.Println("districtColumnIndexes: ", districtColumnIndexes)
	fmt.Println("campusColumnIndexes: ", campusColumnIndexes)
	fmt.Println("emailColumnIndexes: ", emailColumnIndexes)
	fmt.Println("firstNameColumnIndexes1: ", firstNameColumnIndexes1)
	fmt.Println("middleNameColumnIndexes1: ", middleNameColumnIndexes1)
	fmt.Println("lastNameColumnIndexes1: ", lastNameColumnIndexes1)
	fmt.Println("fullNameColumnIndexes1: ", fullNameColumnIndexes1)
	fmt.Println("firstNameColumnIndexes2: ", firstNameColumnIndexes2)
	fmt.Println("middleNameColumnIndexes1: ", middleNameColumnIndexes2)
	fmt.Println("lastNameColumnIndexes2: ", lastNameColumnIndexes2)
	fmt.Println("fullNameColumnIndexes2: ", fullNameColumnIndexes2)
	fmt.Println("firstNameColumnIndexes3: ", firstNameColumnIndexes3)
	fmt.Println("middleNameColumnIndexes1: ", middleNameColumnIndexes3)
	fmt.Println("lastNameColumnIndexes3: ", lastNameColumnIndexes3)
	fmt.Println("fullNameColumnIndexes3: ", fullNameColumnIndexes3)
	fmt.Println("firstNameColumnIndexes4: ", firstNameColumnIndexes4)
	fmt.Println("middleNameColumnIndexes1: ", middleNameColumnIndexes4)
	fmt.Println("lastNameColumnIndexes4: ", lastNameColumnIndexes4)
	fmt.Println("fullNameColumnIndexes4: ", fullNameColumnIndexes4)

	columnCount64, err := strconv.ParseInt(columnCountStr, 10, 64)
	if err != nil {
		fmt.Print("Error 1.5: " + err.Error())
		os.Exit(1)
	}
	columnCount := int(columnCount64)

	anonymizeData(fileName, districtColumnIndexes, campusColumnIndexes, emailColumnIndexes, columnCount,
		firstNameColumnIndexes1, middleNameColumnIndexes1, lastNameColumnIndexes1, fullNameColumnIndexes1,
		firstNameColumnIndexes2, middleNameColumnIndexes2, lastNameColumnIndexes2, fullNameColumnIndexes2,
		firstNameColumnIndexes3, middleNameColumnIndexes3, lastNameColumnIndexes3, fullNameColumnIndexes3,
		firstNameColumnIndexes4, middleNameColumnIndexes4, lastNameColumnIndexes4, fullNameColumnIndexes4)
}
func test(num int) {

}
func testStr(val2 string) {

}

func arrayContains(arr []int, val2 int) bool {
	for _, eachVal := range arr {
		if val2 == eachVal {
			return true
		}
	}
	return false
}

func anonymizeData(fileName string, districtColumnIndexes string, campusColumnIndexes string, emailColumnIndexes string,
	columnCount int, firstNameColumnIndexes1 string, middleNameColumnIndexes1 string,
	lastNameColumnIndexes1 string, fullNameColumnIndexes1 string,
	firstNameColumnIndexes2 string, middleNameColumnIndexes2 string,
	lastNameColumnIndexes2 string, fullNameColumnIndexes2 string,
	firstNameColumnIndexes3 string, middleNameColumnIndexes3 string,
	lastNameColumnIndexes3 string, fullNameColumnIndexes3 string,
	firstNameColumnIndexes4 string, middleNameColumnIndexes4 string,
	lastNameColumnIndexes4 string, fullNameColumnIndexes4 string) {
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

	/**
	 * Gather up district indexes
	 */
	var districtIndexesArrInt []int // an empty list
	districtIndexesArrStr := strings.Split(districtColumnIndexes, ",")
	for _, districtIndexStr := range districtIndexesArrStr {
		testStr(districtIndexStr)
		districtIndexInt, err := strconv.Atoi(districtIndexStr)
		if err != nil {
			fmt.Println("err 1.1: ", err.Error())
		}
		districtIndexesArrInt = append(districtIndexesArrInt, districtIndexInt)
	}

	/**
	 * Gather up  campus indexes
	 */
	var campusIndexesArrInt []int // an empty list
	campusIndexesArrStr := strings.Split(campusColumnIndexes, ",")
	for _, campusIndexStr := range campusIndexesArrStr {
		testStr(campusIndexStr)
		campusIndexInt, err := strconv.Atoi(campusIndexStr)
		if err != nil {
			fmt.Println("err 1.2: ", err.Error())
		}
		campusIndexesArrInt = append(campusIndexesArrInt, campusIndexInt)
	}

	/**
	 * Gather up  email indexes
	 */
	var emailIndexesArrInt []int // an empty list
	emailIndexesArrStr := strings.Split(emailColumnIndexes, ",")
	for _, emailIndexStr := range emailIndexesArrStr {
		testStr(emailIndexStr)
		emailIndexInt, err := strconv.Atoi(emailIndexStr)
		if err != nil {
			fmt.Println("err 1.3: ", err.Error())
		}
		emailIndexesArrInt = append(emailIndexesArrInt, emailIndexInt)
	}

	/**
	 * Gather up Name Set 1 - First Name
	 */
	var firstName1IndexesArrInt []int // an empty list
	if firstNameColumnIndexes1 != "" {
		firstName1IndexesArrStr := strings.Split(firstNameColumnIndexes1, ",")
		for _, firstName1IndexStr := range firstName1IndexesArrStr {
			testStr(firstName1IndexStr)
			firstName1IndexInt, err := strconv.Atoi(firstName1IndexStr)
			if err != nil {
				fmt.Println("err 1.4: ", err.Error())
			}
			firstName1IndexesArrInt = append(firstName1IndexesArrInt, firstName1IndexInt)
		}
	}

	// middle name 1
	var middleName1IndexesArrInt []int // an empty list
	if middleNameColumnIndexes1 != "" {
		middleName1IndexesArrStr := strings.Split(middleNameColumnIndexes1, ",")
		for _, middleName1IndexStr := range middleName1IndexesArrStr {
			testStr(middleName1IndexStr)
			middleName1IndexInt, err := strconv.Atoi(middleName1IndexStr)
			if err != nil {
				fmt.Println("err 1.4: ", err.Error())
			}
			middleName1IndexesArrInt = append(middleName1IndexesArrInt, middleName1IndexInt)
		}
	}

	// last name 1
	var lastName1IndexesArrInt []int // an empty list
	if lastNameColumnIndexes1 != "" {
		lastName1IndexesArrStr := strings.Split(lastNameColumnIndexes1, ",")
		for _, lastName1IndexStr := range lastName1IndexesArrStr {
			testStr(lastName1IndexStr)
			lastName1IndexInt, err := strconv.Atoi(lastName1IndexStr)
			if err != nil {
				fmt.Println("err 1.5: ", err.Error())
			}
			lastName1IndexesArrInt = append(lastName1IndexesArrInt, lastName1IndexInt)
		}
	}

	// full name 1
	var fullName1IndexesArrInt []int // an empty list
	var fullName1IsReverseArr []bool // an empty list
	if fullNameColumnIndexes1 != "" {
		fullName1IndexesArrStr := strings.Split(fullNameColumnIndexes1, ",")
		for _, fullName1IndexStr := range fullName1IndexesArrStr {
			testStr(fullName1IndexStr)

			if strings.Contains(fullName1IndexStr, "R") {
				fullName1IsReverseArr = append(fullName1IsReverseArr, true)
				fullName1IndexStr = strings.Replace(fullName1IndexStr, "R", "", -1)
			} else {
				fullName1IsReverseArr = append(fullName1IsReverseArr, false)
			}

			fullName1IndexInt, err := strconv.Atoi(fullName1IndexStr)
			if err != nil {
				fmt.Println("err 1.6: ", err.Error())
			}
			fullName1IndexesArrInt = append(fullName1IndexesArrInt, fullName1IndexInt)
		}
	}

	/**
	 * Gather up Name Set 2 -  First Name
	 */
	var firstName2IndexesArrInt []int // an empty list
	if firstNameColumnIndexes2 != "" {
		firstName2IndexesArrStr := strings.Split(firstNameColumnIndexes2, ",")
		for _, firstName2IndexStr := range firstName2IndexesArrStr {
			testStr(firstName2IndexStr)
			firstName2IndexInt, err := strconv.Atoi(firstName2IndexStr)
			if err != nil {
				fmt.Println("err 2.1: ", err.Error())
			}
			firstName2IndexesArrInt = append(firstName2IndexesArrInt, firstName2IndexInt)
		}
	}

	// middle name 2
	var middleName2IndexesArrInt []int // an empty list
	if middleNameColumnIndexes2 != "" {
		middleName2IndexesArrStr := strings.Split(middleNameColumnIndexes2, ",")
		for _, middleName2IndexStr := range middleName2IndexesArrStr {
			testStr(middleName2IndexStr)
			middleName2IndexInt, err := strconv.Atoi(middleName2IndexStr)
			if err != nil {
				fmt.Println("err 2.4: ", err.Error())
			}
			middleName2IndexesArrInt = append(middleName2IndexesArrInt, middleName2IndexInt)
		}
	}

	// last name 2
	var lastName2IndexesArrInt []int // an empty list
	if lastNameColumnIndexes2 != "" {
		lastName2IndexesArrStr := strings.Split(lastNameColumnIndexes2, ",")
		for _, lastName2IndexStr := range lastName2IndexesArrStr {
			testStr(lastName2IndexStr)
			lastName2IndexInt, err := strconv.Atoi(lastName2IndexStr)
			if err != nil {
				fmt.Println("err 2.2: ", err.Error())
			}
			lastName2IndexesArrInt = append(lastName2IndexesArrInt, lastName2IndexInt)
		}
	}

	// full name 2
	var fullName2IndexesArrInt []int // an empty list
	var fullName2IsReverseArr []bool // an empty list
	if fullNameColumnIndexes2 != "" {
		fullName2IndexesArrStr := strings.Split(fullNameColumnIndexes2, ",")
		for _, fullName2IndexStr := range fullName2IndexesArrStr {
			testStr(fullName2IndexStr)

			if strings.Contains(fullName2IndexStr, "R") {
				fullName2IsReverseArr = append(fullName2IsReverseArr, true)
				fullName2IndexStr = strings.Replace(fullName2IndexStr, "R", "", -1)
			} else {
				fullName2IsReverseArr = append(fullName2IsReverseArr, false)
			}

			fullName2IndexInt, err := strconv.Atoi(fullName2IndexStr)
			if err != nil {
				fmt.Println("err 2.3: ", err.Error())
			}
			fullName2IndexesArrInt = append(fullName2IndexesArrInt, fullName2IndexInt)
		}
	}
	/**
	 * Gather up Name Set 3 - First Name
	 */
	var firstName3IndexesArrInt []int // an empty list
	if firstNameColumnIndexes3 != "" {
		firstName3IndexesArrStr := strings.Split(firstNameColumnIndexes3, ",")
		for _, firstName3IndexStr := range firstName3IndexesArrStr {
			testStr(firstName3IndexStr)
			firstName3IndexInt, err := strconv.Atoi(firstName3IndexStr)
			if err != nil {
				fmt.Println("err 3.1: ", err.Error())
			}
			firstName3IndexesArrInt = append(firstName3IndexesArrInt, firstName3IndexInt)
		}
	}

	// middle name 3
	var middleName3IndexesArrInt []int // an empty list
	if middleNameColumnIndexes3 != "" {
		middleName3IndexesArrStr := strings.Split(middleNameColumnIndexes3, ",")
		for _, middleName3IndexStr := range middleName3IndexesArrStr {
			testStr(middleName3IndexStr)
			middleName3IndexInt, err := strconv.Atoi(middleName3IndexStr)
			if err != nil {
				fmt.Println("err 3.4: ", err.Error())
			}
			middleName3IndexesArrInt = append(middleName3IndexesArrInt, middleName3IndexInt)
		}
	}

	// last name 3
	var lastName3IndexesArrInt []int // an empty list
	if lastNameColumnIndexes3 != "" {
		lastName3IndexesArrStr := strings.Split(lastNameColumnIndexes3, ",")
		for _, lastName3IndexStr := range lastName3IndexesArrStr {
			testStr(lastName3IndexStr)
			lastName3IndexInt, err := strconv.Atoi(lastName3IndexStr)
			if err != nil {
				fmt.Println("err 3.2: ", err.Error())
			}
			lastName3IndexesArrInt = append(lastName3IndexesArrInt, lastName3IndexInt)
		}
	}

	// full name 3
	var fullName3IndexesArrInt []int // an empty list
	var fullName3IsReverseArr []bool // an empty list
	if fullNameColumnIndexes3 != "" {
		fullName3IndexesArrStr := strings.Split(fullNameColumnIndexes3, ",")
		for _, fullName3IndexStr := range fullName3IndexesArrStr {
			testStr(fullName3IndexStr)

			if strings.Contains(fullName3IndexStr, "R") {
				fullName3IsReverseArr = append(fullName3IsReverseArr, true)
				fullName3IndexStr = strings.Replace(fullName3IndexStr, "R", "", -1)
			} else {
				fullName3IsReverseArr = append(fullName3IsReverseArr, false)
			}

			fullName3IndexInt, err := strconv.Atoi(fullName3IndexStr)
			if err != nil {
				fmt.Println("err 3.3: ", err.Error())
			}
			fullName3IndexesArrInt = append(fullName3IndexesArrInt, fullName3IndexInt)
		}
	}

	/**
	 * Gather up Name Set 4 - First Name
	 */
	var firstName4IndexesArrInt []int // an empty list
	if firstNameColumnIndexes4 != "" {
		firstName4IndexesArrStr := strings.Split(firstNameColumnIndexes4, ",")
		for _, firstName4IndexStr := range firstName4IndexesArrStr {
			testStr(firstName4IndexStr)
			firstName4IndexInt, err := strconv.Atoi(firstName4IndexStr)
			if err != nil {
				fmt.Println("err 4.1: ", err.Error())
			}
			firstName4IndexesArrInt = append(firstName4IndexesArrInt, firstName4IndexInt)
		}
	}

	// middle name 4
	var middleName4IndexesArrInt []int // an empty list
	if middleNameColumnIndexes4 != "" {
		middleName4IndexesArrStr := strings.Split(middleNameColumnIndexes4, ",")
		for _, middleName4IndexStr := range middleName4IndexesArrStr {
			testStr(middleName4IndexStr)
			middleName4IndexInt, err := strconv.Atoi(middleName4IndexStr)
			if err != nil {
				fmt.Println("err 4.4: ", err.Error())
			}
			middleName4IndexesArrInt = append(middleName4IndexesArrInt, middleName4IndexInt)
		}
	}

	// last name 4
	var lastName4IndexesArrInt []int // an empty list
	if lastNameColumnIndexes4 != "" {
		lastName4IndexesArrStr := strings.Split(lastNameColumnIndexes4, ",")
		for _, lastName4IndexStr := range lastName4IndexesArrStr {
			testStr(lastName4IndexStr)
			lastName4IndexInt, err := strconv.Atoi(lastName4IndexStr)
			if err != nil {
				fmt.Println("err 4.2: ", err.Error())
			}
			lastName4IndexesArrInt = append(lastName4IndexesArrInt, lastName4IndexInt)
		}
	}

	// full name 4
	var fullName4IndexesArrInt []int // an empty list
	var fullName4IsReverseArr []bool // an empty list
	if fullNameColumnIndexes4 != "" {
		fullName4IndexesArrStr := strings.Split(fullNameColumnIndexes4, ",")
		for _, fullName4IndexStr := range fullName4IndexesArrStr {
			testStr(fullName4IndexStr)

			if strings.Contains(fullName4IndexStr, "R") {
				fullName4IsReverseArr = append(fullName4IsReverseArr, true)
				fullName4IndexStr = strings.Replace(fullName4IndexStr, "R", "", -1)
			} else {
				fullName4IsReverseArr = append(fullName4IsReverseArr, false)
			}

			fullName4IndexInt, err := strconv.Atoi(fullName4IndexStr)
			if err != nil {
				fmt.Println("err 4.3: ", err.Error())
			}
			fullName4IndexesArrInt = append(fullName4IndexesArrInt, fullName4IndexInt)
		}
	}

	var districtKeys []string    // an empty list
	var campusKeys []string      // an empty list
	var emailKeys []string       // an empty list
	var firstNameKeys1 []string  // an empty list
	var firstNameKeys2 []string  // an empty list
	var firstNameKeys3 []string  // an empty list
	var firstNameKeys4 []string  // an empty list
	var middleNameKeys1 []string // an empty list
	var middleNameKeys2 []string // an empty list
	var middleNameKeys3 []string // an empty list
	var middleNameKeys4 []string // an empty list
	var lastNameKeys1 []string   // an empty list
	var lastNameKeys2 []string   // an empty list
	var lastNameKeys3 []string   // an empty list
	var lastNameKeys4 []string   // an empty list
	var fullNameKeys1 []string   // an empty list
	var fullNameKeys2 []string   // an empty list
	var fullNameKeys3 []string   // an empty list
	var fullNameKeys4 []string   // an empty list

	for i, line := range lines {

		if i == 0 { // first line

			j := 0
			test(j)
			var records2 []string
			for i := 0; i < columnCount; i++ {

				if arrayContains(districtIndexesArrInt, i) {
					districtKeys = append(districtKeys, line[i])
				}
				if arrayContains(campusIndexesArrInt, i) {
					campusKeys = append(campusKeys, line[i])
				}
				if arrayContains(emailIndexesArrInt, i) {
					emailKeys = append(emailKeys, line[i])
				}

				if arrayContains(firstName1IndexesArrInt, i) {
					firstNameKeys1 = append(firstNameKeys1, line[i])
				}
				if arrayContains(firstName2IndexesArrInt, i) {
					firstNameKeys2 = append(firstNameKeys2, line[i])
				}
				if arrayContains(firstName3IndexesArrInt, i) {
					firstNameKeys3 = append(firstNameKeys4, line[i])
				}
				if arrayContains(firstName4IndexesArrInt, i) {
					firstNameKeys4 = append(firstNameKeys4, line[i])
				}

				if arrayContains(middleName1IndexesArrInt, i) {
					middleNameKeys1 = append(middleNameKeys1, line[i])
				}
				if arrayContains(middleName2IndexesArrInt, i) {
					middleNameKeys2 = append(middleNameKeys2, line[i])
				}
				if arrayContains(middleName3IndexesArrInt, i) {
					middleNameKeys3 = append(middleNameKeys4, line[i])
				}
				if arrayContains(middleName4IndexesArrInt, i) {
					middleNameKeys4 = append(middleNameKeys4, line[i])
				}

				if arrayContains(lastName1IndexesArrInt, i) {
					lastNameKeys1 = append(lastNameKeys1, line[i])
				}
				if arrayContains(lastName2IndexesArrInt, i) {
					lastNameKeys2 = append(lastNameKeys2, line[i])
				}
				if arrayContains(lastName3IndexesArrInt, i) {
					lastNameKeys3 = append(lastNameKeys4, line[i])
				}
				if arrayContains(lastName4IndexesArrInt, i) {
					lastNameKeys4 = append(lastNameKeys4, line[i])
				}

				if arrayContains(fullName1IndexesArrInt, i) {
					fullNameKeys1 = append(fullNameKeys1, line[i])
				}
				if arrayContains(fullName2IndexesArrInt, i) {
					fullNameKeys2 = append(fullNameKeys2, line[i])
				}
				if arrayContains(fullName3IndexesArrInt, i) {
					fullNameKeys3 = append(fullNameKeys4, line[i])
				}
				if arrayContains(fullName4IndexesArrInt, i) {
					fullNameKeys4 = append(fullNameKeys4, line[i])
				}
				records2 = append(records2, line[i])
				j = i
			}

			if len(districtKeys) > 0 {
				records2 = append(records2, districtKeys[0]+"_anon")
			}
			if len(campusKeys) > 0 {
				records2 = append(records2, campusKeys[0]+"_anon")
			}
			if len(emailKeys) > 0 {
				records2 = append(records2, emailKeys[0]+"_anon")
			}
			for _, eachKey := range firstNameKeys1 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range middleNameKeys1 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range lastNameKeys1 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range fullNameKeys1 {
				records2 = append(records2, eachKey+"_anon")
			}

			for _, eachKey := range firstNameKeys2 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range middleNameKeys2 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range lastNameKeys2 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range fullNameKeys2 {
				records2 = append(records2, eachKey+"_anon")
			}

			for _, eachKey := range firstNameKeys3 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range middleNameKeys3 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range lastNameKeys3 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range fullNameKeys3 {
				records2 = append(records2, eachKey+"_anon")
			}

			for _, eachKey := range firstNameKeys4 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range middleNameKeys4 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range lastNameKeys4 {
				records2 = append(records2, eachKey+"_anon")
			}
			for _, eachKey := range fullNameKeys4 {
				records2 = append(records2, eachKey+"_anon")
			}

			_ = csvwriter.Write(records2)
			csvwriter.Flush()

		} else { // after first line

			useCity1 := gofakeit.City()
			useCity2 := gofakeit.City()
			username1 := gofakeit.Username()
			firstName1 := gofakeit.FirstName()
			middleName1 := gofakeit.FirstName()
			lastName1 := gofakeit.LastName()
			fullName1 := firstName1 + " " + lastName1
			fullNameReverse1 := lastName1 + ", " + firstName1
			firstName2 := gofakeit.FirstName()
			middleName2 := gofakeit.FirstName()
			lastName2 := gofakeit.LastName()
			fullName2 := firstName2 + " " + lastName2
			fullNameReverse2 := lastName2 + ", " + firstName2
			firstName3 := gofakeit.FirstName()
			middleName3 := gofakeit.FirstName()
			lastName3 := gofakeit.LastName()
			fullName3 := firstName3 + " " + lastName3
			fullNameReverse3 := lastName3 + ", " + firstName3
			firstName4 := gofakeit.FirstName()
			middleName4 := gofakeit.FirstName()
			lastName4 := gofakeit.LastName()
			fullName4 := firstName4 + " " + lastName4
			fullNameReverse4 := lastName4 + ", " + firstName4

			/**
			 * Anonymize District
			 */
			districtNameAnon := useCity1 + " ISD"

			/**
			 * Anonymize Campus
			 */
			schoolSuffixesArr := [4]string{"High School", "Middle School", "Elementary School", "Academy"}
			min := 0
			max := 3
			schoolSuffixIndex := (rand.Intn(max-min) + min)
			schoolNameAnon := useCity2 + " " + schoolSuffixesArr[schoolSuffixIndex]

			/**
			 * Anonymize Emails
			 */
			useCityCompressed1 := strings.ToLower(strings.Replace(useCity1, " ", "", -1))
			emailSuffix := useCityCompressed1 + "isd.org"
			emailAnon := strings.ToLower(username1) + "@" + emailSuffix

			var records2 []string
			j := 0
			for i := 0; i < columnCount; i++ {
				records2 = append(records2, line[i])
				j = i
			}
			test(j)

			if len(districtKeys) > 0 {
				records2 = append(records2, districtNameAnon)
			}
			if len(campusKeys) > 0 {
				records2 = append(records2, schoolNameAnon)
			}
			if len(emailKeys) > 0 {
				records2 = append(records2, emailAnon)
			}

			for _, eachKey := range firstNameKeys1 {
				testStr(eachKey)
				records2 = append(records2, firstName1)
			}
			for _, eachKey := range middleNameKeys1 {
				testStr(eachKey)
				records2 = append(records2, middleName1)
			}
			for _, eachKey := range lastNameKeys1 {
				testStr(eachKey)
				records2 = append(records2, lastName1)
			}
			for t, eachKey := range fullNameKeys1 {
				testStr(eachKey)
				if fullName1IsReverseArr[t] {
					records2 = append(records2, fullNameReverse1)
				} else {
					records2 = append(records2, fullName1)
				}
			}

			for _, eachKey := range firstNameKeys2 {
				testStr(eachKey)
				records2 = append(records2, firstName2)
			}
			for _, eachKey := range middleNameKeys2 {
				testStr(eachKey)
				records2 = append(records2, middleName2)
			}
			for _, eachKey := range lastNameKeys2 {
				testStr(eachKey)
				records2 = append(records2, lastName2)
			}
			for t, eachKey := range fullNameKeys2 {
				testStr(eachKey)
				if fullName2IsReverseArr[t] {
					records2 = append(records2, fullNameReverse2)
				} else {
					records2 = append(records2, fullName2)
				}
			}

			for _, eachKey := range firstNameKeys3 {
				testStr(eachKey)
				records2 = append(records2, firstName3)
			}
			for _, eachKey := range middleNameKeys3 {
				testStr(eachKey)
				records2 = append(records2, middleName3)
			}
			for _, eachKey := range lastNameKeys3 {
				testStr(eachKey)
				records2 = append(records2, lastName3)
			}
			for t, eachKey := range fullNameKeys3 {
				testStr(eachKey)
				if fullName3IsReverseArr[t] {
					records2 = append(records2, fullNameReverse3)
				} else {
					records2 = append(records2, fullName3)
				}
			}

			for _, eachKey := range firstNameKeys4 {
				testStr(eachKey)
				records2 = append(records2, firstName4)
			}
			for _, eachKey := range lastNameKeys4 {
				testStr(eachKey)
				records2 = append(records2, lastName4)
			}
			for _, eachKey := range middleNameKeys4 {
				testStr(eachKey)
				records2 = append(records2, middleName4)
			}
			for t, eachKey := range fullNameKeys4 {
				testStr(eachKey)
				if fullName4IsReverseArr[t] {
					records2 = append(records2, fullNameReverse4)
				} else {
					records2 = append(records2, fullName4)
				}
			}
			_ = csvwriter.Write(records2)

		}
	}
	csvwriter.Flush()
	csvFile.Close()
	fmt.Println("Script completed")
}
