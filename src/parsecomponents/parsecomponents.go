package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

var selfReferencingIds = []int{146, 328}
var configFilePath = "/home/vagrant/components_tree.xml"

type ComponentsTree struct {
	id   int    `json:"id"`
	name string `json:"name"`
}

type CsvLine struct {
	campus string
	campus_id string
	StudentID string
	user string
	email string
	user_id string
	Grade  string
	NWEAStandard_Grade string
	Subject string
	TestID string
	date string
	PercentCorrect string
	district_id string
	ProjectedProficiencyLevel4 string
}

func main() {

	// Remove ConfigFile
	/*/
	e := os.Remove(configFilePath)
	if e != nil {
		log.Fatal(e)
	}
	//*/
	generateComponents()
}

func generateComponents() {

	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print(err.Error())
	}
	defer db.Close()

	/*/
	appendToFile("<" + "?" + "xml version=\"1+0\" encoding=\"UTF-8\"" + "?" + ">" + "\n" +
		"<root>\n" +
		"<@array >")
	//*/

	// /Users/alexhawley/Documents/tmp/go_enrich_dataDallas_AIM_Map_11.2.21.csv


	filename := "/Users/alexhawley/Documents/tmp/go_enrich_data/Dallas_AIM_Map_11.2.21.csv"

	// Open CSV file
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Read File into a Variable
	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	// Loop through lines & turn into object
	for _, line := range lines {
		data := CsvLine{

			campus: line[0],
			campus_id: line[1],
			StudentID: line[2],
			user: line[3],
			email: line[4],
			user_id: line[5],
			Grade: line[6],
			NWEAStandard_Grade: line[7],
			Subject: line[8],
			TestID: line[9],
			date: line[10],
			PercentCorrect: line[11],
			district_id: line[12],
			ProjectedProficiencyLevel4: line[13],
		}

		fmt.Println(data.campus)
		fmt.Println(data.campus_id)
		fmt.Println(data.StudentID)
		fmt.Println(data.user)
		fmt.Println(data.email)
		fmt.Println(data.user_id)
		fmt.Println(data.Grade)
		fmt.Println(data.NWEAStandard_Grade)
		fmt.Println(data.Subject)
		fmt.Println(data.TestID)
		fmt.Println(data.date)
		fmt.Println(data.PercentCorrect)
		fmt.Println(data.district_id)
		fmt.Println(data.ProjectedProficiencyLevel4)
	}

	//*/
	results, err := db.Query("SELECT dct.id, dct.name " +
		"FROM devdocs_component_tree dct " +
		"LEFT JOIN devdocs_component_tree_parent dctp " +
		"ON dct.id = dctp.component_id " +
		"WHERE dctp.parent_id IS NULL ORDER BY name")

	if err != nil {
		fmt.Print(err.Error())
	}
	for results.Next() {

		var componentsTree ComponentsTree
		err = results.Scan(&componentsTree.id, &componentsTree.name)
		if err != nil {
			fmt.Print(err.Error())
		}
		fmt.Println(componentsTree.name)
	}
	//*/
}

func _genComps(component ComponentsTree) {

	time.Sleep( 300 * time.Millisecond)

	db, err := sql.Open("mysql", "root:eStud10@/e2lyii")
	if err != nil {
		fmt.Print(err.Error())
	}
	defer db.Close()

	parentExists := Find(selfReferencingIds, component.id)
	appendToFile("<@array >")
	if parentExists == false {
		hasComponentsArr := make([]int, 0)

		fmt.Println("component.id: ", component.id)

		results, err := db.Query("SELECT dct.id, dct.name " +
			"FROM devdocs_component_tree dct " +
			"INNER JOIN devdocs_component_tree_parent dctp " +
			"ON dct.id = dctp.component_id " +
			"WHERE dctp.parent_id = ?", component.id)
		if err != nil {
			fmt.Print(err.Error())
		}
		for results.Next() {

			var nextComponent ComponentsTree
			err = results.Scan(&nextComponent.id, &nextComponent.name)
			if err != nil {
				fmt.Print(err.Error())
			}
			componentExists := false
			if len(hasComponentsArr) > 0 {
				componentExists = Find(hasComponentsArr, nextComponent.id)
			}
			if componentExists == false {

				appendToFile("<@hash-array-item >"+
					"<@key>text</@key>" +
					"<@value>" + strings.ReplaceAll(nextComponent.name, "-", "_") + "</@value>" +
					"<@key>state</@key>" +
					"<@hash-array-item >" +
					"<@key>opened</@key>" +
					"<@value>true</@value>" +
					"</@hash-array-item>");
				appendToFile("<@key>id</@key>" +
					"<@value>" + string(nextComponent.id) + "~" + string(component.id) + "</@value>" +
					"<@key>children</@key>")

				_genComps(nextComponent)
				appendToFile("</@hash-array-item>")

				hasComponentsArr = append(hasComponentsArr, nextComponent.id)
			}
		}
	}
	appendToFile("</@array >")
}

// Find value in slice/array
func Find(slice []int, val int) (bool) {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func appendToFile(str string) {
	f, err := os.OpenFile(configFilePath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(str); err != nil {
		log.Println(err)
	}
}