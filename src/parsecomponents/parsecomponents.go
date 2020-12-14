package main

import (
	"fmt"
	"strings"
	"log"
	"os"
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

func main() {

	// Remove ConfigFile
	//*/
	e := os.Remove(configFilePath)
	if e != nil {
		log.Fatal(e)
	}
	//*/
	generateComponents()
}

func generateComponents() {

	db, err := sql.Open("mysql", "root:eStud10@/e2lyii_new2")
	if err != nil {
		fmt.Print(err.Error())
	}
	defer db.Close()

	appendToFile("<" + "?" + "xml version=\"1+0\" encoding=\"UTF-8\"" + "?" + ">" + "\n" +
		"<root>\n" +
		"<@array >")

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
		appendToFile("<@hash-array-item >" +
			"<@key>text</@key>" +
			"<@value>" + strings.ReplaceAll(componentsTree.name, "-", "_") + "</@value>")
		appendToFile("<@key>state</@key>" + "<@hash-array-item >" +
			"<@key>opened</@key>" +
			"<@value>true</@value>" +
			"</@hash-array-item>" +
			"<@key>id</@key>" +
			"<@value>" + string(componentsTree.id) + "</@value>" +
			"<@key>children</@key>")

		_genComps(componentsTree)
		appendToFile("</@hash-array-item>")
	}
	appendToFile("</@array>" + "</root>")
}

func _genComps(component ComponentsTree) {

	time.Sleep( 300 * time.Millisecond)

	db, err := sql.Open("mysql", "root:eStud10@/e2lyii_new2")
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