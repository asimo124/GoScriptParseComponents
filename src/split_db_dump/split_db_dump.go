package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)
import _ "github.com/go-sql-driver/mysql"

func main() {
	if len(os.Args) < 1 {
		fmt.Println("")
		fmt.Println("Invalid usage:")
		fmt.Println("CORRECT Usage: split_db_dump {test1}")
		fmt.Println("")
		fmt.Println("{test1} - test1")
		fmt.Println("")
		fmt.Println("")
	}

	//test1 := os.Args[1]
	//testStr(test1)

	/*/
	//file, err := os.Open("/Users/alexhawley/Documents/tmp/qa_test_how_to_dump_qa_20220627.sql")
	file, err := os.Open("/Users/alexhawley/Documents/tmp/qa_test_how_to.sql")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	//var lines []string
	scanner := bufio.NewScanner(file)
	const maxCapacity int = 100000000 // your required line length
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	linesContent := ""
	for scanner.Scan() {
		char := scanner.Text()
		if char == "\n" || char == "\r" {
			if char == "\n" {
				fmt.Println("is line break")
			} else if char == "\r" {
				fmt.Println("is carrage return")
			}
		}
		linesContent += scanner.Text()
	}
	//fmt.Println("linesContent: ", linesContent)
	//os.Exit(1)
	tablesContent := strings.Split(linesContent, "DROP TABLE IF EXISTS")
	//fmt.Println(len(tablesContent))
	fmt.Println(tablesContent[0])
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	//*/

	//var lines []string
	//logFile := "/Users/alexhawley/Documents/tmp/qa_test_how_to.sql"
	logFile := "/Users/alexhawley/Documents/tmp/qa_test_how_to_dump_qa_20220627.sql"

	f, err := os.OpenFile(logFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	defer f.Close()

	lineContent := ""
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("read file line error: %v", err)
			return
		}
		//lines = append(lines, line)
		lineContent += line
	}

	lineContentArr := strings.Split(lineContent, "DROP TABLE IF EXISTS")

	//fmt.Println("lineContentArr lengh: ", len(lineContentArr))

	var tablesArr []string
	for i, lineContent := range lineContentArr {

		fmt.Println("i: ", i)
		testStr(lineContent)
		if i < len(lineContentArr)-1 {
			//fmt.Println("lineContent: ", lineContent + "DROP TABLE IF EXISTS")
			tablesArr = append(tablesArr, lineContent+"DROP TABLE IF EXISTS")
		} else {
			//fmt.Println("lineContent: ", lineContent)
			tablesArr = append(tablesArr, lineContent)
		}
		/*/
		fmt.Println("__________________________________________________")
		fmt.Println("")
		fmt.Println("")
		//*/
	}

	fmt.Println("tablesArr length: ", len(tablesArr))

	/*/
	fmt.Println("tablesArr[0]: ", tablesArr[0])
	fmt.Println("_________--------_________--------")
	fmt.Println("tablesArr[1]: ", tablesArr[1])
	//*/

	//linesContent := strings.Join(strings.Fields(fmt.Sprint(lines)), "\n")

	//println("linesContent: ", linesContent)

}

func test(num int) {

}
func testStr(val2 string) {

}
func testBadge(obj map[string]int) {

}
