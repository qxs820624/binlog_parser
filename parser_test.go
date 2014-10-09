package binlog_parser

import (
	"fmt"
	"testing"
	"time"
	"utils"
)

func TestParseLocalBinLog(t *testing.T) {
	logChan, err := ParseLocalBinLog("mysql-bin.000001", false)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		select {
		case log := <-logChan:
			if log == nil {
				fmt.Println("end of log")
				return
			}
			fmt.Println("***********")
			fmt.Printf("%s\n", TypeCode2String(log.Header.TypeCode))
			utils.SmartPrint(log.Header)
			utils.SmartPrint(log.Data)
			fmt.Println("***********")
		case <-time.After(5 * time.Second):
			fmt.Println("timeout")
			return
		}
	}
}

func TestGetSQLStatement(t *testing.T) {
	logChan, err := ParseLocalBinLog("mysql-bin.000001", false)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		select {
		case log := <-logChan:
			if log == nil {
				fmt.Println("end of log")
				return
			} else {
				stmt, err := log.GetSQLStatement()
				start, end := log.GetPosition()
				if err == nil && stmt != "BEGIN" {
					fmt.Print(log.GetTimestamp(), "\t")
					fmt.Print("start: ", start, "\t\tend:", end, "\t\t")
					fmt.Printf("%v\n", stmt)
				}
			}
		case <-time.After(5 * time.Second):
			fmt.Println("timeout")
			return
		}
	}
}
