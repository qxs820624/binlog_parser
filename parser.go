package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	//LOG_EVENT_TYPES      = 35 //不同版本地 Mysql 可能有所不同 TODO 做成可配置地 5.6
	LOG_EVENT_TYPES      = 27 //不同版本地 Mysql 可能有所不同 TODO 做成可配置地 5.5
	EVENT_HEADER_FIX_LEN = 19 //事件头固定部分大小
)

var (
	BINLOG_MAGIC_NUM = []byte{0xfe, 0x62, 0x69, 0x6e}
)

type EventHeader struct {
	Timestamp    uint32
	TypeCode     uint8
	ServerID     uint32
	EventLength  uint32
	NextPosition uint32
	Flag         uint16
}

type DescEventData struct {
	BinlogVersion   uint16
	ServerVersion   [50]byte
	CreateTimestamp uint32
	HeaderLength    uint8
	PostHeader      [LOG_EVENT_TYPES]byte
}

type QueryLogEventFixedData struct {
	ThreadID          uint32 //发起SQL语句地线程
	Timestamp         uint32 //SQL语句发起时间戳
	DatabaseNameLen   uint8  //SQL语句默认数据库名称地长度
	ErrorCode         uint16 //错误码 include/mysqld_error.h
	StatusVarBlockLen uint16 //状态变量块长度
}

type QueryLogEventVarData struct {
	StatusVariables []byte //状态变量，长度有 QueryLogEventFixedDatastruct.StatusVarBlockLen 决定
	DatabaseName    []byte //数据库名 0字节结尾
	SQLStatement    []byte //SQL语句，log 的总长度有 EventHeader 给出
}

func main() {
	file, err := os.Open("mysql-bin.000001")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buffReader := bufio.NewReader(file)

	parser := &Parser{
		dataSource: buffReader,
	}

	if err := parser.ParseMagicNum(); err != nil {
		panic(err)
	}

	descHeader, descEvent, _ := parser.ParseFDE()

	fmt.Printf("%v\n", descHeader)
	fmt.Printf("%v\n", descEvent)

	header, fixedData, varData, err := parser.ParseQueryLogEvent()
	fmt.Printf("%v\n %v\n dbname %s status %v\n sql %v\n", header, fixedData,
		varData.DatabaseName, varData.StatusVariables, varData.SQLStatement)

}

type Parser struct {
	dataSource *bufio.Reader
	HeaderLen  uint8
}

func (parser *Parser) ParseMagicNum() (err error) {
	data := make([]byte, 4)
	err = binary.Read(parser.dataSource, binary.LittleEndian, data)
	if err == nil && !bytes.Equal(data, BINLOG_MAGIC_NUM) {
		err = errors.New("invalid binlog file")
	}
	return err
}

/*
解析 Format Description Event
http://dev.mysql.com/doc/internals/en/binary-log-versions.html
FDE地头和其他 Event 地头地差别在于： FDE 不可能有 ExtraHeaders ，即长度是固定 19 bytes
*/
func (parser *Parser) ParseFDE() (descEventHeader *EventHeader, descEventData *DescEventData, err error) {
	descEventHeader = &EventHeader{}
	descEventData = &DescEventData{}

	if err = binary.Read(parser.dataSource, binary.LittleEndian, descEventHeader); err != nil {
		return nil, nil, err
	}

	if err = binary.Read(parser.dataSource, binary.LittleEndian, descEventData); err != nil {
		return nil, nil, err
	}
	return descEventHeader, descEventData, err
}

func (parser *Parser) ParseEventHeader() (*EventHeader, error) {
	header := &EventHeader{}
	var err error

	err = binary.Read(parser.dataSource, binary.LittleEndian, header)
	return header, err
}

//获取 extra header 如果有的话
func (parser *Parser) ParseEventExtraHeader() ([]byte, error) {
	if parser.HeaderLen <= EVENT_HEADER_FIX_LEN {
		return nil, nil
	}
	extHeader := make([]byte, parser.HeaderLen-EVENT_HEADER_FIX_LEN)
	err := binary.Read(parser.dataSource, binary.LittleEndian, extHeader)
	return extHeader, err
}

func (parser *Parser) ParseQueryLogEvent() (*EventHeader, *QueryLogEventFixedData, *QueryLogEventVarData, error) {
	var header EventHeader
	var fixedData QueryLogEventFixedData
	var varData QueryLogEventVarData
	var err error
	var size int
	size = binary.Size(header) + binary.Size(fixedData)
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &header); err != nil {
		goto ERR
	}

	if err = binary.Read(parser.dataSource, binary.LittleEndian, &fixedData); err != nil {
		goto ERR
	}

	fmt.Println("variables block len", fixedData.StatusVarBlockLen)
	if fixedData.StatusVarBlockLen > 0 {
		size += int(fixedData.StatusVarBlockLen)
		varData.StatusVariables = make([]byte, fixedData.StatusVarBlockLen)
		if _, err = io.ReadFull(parser.dataSource, varData.StatusVariables); err != nil {
			goto ERR
		}
	}

	fmt.Println("dbname len", fixedData.DatabaseNameLen)
	size += int(fixedData.DatabaseNameLen)
	varData.DatabaseName = make([]byte, fixedData.DatabaseNameLen)
	if _, err = io.ReadFull(parser.dataSource, varData.DatabaseName); err != nil {
		goto ERR
	}

	varData.SQLStatement = make([]byte, header.EventLength-uint32(size))
	if _, err = io.ReadFull(parser.dataSource, varData.SQLStatement); err != nil {
		panic(err)
		goto ERR
	}

	return &header, &fixedData, &varData, err
ERR:
	return nil, nil, nil, err
}
