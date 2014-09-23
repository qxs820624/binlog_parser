package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	//"io"
	"os"
)

const (
	LOG_EVENT_TYPES = 35 //不同版本地 Mysql 可能有所不同 TODO 做成可配置地
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

type QueryLogEventData struct {
	ThreadID          uint32 //发起SQL语句地线程
	Timestamp         uint32 //SQL语句发起时间戳
	DatabaseNameLen   uint8  //SQL语句默认数据库名称地长度
	ErrorCode         uint16 //错误码 include/mysqld_error.h
	StatusVarBlockLen uint16 //状态变量块长度
}

func main() {
	file, err := os.Open("mysql-bin.000001")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buffReader := bufio.NewReader(file)

	if err := parserMagicNum(buffReader); err != nil {
		panic(err)
	}

	var descHeader EventHeader
	var descEvent DescEventData
	parserFDE(buffReader, &descHeader, &descEvent)

	fmt.Printf("%v\n", descHeader)
	fmt.Printf("%v\n", descEvent)
}

func parserMagicNum(reader *bufio.Reader) (err error) {
	data := make([]byte, 4)
	err = binary.Read(reader, binary.LittleEndian, data)
	if err == nil && !bytes.Equal(data, BINLOG_MAGIC_NUM) {
		err = errors.New("invalid binlog file")
	}
	return err
}

type Parser struct {
	FDEHeader  EventHeader
	dataSource *bufio.Reader
	HeaderLen  uint8
}

/*
解析 Format Description Event
http://dev.mysql.com/doc/internals/en/binary-log-versions.html
FDE地头和其他 Event 地头地差别在于： FDE 不可能有 ExtraHeaders ，即长度是固定 19 bytes
*/
func (parser *Parser) ParserFDE() (descEventHeader *EventHeader, descEventData *DescEventData, err error) {
	descEventHeader = &EventHeader{}
	descEventData = &DescEventData{}

	if err = binary.Read(reader, binary.LittleEndian, descEventHeader); err != nil {
		return err
	}

	if err = binary.Read(reader, binary.LittleEndian, descEventData); err != nil {
		return err
	}
	return err
}

func (parser *Parser) PaserEventHeader() (*EventHeader, error) {
	header = &EventHeader{}
	var err error

	err = binary.Reader(reader, binary.LittleEndian, header)
	return header, err
}
