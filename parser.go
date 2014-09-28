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

import (
	"utils"
)

const (
	//LOG_EVENT_TYPES      = 35 //不同版本地 Mysql 可能有所不同 TODO 做成可配置地 5.6
	LOG_EVENT_TYPES      = 27 //不同版本地 Mysql 可能有所不同 TODO 做成可配置地 5.5
	EVENT_HEADER_FIX_LEN = 19 //事件头固定部分大小
)

var (
	BINLOG_MAGIC_NUM = []byte{0xfe, 0x62, 0x69, 0x6e}
)

const (
	/*
		Every time you update this enum (when you add a type), you have to
		fix Format_description_log_event::Format_description_log_event().
	*/
	UNKNOWN_EVENT      = 0
	START_EVENT_V3     = 1
	QUERY_EVENT        = 2
	STOP_EVENT         = 3
	ROTATE_EVENT       = 4
	INTVAR_EVENT       = 5
	LOAD_EVENT         = 6
	SLAVE_EVENT        = 7
	CREATE_FILE_EVENT  = 8
	APPEND_BLOCK_EVENT = 9
	EXEC_LOAD_EVENT    = 10
	DELETE_FILE_EVENT  = 11
	/*
		NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
		sql_ex allowing multibyte TERMINATED BY etc; both types share the
		same class (Load_log_event)
	*/
	NEW_LOAD_EVENT           = 12
	RAND_EVENT               = 13
	USER_VAR_EVENT           = 14
	FORMAT_DESCRIPTION_EVENT = 15
	XID_EVENT                = 16
	BEGIN_LOAD_QUERY_EVENT   = 17
	EXECUTE_LOAD_QUERY_EVENT = 18

	TABLE_MAP_EVENT = 19

	/*
		These event numbers were used for 5.1.0 to 5.1.15 and are
		therefore obsolete.
	*/
	PRE_GA_WRITE_ROWS_EVENT  = 20
	PRE_GA_UPDATE_ROWS_EVENT = 21
	PRE_GA_DELETE_ROWS_EVENT = 22

	/*
		These event numbers are used from 5.1.16 and forward
	*/
	WRITE_ROWS_EVENT  = 23
	UPDATE_ROWS_EVENT = 24
	DELETE_ROWS_EVENT = 25

	/*
		Something out of the ordinary happened on the master
	*/
	INCIDENT_EVENT = 26

	/*
		Heartbeat event to be send by master at its idle time
		to ensure master's online status to slave
	*/
	HEARTBEAT_LOG_EVENT = 27

	/*
		Add new events here - right above this comment!
		Existing events (except ENUM_END_EVENT) should never change their numbers
	*/

)

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

type EventHeader struct {
	Timestamp    uint32
	TypeCode     uint8
	ServerID     uint32
	EventLength  uint32
	NextPosition uint32
	Flag         uint16
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

//定义 Binlog 日志数据接口
type BinLogEventData interface {
	//TODO more API
}

type DescEventData struct {
	BinlogVersion   uint16
	ServerVersion   [50]byte `field_style:"string"`
	CreateTimestamp uint32
	HeaderLength    uint8
	PostHeader      [LOG_EVENT_TYPES]byte `field_ignore:"ignore"`
}

func (parser *Parser) ParseFDEData() (*DescEventData, error) {
	var data DescEventData
	var err error
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	//FDE 以外的 log event 头 可能有扩展字段，故头的总长度由 FDE 中的HeaderLength 指定
	parser.HeaderLen = data.HeaderLength
	return &data, nil
}

type QueryLogEventData struct {
	FixedPart QueryLogEventFixedData
	VarPart   QueryLogEventVarData
}

type QueryLogEventFixedData struct {
	ThreadID          uint32 //发起SQL语句地线程
	Timestamp         uint32 //SQL语句发起时间戳
	DatabaseNameLen   uint8  //SQL语句默认数据库名称地长度
	ErrorCode         uint16 //错误码 include/mysqld_error.h
	StatusVarBlockLen uint16 //状态变量块长度
}

type QueryLogEventVarData struct {
	StatusVariables []byte `field_style:"string"` //状态变量，长度有 QueryLogEventFixedDatastruct.StatusVarBlockLen 决定
	DatabaseName    []byte `field_style:"string"` //数据库名 0字节结尾
	SQLStatement    []byte `field_style:"string"` //SQL语句，log 的总长度由 EventHeader.EventLength 给出，再减去其他部分长度得到 SQL 语句长度
}

func (parser *Parser) ParseQueryLogEvent(header *EventHeader) (*QueryLogEventData, error) {
	var data QueryLogEventData
	var err error
	var size int
	size = binary.Size(header) + binary.Size(data.FixedPart)

	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data.FixedPart); err != nil {
		goto ERR
	}

	if data.FixedPart.StatusVarBlockLen > 0 {
		size += int(data.FixedPart.StatusVarBlockLen)
		data.VarPart.StatusVariables = make([]byte, data.FixedPart.StatusVarBlockLen)
		if _, err = io.ReadFull(parser.dataSource, data.VarPart.StatusVariables); err != nil {
			goto ERR
		}
	}

	size += int(data.FixedPart.DatabaseNameLen)
	data.VarPart.DatabaseName = make([]byte, data.FixedPart.DatabaseNameLen)
	if _, err = io.ReadFull(parser.dataSource, data.VarPart.DatabaseName); err != nil {
		goto ERR
	}

	data.VarPart.SQLStatement = make([]byte, header.EventLength-uint32(size))
	if _, err = io.ReadFull(parser.dataSource, data.VarPart.SQLStatement); err != nil {
		panic(err)
		goto ERR
	}
	utils.DebugLog("parser query log", size)

	return &data, err
ERR:
	return nil, err
}

type IntvarLogEventData struct {
	Type  uint8  //A value indicating the variable type: LAST_INSERT_ID_EVENT = 1 or INSERT_ID_EVENT = 2.
	Value uint64 //last insert id or auto increment column
}

func (parser *Parser) ParseIntValLogEvent() (*IntvarLogEventData, error) {
	var data IntvarLogEventData
	var err error
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data); err != nil {
		goto ERR
	}
	return &data, nil
ERR:
	return nil, err
}

type XidLogEventData struct {
	XID uint64 //事务ID
}

func (parser *Parser) ParseXIDLogEvent() (*XidLogEventData, error) {
	var data XidLogEventData
	var err error
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data); err != nil {
		goto ERR
	}
	return &data, nil
ERR:
	return nil, err
}

type RotateLogEventData struct {
	FirstLogPos uint64 //下一个文件中，第一个日志的其实位置
	NextLogName []byte `field_style:"string"`
}

func (parser *Parser) ParseRotateLogEvent(header *EventHeader) (*RotateLogEventData, error) {
	var data RotateLogEventData
	var err error
	varPartSize := int(header.EventLength) - binary.Size(data.FirstLogPos)
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data.FirstLogPos); err != nil {
		goto ERR
	}
	data.NextLogName = make([]byte, varPartSize)
	if _, err = parser.dataSource.Read(data.NextLogName); err != nil {
		goto ERR
	}
	return &data, nil
ERR:
	return nil, err
}

type RandLogEventData struct {
	FirstSeed  [8]byte `field_ignore:"ignore"`
	SecondSeed [8]byte `field_ignore:"ignore"`
}

func (parser *Parser) ParserRandLogEvent(header *EventHeader) (*RandLogEventData, error) {
	var (
		data RandLogEventData
		err  error
	)
	if err = binary.Read(parser.dataSource, binary.LittleEndian, &data); err != nil {
		goto ERR
	}
	return &data, nil
ERR:
	return nil, err
}

type TableMapEventData struct {
	TableId         [6]byte
	Reserved        [2]byte `field_ignore:"ignore"`
	DatabaseNameLen uint8
	DatabaseName    []byte `field_style:"string"`
	TableNameLen    uint8
	TableName       []byte `field_style:"string"`
	ColumnNum       int    //表的行数，TODO 不确定 size 是否未 int
	ColumnBytes     []byte //一个 column 一个 byte
}

type UnkonwEventData struct {
	Data []byte `field_ignore:"ignore"`
}

func (parser *Parser) ParserUnkonwLogEvent(header *EventHeader) (BinLogEventData, error) {
	var data UnkonwEventData
	size := int(header.EventLength) - int(parser.HeaderLen)
	data.Data = make([]byte, size)
	err := binary.Read(parser.dataSource, binary.LittleEndian, data.Data)
	return data, err
}

func (parser *Parser) ParseLogEventData(code uint8, header *EventHeader) (BinLogEventData, error) {
	switch code {
	case UNKNOWN_EVENT:
		return nil, errors.New("can not parse unkonw log event type")
	case START_EVENT_V3:
		return nil, errors.New("unsupport event type *yet*")
	case QUERY_EVENT:
		return parser.ParseQueryLogEvent(header)
	case STOP_EVENT:
	case ROTATE_EVENT:
		return parser.ParseRotateLogEvent(header)
	case INTVAR_EVENT:
		return parser.ParseIntValLogEvent()
	case LOAD_EVENT:
	case SLAVE_EVENT:
	case CREATE_FILE_EVENT:
	case APPEND_BLOCK_EVENT:
	case EXEC_LOAD_EVENT:
	case DELETE_FILE_EVENT:
	case NEW_LOAD_EVENT:
	case RAND_EVENT:
		return parser.ParserRandLogEvent(header)
	case USER_VAR_EVENT:
	case FORMAT_DESCRIPTION_EVENT:
		return parser.ParseFDEData()
	case XID_EVENT:
		return parser.ParseXIDLogEvent()
	case BEGIN_LOAD_QUERY_EVENT:
	case EXECUTE_LOAD_QUERY_EVENT:
	case TABLE_MAP_EVENT:
	case PRE_GA_WRITE_ROWS_EVENT:
	case PRE_GA_UPDATE_ROWS_EVENT:
	case PRE_GA_DELETE_ROWS_EVENT:
	case WRITE_ROWS_EVENT:
	case UPDATE_ROWS_EVENT:
	case DELETE_ROWS_EVENT:
	case INCIDENT_EVENT:
	case HEARTBEAT_LOG_EVENT:
	}
	return parser.ParserUnkonwLogEvent(header)
}

func TypeCode2String(code uint8) string {
	switch code {
	case 0:
		return "UNKONW"
	case 1:
		return "START_EVENT_V3"
	case 2:
		return "QUERY_EVENT"
	case 3:
		return "STOP_EVENT"
	case 4:
		return "ROTATE_EVENT"
	case 5:
		return "INTVAR_EVENT"
	case 6:
		return "LOAD_EVENT"
	case 7:
		return "SLAVE_EVENT"
	case 8:
		return "CREATE_FILE_EVENT"
	case 9:
		return "APPEND_BLOCK_EVENT"
	case 10:
		return "EXEC_LOAD_EVENT"
	case 11:
		return "DELETE_FILE_EVENT"
	case 12:
		return "NEW_LOAD_EVENT"
	case 13:
		return "RAND_EVENT"
	case 14:
		return "USER_VAR_EVENT"
	case 15:
		return "FORMAT_DESCRIPTION_EVENT"
	case 16:
		return "XID_EVENT"
	}
	panic("unsupported type code yet")
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

	var header *EventHeader
	//for i := 1; i < 33; i++ {
	for {
		if header, err = parser.ParseEventHeader(); err != nil {
			if err.Error() == "unexpected EOF" {
				fmt.Println("End of Log")
			} else {
				fmt.Println(err)
			}
			return
		}
		fmt.Println("***********")
		fmt.Printf("%s\n", TypeCode2String(header.TypeCode))
		utils.SmartPrint(header)

		data, _ := parser.ParseLogEventData(header.TypeCode, header)
		utils.SmartPrint(data)
		fmt.Println("***********")
	}
}
