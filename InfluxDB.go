package influx

import (
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	client "github.com/influxdata/influxdb1-client"
)

type DBUtils struct {
	Ip       string
	Port     int
	Username string
	Password string
	Conn     client.Client
}

var dbConn *DBUtils

var lock sync.Mutex

var db = &DBUtils{
	Ip:       "127.0.0.1",
	Port:     8086,
	Username: "",
	Password: "",
	//Conn:     client.Client{},
}

func GetInfluxDB() *DBUtils {

	lock.Lock()
	defer lock.Unlock()

	return db.Init()
}

func (db *DBUtils) Init() *DBUtils {

	host, err := url.Parse(fmt.Sprintf("http://%s:%d", db.Ip, db.Port))
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
		db.Conn = client.Client{}
	}

	conf := client.Config{
		URL:      *host,
		Username: "guozheng",
		Password: "123456",
	}

	con, err := client.NewClient(conf)
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
		db.Conn = client.Client{}
	}

	db.Conn = *con

	return db
}

func (db DBUtils) Query(sql string, database string, s interface{}) []interface{} {

	q := client.Query{
		Command:  sql,
		Database: database,
	}

	if response, err := db.Conn.Query(q); err == nil && response.Error() == nil {
		rs := response.Results
		if len(rs) <= 0 {
			return nil
		}
		ss := rs[0].Series
		if len(ss) <= 0 {
			return nil
		}

		e0 := make([]interface{}, 0)

		for i := 0; i < len(ss[0].Values); i++ {
			data := ss[0].Values[i]
			bean := reflect.New(reflect.ValueOf(s).Type()).Elem()

			for j := 0; j < len(ss[0].Columns); j++ {

				colName := ss[0].Columns[j]
				name := convertToBeanColName(colName)

				sf, _ := reflect.TypeOf(s).FieldByName(name)

				colType := sf.Type.Name()

				if colType == "string" {
					bean.Field(j).SetString(data[j].(string))
				}
				if colType == "int" {
					bean.Field(j).SetInt(data[j].(int64))
				}

			}
			e0 = append(e0, bean)

		}
		return e0

	}

	return nil
}

func convertToBeanColName(colName string) string {

	if strings.Contains(colName, "_") {
		cs := strings.Split(colName, "_")
		first := cs[1][0:1]
		first = strings.ToUpper(first)
		name := strings.ToUpper(cs[0][0:1]) + cs[0][1:] + first + cs[1][1:]
		return name
	} else {
		return strings.ToUpper(colName[0:1]) + colName[1:]
	}

	return colName
}

func (db DBUtils) Insert(tags map[string]string, fields map[string]interface{}, measurement string, database string) bool {
	p := client.Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Time:        time.Now(),
		Precision:   "s",
	}

	pts := make([]client.Point, 1)
	pts[0] = p

	bps := client.BatchPoints{
		Points:   pts,
		Database: database,
		//RetentionPolicy: "default",
	}

	_, err := db.Conn.Write(bps)
	if err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func (db DBUtils) BatchInsert(tags []map[string]string, fields []map[string]interface{}, measurement string, database string) bool {

	ps := make([]client.Point, len(tags))

	for i := 0; i < len(tags); i++ {
		ps[i] = client.Point{
			Measurement: measurement,
			Tags:        tags[i],
			Fields:      fields[i],
			Time:        time.Now(),
			Precision:   "s",
		}
	}

	bps := client.BatchPoints{
		Points:   ps,
		Database: database,
		//RetentionPolicy: "default",
	}

	_, err := db.Conn.Write(bps)
	if err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func (db DBUtils) delete(sql string, database string) bool {
	q := client.Query{
		Command:  sql,
		Database: database,
	}
	_, err := db.Conn.Query(q)
	if err != nil {
		log.Println(err)
		return false
	}
	return true
}
