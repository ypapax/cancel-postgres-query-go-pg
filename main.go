package main

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/go-pg/pg"
	_ "github.com/lib/pq" // postgres driver
	"github.com/pkg/errors"
)


func main()  {
	Prepare(logrus.TraceLevel)
	const connStrEnv = "POSTGRESQL_ADDRESS"
	connectionString := os.Getenv(connStrEnv)
	if len(connectionString) == 0 {
		logrus.Fatalf("missing env var '%+v'", connStrEnv)
	}
	db, err := ConnectToPostgresTimeout(connectionString, 5 * time.Second, time.Second)
	if err != nil {
		logrus.Fatalf("%+v", err)
	}
	logrus.Infof("connected to db")
	type Filing struct {
	}
	var c []Filing
	count, err := db.Model(&c).Count()
	if err != nil {
		logrus.Fatalf("%+v", err)
	}
	logrus.Infof("request is done: %+v", count)
}

// ConnectToPostgres connects to postgres instance
func ConnectToPostgres(connectionString string) (*pg.DB, error) {
	if connectionString == "" {
		return nil, errors.Errorf("missing connectionString")
	}
	opt, err := pg.ParseURL(connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres with connection string: "+connectionString)
	}

	db := pg.Connect(opt)
	_, err = db.Exec("SELECT 1")
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	return db, nil
}

// ConnectToPostgres connects to postgres instance
func ConnectToPostgresTimeout(connectionString string, timeout, retry time.Duration) (*pg.DB, error) {
	var (
		connectionError error
		db              *pg.DB
	)
	connected := make(chan bool)
	go func() {
		for {
			db, connectionError = ConnectToPostgres(connectionString)
			if connectionError != nil {
				time.Sleep(retry)
				continue
			}
			connected <- true
			break
		}
	}()
	select {
	case <-time.After(timeout):
		err := errors.Wrapf(connectionError, "timeout %s connecting to db", timeout)
		return nil, err
	case <-connected:
	}
	return db, nil
}

func Prepare(logLevel logrus.Level) {
	customFormatter := logrus.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	}
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.999999999 -0700"
	logrus.SetFormatter(&customFormatter)
	logrus.SetReportCaller(true)
	logrus.SetLevel(logLevel)
}