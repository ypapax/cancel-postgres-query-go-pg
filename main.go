package main

import (
	"context"
	"github.com/labstack/echo"
	"os"
	"time"

	"github.com/go-pg/pg"
	_ "github.com/lib/pq" // postgres driver
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const port = "8001"

var db *pg.DB

func main()  {
	Prepare(logrus.TraceLevel)
	const connStrEnv = "POSTGRESQL_ADDRESS"
	connectionString := os.Getenv(connStrEnv)
	if len(connectionString) == 0 {
		logrus.Fatalf("missing env var '%+v'", connStrEnv)
	}
	var err error
	db, err = ConnectToPostgresTimeout(connectionString, 5 * time.Second, time.Second)
	if err != nil {
		logrus.Fatalf("%+v", err)
	}
	logrus.Infof("connected to db")

	go server()

	cont, cancel := context.WithCancel(context.Background())
	go func() {
		sl := 17 * time.Second
		logrus.Infof("waiting for %s before canceling", sl)
		time.Sleep(sl)
		logrus.Infof("canceling")
		cancel()
	}()
	c, err := queryContext(cont, db)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	logrus.Infof("count: %+v", c)
	select {
	}
}
type Filing struct {
}

func queryContext(cont context.Context, db *pg.DB) (int, error) {
	var c []Filing
	count, err := db.ModelContext(cont, &c).Count()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	logrus.Infof("request is done: %+v", count)
	return count, nil
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

func server(){
	e := echo.New()
	e.GET("/hello", handlerWithContext)
	e.GET("/no", handlerNoContext)
	logrus.Tracef("listening %+v", port)
	e.Logger.Fatal(e.Start(":" + port))

}

func handlerWithContext(c echo.Context) error {
	logrus.Tracef("hello handlerWithContext started")
	count, err := queryContext(c.Request().Context(), db)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	if err := c.JSON(200, map[string]int{"count": count}); err != nil {
		logrus.Errorf("error: %+v", err)
		return err
	}
	return nil
}

func handlerNoContext(c echo.Context) error {
	logrus.Tracef("hello handlerWithContext started")
	count, err := queryContext(context.Background(), db)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	if err := c.JSON(200, map[string]int{"count": count}); err != nil {
		logrus.Errorf("error: %+v", err)
		return err
	}
	return nil
}