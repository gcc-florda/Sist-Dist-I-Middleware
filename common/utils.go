package common

import "github.com/op/go-logging"

var log = logging.MustGetLogger("log")

func FailOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
