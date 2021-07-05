package utils

import "log"

/*
CatchError tests if the given error is nil. If it's not nil, it panics with the error message
 */
func CatchError(err error)  {
	if err != nil {
		log.Fatalln(err.Error())
	}
}
