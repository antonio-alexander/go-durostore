package queue

//ErrorHandler is a function type that allows handling of any
// errors that occur within duroqueue that can't be output
type ErrorHandler func(err error)

type Duroqueue interface {
	//SetErrorHandler can be used to set an error handler to be called
	// when an error occurs
	SetErrorHandler(ErrorHandler)
}
