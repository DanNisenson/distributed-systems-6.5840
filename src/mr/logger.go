package mr

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Logger struct {
	defaults []any
}

func NewLogger() *Logger {
	l := Logger{
		defaults: []any{},
	}
	return &l
}

func (l *Logger) Debug(kvs ...any) {
	l.print("DEBUG", kvs...)
}

func (l *Logger) Info(kvs ...any) {
	l.print("INFO", kvs...)
}

func (l *Logger) Error(kvs ...any) {
	l.print("ERROR", kvs...)
}

func (l *Logger) AddDefault(k string, v any) {
	if len(l.defaults) == 0 {
		l.defaults = append(l.defaults, k, v)
	}

	for i := 0; i < len(l.defaults); i += 2 {
		if l.defaults[i] == k {
			l.defaults[i+1] = v
		} else {
			l.defaults = append(l.defaults, k, v)
		}
	}

}

func (l *Logger) print(level string, kvs ...any) {

	if level == "DEBUG" {
		return
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("[%s] [%s]", time.Now().Format("15:04:05.000"), level))

	for i := 0; i < len(kvs); i += 2 {
		buf.WriteString(fmt.Sprintf(" %v=%v", kvs[i], kvs[i+1]))
	}

	for i := 0; i < len(l.defaults); i += 2 {
		buf.WriteString(fmt.Sprintf(" %v=%v", l.defaults[i], l.defaults[i+1]))
	}

	f, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(buf.String() + "\n"); err != nil {
		log.Printf("failed to write log: %v", err)
	}
}
