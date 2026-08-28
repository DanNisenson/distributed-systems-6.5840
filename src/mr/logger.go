package mr

import (
	"fmt"
	"log"
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

func (l *Logger) Info(kvs ...any) {
	l.print("INFO", kvs...)
}

func (l *Logger) Error(kvs ...any) {
	l.print("ERROR", kvs...)
}

func (l *Logger) AddDefault(k string, v any) {
	l.defaults = append(l.defaults, k, v)
}

func (l *Logger) print(level string, kvs ...any) {

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("[%s] [%s]", time.Now().Format("15:04:05.000"), level))

	for i := 0; i < len(kvs); i += 2 {
		buf.WriteString(fmt.Sprintf(" %v=%v", kvs[i], kvs[i+1]))
	}

	for i := 0; i < len(l.defaults); i += 2 {
		buf.WriteString(fmt.Sprintf(" %v=%v", l.defaults[i], l.defaults[i+1]))
	}

	log.Println(buf.String())
}
