package kvsrv

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Logger struct {
	// defaults []any
}

func NewLogger() *Logger {
	l := Logger{
		// defaults: []any{},
	}
	return &l
}

func (l *Logger) Debug(str string, vals ...any) {
	l.print("DEBUG", str, vals...)
}

func (l *Logger) Info(str string, vals ...any) {
	l.print("INFO", str, vals...)
}

func (l *Logger) Error(str string, vals ...any) {
	l.print("ERROR", str, vals...)
}

// func (l *Logger) AddDefault(k string, v any) {
// 	if len(l.defaults) == 0 {
// 		l.defaults = append(l.defaults, k, v)
// 	}

// 	for i := 0; i < len(l.defaults); i += 2 {
// 		if l.defaults[i] == k {
// 			l.defaults[i+1] = v
// 		} else {
// 			l.defaults = append(l.defaults, k, v)
// 		}
// 	}

// }

func (l *Logger) print(level string, str string, vals ...any) {

	if level == "DEBUG" {
		return
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("[%s] [%s]", time.Now().Format("15:04:05.000"), level))

	buf.WriteString(fmt.Sprintf(str, vals...))

	// for i := 0; i < len(kvs); i++ {
	// 	buf.WriteString(fmt.Sprintf(" %v", kvs[i]))
	// }

	// for j := 0; j < len(l.defaults); j += 2 {
	// 	buf.WriteString(fmt.Sprintf(" %v=%v", l.defaults[j], l.defaults[j+1]))
	// }

	// fmt.Println(buf.String())

	f, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(buf.String() + "\n"); err != nil {
		log.Printf("failed to write log: %v", err)
	}
}
