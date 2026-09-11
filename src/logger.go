package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Logger struct {
	defaults string
}

func NewLogger() *Logger {
	l := Logger{
		defaults: "",
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

func (l *Logger) AddDefault(str string, vals ...any) {
	l.defaults += fmt.Sprintf(str, vals...)
}

func (l *Logger) print(level string, str string, vals ...any) {

	if level == "DEBUG" {
		return
	}

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("[%s] [%s]", time.Now().Format("15:04:05.000"), level))

	buf.WriteString(l.defaults)

	buf.WriteString(fmt.Sprintf(str, vals...))

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
