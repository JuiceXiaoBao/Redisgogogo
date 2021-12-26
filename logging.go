package Redisgogogo

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var LOGGER *Logger

func init() {
	LOGGER = NewLogger()
}

type Logger struct {
	*zap.SugaredLogger
}

func NewLogger() *Logger {
	cfg := defaultEncoderConfig
	lvl := zap.NewAtomicLevelAt(zap.InfoLevel)
	return &Logger{
		SugaredLogger: zap.New(zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.Lock(os.Stderr), lvl)).WithOptions(zap.AddCaller(), zap.AddCallerSkip(1)).Sugar(),
	}
}

var defaultEncoderConfig = zapcore.EncoderConfig{
	CallerKey:      "caller",
	StacktraceKey:  "stack",
	LineEnding:     zapcore.DefaultLineEnding,
	TimeKey:        "time",
	MessageKey:     "msg",
	LevelKey:       "level",
	NameKey:        "logger",
	EncodeCaller:   zapcore.ShortCallerEncoder,
	EncodeLevel:    zapcore.CapitalColorLevelEncoder,
	EncodeTime:     MilliSecondTimeEncoder,
	EncodeDuration: zapcore.StringDurationEncoder,
	EncodeName:     zapcore.FullNameEncoder,
}

func MilliSecondTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}
