package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	libraryinputresources "github.com/openshift/multi-operator-manager/pkg/library/libraryinputresources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var inputResources = []libraryinputresources.ExactResourceID{
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "configmaps",
		},
		Namespace: "kube-system",
		Name:      "kube-root-ca.crt",
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "secrets",
		},
		Namespace: "kube-system",
		Name:      "bootstrap-token-abcdef",
	},
	{
		InputResourceTypeIdentifier: libraryinputresources.InputResourceTypeIdentifier{
			Group:    "",
			Version:  "v1",
			Resource: "nodes",
		},
		Name: "kind-control-plane",
	},
}

func main() {
	config, err := parseConfiguration(flag.CommandLine, os.Args[1:])
	if err != nil {
		panic(err)
	}
	logger, err := initCustomZapLogger(config.LogLevel, config.LogEncoder)
	if err != nil {
		panic(err)
	}
	logrLogger := zapr.NewLogger(logger)
	ctrl.SetLogger(logrLogger.WithName("ctrl"))
	klog.SetLogger(logrLogger.WithName("klog"))

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:  scheme,
		Metrics: server.Options{BindAddress: "0"},
	})
	if err != nil {
		os.Exit(1)
	}

	reconciler := &DynamicReconciler{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("dynamic-unstructured"),
		Mapper: mgr.GetRESTMapper(),
		Scheme: scheme,
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}

func initCustomZapLogger(level, encoding string) (*zap.Logger, error) {
	lv := zap.AtomicLevel{}

	i64, err := strconv.ParseInt(level, 10, 8)
	numericLevel := int8(i64)
	if err != nil {
		// not a numeric level, try to unmarshal it as a zapcore.Level ("debug", "info", "warn", "error", "dpanic", "panic", or "fatal")
		err := lv.UnmarshalText([]byte(strings.ToLower(level)))
		if err != nil {
			return nil, err
		}
	} else {
		// numeric level:
		// 1. configure klog if the numeric log level is negative and the absolute value of the negative numeric value represents the klog level.
		// 2. configure the atomic zap level based on the numeric value (5..-9).

		var klogLevel int8 = 0
		if numericLevel < 0 {
			klogLevel = -numericLevel
		}

		klogFlagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		klog.InitFlags(klogFlagSet)
		if err := klogFlagSet.Set("v", strconv.Itoa(int(klogLevel))); err != nil {
			return nil, err
		}

		lv = zap.NewAtomicLevelAt(zapcore.Level(numericLevel))
	}

	enc := strings.ToLower(encoding)
	if enc != "json" && enc != "console" {
		return nil, errors.New("'encoding' parameter can only by either 'json' or 'console'")
	}

	cfg := zap.Config{
		Level:             lv,
		OutputPaths:       []string{"stdout"},
		DisableCaller:     false,
		DisableStacktrace: false,
		Encoding:          enc,
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:  "msg",
			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,
			TimeKey:     "time",
			EncodeTime:  zapcore.ISO8601TimeEncoder,
		},
	}
	return cfg.Build()
}

type Config struct {
	LogLevel   string
	LogEncoder string
}

// ParseConfiguration fills the 'OperatorConfig' from the flags passed to the program
func parseConfiguration(fs *flag.FlagSet, args []string) (Config, error) {
	config := Config{}
	fs.StringVar(&config.LogLevel, "log-level", "info", "Log level. Available values: debug | info | warn | error | dpanic | panic | fatal or a numeric value from -9 to 5, where -9 is the most verbose and 5 is the least verbose.")
	fs.StringVar(&config.LogEncoder, "log-encoder", "json", "Log encoder. Available values: json | console")

	if err := fs.Parse(args); err != nil {
		return Config{}, fmt.Errorf("failed to parse arguments: %w", err)
	}

	return config, nil
}
