// Command httppooler is the whole program: genkey prints a shared secret, run is the daemon.
//
// The daemon never exits because of its conf. An invalid or roleless conf at startup is logged and the
// daemon idles; SIGHUP re-reads the file, applies a valid conf by replacing this process with a fresh
// one, and keeps the running conf when the new one does not parse. SIGUSR1 logs peers, slots and queues.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/henu/httppooler/internal/conf"
	"github.com/henu/httppooler/internal/peer"
)

// defaultConf is where the install puts the conf and where the unit file looks for it.
const defaultConf = "/etc/httppooler/httppooler.conf"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "genkey":
		err = genkey()
	case "run":
		err = run(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "httppooler: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `httppooler pools HTTP services between trusted peers.

  httppooler genkey              print a fresh shared secret
  httppooler run [-conf path]    run the daemon, default %s
`, defaultConf)
}

// genkey prints a shared secret: the bytes a Noise pre-shared key has, written as hex. The install
// script puts one in the conf it writes, and every client uses the server's.
func genkey() error {
	key := make([]byte, conf.SecretSize)
	if _, err := rand.Read(key); err != nil {
		return fmt.Errorf("reading random bytes: %w", err)
	}
	fmt.Println(hex.EncodeToString(key))
	return nil
}

// runner is whichever role the conf asked for. Both do the same two things to the daemon.
type runner interface {
	Run(ctx context.Context) error
	LogState()
}

// run is the daemon.
func run(args []string) error {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	path := flags.String("conf", defaultConf, "path to the configuration file")
	verbose := flags.Bool("v", false, "log every message that is normally too much detail")
	if err := flags.Parse(args); err != nil {
		return err
	}

	log := logger(*verbose)

	// Signals are caught before anything is started, so a reload that arrives during startup is not
	// the one that kills the daemon.
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	role := start(ctx, *path, log)

	for signal := range signals {
		switch signal {
		case syscall.SIGHUP:
			reload(*path, log)
		case syscall.SIGUSR1:
			if role == nil {
				log.Info("idle", "conf", *path)
				continue
			}
			role.LogState()
		case syscall.SIGINT, syscall.SIGTERM:
			log.Info("stopping", "signal", signal.String())
			stop()
			return nil
		}
	}
	return nil
}

// start reads the conf and puts the role it asks for to work. A conf that does not parse, or one that
// has not been finished yet, leaves the daemon idling: it keeps running, and a reload is all it takes.
func start(ctx context.Context, path string, log *slog.Logger) runner {
	cfg, err := conf.Load(path)
	if err != nil {
		log.Error("conf refused, idling until it is reloaded", "conf", path, "error", err)
		return nil
	}

	if cfg.Role == conf.RoleNone {
		log.Warn("conf has no role, idling until it is reloaded", "conf", path)
		return nil
	}

	var role runner
	switch cfg.Role {
	case conf.RoleServer:
		role = peer.NewServer(cfg, peer.Options{}, log)
	case conf.RoleClient:
		role = peer.NewClient(cfg, peer.Options{}, log)
	}

	log.Info("starting", "role", cfg.Role.String(), "name", cfg.Name, "conf", path)
	go func() {
		if err := role.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("stopped", "role", cfg.Role.String(), "error", err)
		}
	}()
	return role
}

// reload re-reads the conf. A conf that parses is applied by replacing this process with a fresh one,
// which is the only way to be sure nothing of the old one is left; requests in flight are dropped. A
// conf that does not parse changes nothing at all.
func reload(path string, log *slog.Logger) {
	if _, err := conf.Load(path); err != nil {
		log.Error("conf refused, keeping the running one", "conf", path, "error", err)
		return
	}

	binary, err := os.Executable()
	if err != nil {
		log.Error("conf is fine but this process cannot find itself to restart", "error", err)
		return
	}

	log.Info("conf reloaded, restarting", "conf", path)
	if err := syscall.Exec(binary, os.Args, os.Environ()); err != nil {
		log.Error("restart failed, keeping the running conf", "binary", binary, "error", err)
	}
}

// logger writes to stderr for the journal, which stamps its own time on every line.
func logger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}

	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
}
