//go:build !windows

package main

import (
	"context"
	"log"
	"runtime"
	"syscall"
)

func (t *Telegraf) Run() error {
	stop = make(chan struct{})
	defer close(stop)

	staged := t.stageConfiguration(context.Background())
	if staged.err != nil {
		return staged.err
	}
	if err := t.activateConfiguration(staged); err != nil {
		return err
	}
	return t.reloadLoop(staged.snapshot.LastModified)
}

func getLockedMemoryLimit() uint64 {
	var rLimitMemlock int

	switch runtime.GOOS {
	case "dragonfly", "freebsd", "netbsd", "openbsd":
		// From https://cgit.freebsd.org/src/tree/sys/sys/resource.h#n107
		rLimitMemlock = 6
	default:
		// From https://elixir.bootlin.com/linux/latest/source/include/uapi/asm-generic/resource.h#L35
		rLimitMemlock = 8
	}

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(rLimitMemlock, &limit); err != nil {
		log.Printf("E! Cannot get limit for locked memory: %v", err)
		return 0
	}
	//nolint:unconvert // required for e.g. FreeBSD that has the field as int64
	return uint64(limit.Max)
}
