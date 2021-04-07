package main

import (
	"github.com/alecthomas/kong"
	"github.com/criteo-forks/aerolru/cmd"
)

var CLI struct {
	Serve cmd.ServeCmd `cmd help:"aerolru get lru and delete data if needed"`
}

func main() {
	ctx := kong.Parse(&CLI)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
