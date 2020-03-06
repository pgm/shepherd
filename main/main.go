package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/pgm/shepherd"
)

func main() {
	p := shepherd.Parameters{}

	filename := os.Args[1]
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	json.Unmarshal(buf, &p)

	workDir, err := ioutil.TempDir(".", "tmp-work-")
	if err != nil {
		panic(err)
	}

	log.Printf("Executing job in new directory: %s", workDir)

	err = shepherd.Execute(workDir, workDir, &p, shepherd.NewDownloader(workDir))
	if err != nil {
		panic(err)
	}
}
