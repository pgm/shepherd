package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/pgm/shepherd"
	"github.com/spf13/cobra"
)

const DownloadStrategy = "download"
const GCSFuseStrategy = "gcsfuse"

func execShepherd(filename string, strategy string) {
	p := shepherd.Parameters{}

	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	json.Unmarshal(buf, &p)

	rootDir, err := ioutil.TempDir(".", "tmp-work-")
	if err != nil {
		panic(err)
	}

	workDir := path.Join(rootDir, "work")

	log.Printf("Executing job in new directory: %s", workDir)

	var localizer shepherd.Localizer
	var uploader shepherd.Uploader

	if strategy == DownloadStrategy {
		l := shepherd.NewDownloader(workDir)
		localizer = l
		uploader = l
	} else if strategy == GCSFuseStrategy {
		l := shepherd.NewGCSMounter(rootDir, workDir)
		localizer = l
		uploader = l
	} else {
		panic("unknown strategy")
	}

	err = shepherd.Execute(workDir, workDir, &p, localizer, uploader)
	if err != nil {
		panic(err)
	}
}

func main() {

	var rootCmd = &cobra.Command{
		Use:   "shepherd",
		Short: "shepherd is a tool for executing a command where inputs are localized from GCS and then uploaded afterwards",
		Run: func(cmd *cobra.Command, args []string) {
			var strategy string
			cmd.LocalFlags().StringVarP(&strategy, "strategy", "s", DownloadStrategy, "either \"download\" or \"gcsfuse\"")
			execShepherd(args[0], strategy)
		},
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
