package shepherd

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteAndLogCapture(t *testing.T) {
	workDir, err := ioutil.TempDir("", t.Name())
	assert.Nil(t, err)
	defer os.RemoveAll(workDir)

	assertFileContent := func(expected, filename string) {
		b, err := ioutil.ReadFile(path.Join(workDir, filename))
		assert.Nil(t, err)
		content := string(b)
		assert.Equal(t, expected, content)
	}

	params := &Parameters{
		Command:    []string{"bash", "-c", "echo out && echo err 1>&2"},
		ResultPath: "merged-results.json",
		StdoutPath: "merged.txt",
		StderrPath: "merged.txt"}

	err = Execute(workDir, workDir, params, NewMockLocalizer(workDir), NewMockUploader(workDir))
	assert.Nil(t, err)
	assertFileContent("out\nerr\n", "merged.txt")

	params = &Parameters{
		Command:    []string{"bash", "-c", "echo out && echo err 1>&2"},
		ResultPath: "split-results.json",
		StdoutPath: "out.txt",
		StderrPath: "err.txt"}

	err = Execute(workDir, workDir, params, NewMockLocalizer(workDir), NewMockUploader(workDir))
	assert.Nil(t, err)
	assertFileContent("out\n", "out.txt")
	assertFileContent("err\n", "err.txt")
}

func TestDocker(t *testing.T) {
	workDir, err := ioutil.TempDir(".", t.Name())
	assert.Nil(t, err)
	defer os.RemoveAll(workDir)

	params := &Parameters{
		Uploads: &UploadPatterns{Filters: []*Filter{&Filter{Pattern: "*"}},
			DestinationURLPrefix: "gs://mock"},
		Downloads: []*Download{&Download{SourceURL: "gs://mock/1",
			DestinationPath: "1"}},
		DockerImage: "alpine:3.7",
		Command:     []string{"sh", "-c", "echo out && echo err 1>&2 && cp 1 2"},
		StdoutPath:  "out.txt",
		StderrPath:  "err.txt"}

	localizer := NewMockLocalizer(workDir)
	localizer.urlToContent["gs://mock/1"] = "one"
	uploader := NewMockUploader(workDir)

	err = Execute(workDir, workDir, params, localizer, uploader)
	assert.Nil(t, err)

	assert.Equal(t, map[string]string{"gs://mock/2": "one",
		"gs://mock/out.txt": "out\n",
		"gs://mock/err.txt": "err\n"},
		uploader.uploaded)

}

type MockLocalizer struct {
	workDir      string
	localized    map[string]bool
	urlToContent map[string]string // url -> content
}

func NewMockLocalizer(workDir string) *MockLocalizer {
	return &MockLocalizer{workDir: workDir,
		localized:    make(map[string]bool),
		urlToContent: make(map[string]string)}
}

type MockUploader struct {
	workDir  string
	uploaded map[string]string // url -> content
}

func NewMockUploader(workDir string) *MockUploader {
	return &MockUploader{workDir: workDir,
		uploaded: make(map[string]string)}
}

func (m *MockLocalizer) WasLocalized(path string) bool {
	return m.localized[path]
}

func (m *MockLocalizer) Prepare(downloads []*Download) error {
	for _, download := range downloads {
		content, exists := m.urlToContent[download.SourceURL]
		if !exists {
			panic(fmt.Errorf("Could not find %s", download.SourceURL))
		}
		f, err := os.Create(path.Join(m.workDir, download.DestinationPath))
		if err != nil {
			panic(err)
		}
		f.WriteString(content)
		f.Close()
		m.localized[download.DestinationPath] = true
	}
	return nil
}

func (m *MockUploader) Upload(uploads []*Upload) error {
	for _, upload := range uploads {
		f, err := os.Open(path.Join(m.workDir, upload.SourcePath))
		if err != nil {
			panic(err)
		}
		b, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		m.uploaded[upload.DestinationURL] = string(b)
	}
	return nil
}

func (m *MockLocalizer) Clean() {
}

func TestSimpleTransfers(t *testing.T) {
	workDir, err := ioutil.TempDir("", t.Name())
	assert.Nil(t, err)
	log.Printf("test dir: %s", workDir)

	params := &Parameters{
		Uploads: &UploadPatterns{Filters: []*Filter{&Filter{Pattern: "*"}},
			DestinationURLPrefix: "gs://mock"},
		Downloads: []*Download{&Download{SourceURL: "gs://mock/1",
			DestinationPath: "1"}},
		Command: []string{"cp", "1", "2"}}

	localizer := NewMockLocalizer(workDir)
	localizer.urlToContent["gs://mock/1"] = "one"
	uploader := NewMockUploader(workDir)

	err = Execute(workDir, workDir, params, localizer, uploader)
	require.Nil(t, err)

	assert.Equal(t, map[string]string{"gs://mock/2": "one"}, uploader.uploaded)
}

func TestDirUpload(t *testing.T) {
	workDir, err := ioutil.TempDir("", t.Name())
	assert.Nil(t, err)
	defer os.RemoveAll(workDir)

	params := &Parameters{
		Uploads: &UploadPatterns{Filters: []*Filter{&Filter{Pattern: "*"}},
			DestinationURLPrefix: "gs://mock"},
		Command: []string{"bash", "-c", "echo -n one > 1 && mkdir subdir && echo -n two > subdir/2 && echo -n hello > subdir/hello.txt"}}

	localizer := NewMockLocalizer(workDir)
	uploader := NewMockUploader(workDir)

	err = Execute(workDir, workDir, params, localizer, uploader)
	assert.Nil(t, err)

	assert.Equal(t,
		map[string]string{"gs://mock/subdir/2": "two", "gs://mock/1": "one", "gs://mock/subdir/hello.txt": "hello"},
		uploader.uploaded)
}

func TestGCSMount(t *testing.T) {
	rootDir, err := ioutil.TempDir("", t.Name())
	assert.Nil(t, err)
	//defer os.RemoveAll(rootDir)
	log.Printf("rootDir %s", rootDir)

	// make mock gcsfuse script that we'll run instead of the normal mount command
	mockGCSExecutable := path.Join(rootDir, "mockgcsfuse")
	mockGCSScript := []byte(`#!/usr/bin/python
import sys
import time
import os
bucket_name = sys.argv[-2]
mount_dir = sys.argv[-1]

os.mkdir(mount_dir+"/something")
with open(mount_dir+"/something/1", "wt") as fd:
	fd.write(bucket_name)
#while True:
#	print("sleeping...")
#	time.sleep(1000)
	`)

	mockUmountScript := []byte(`#!/usr/bin/python
import sys
sys.exit(0)
		`)
	mockUmountExecutable := path.Join(rootDir, "mockumount")

	err = ioutil.WriteFile(mockGCSExecutable, mockGCSScript, 0700)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(mockUmountExecutable, mockUmountScript, 0700)
	if err != nil {
		panic(err)
	}

	workDir := path.Join(rootDir, "work")

	params := &Parameters{
		Uploads: &UploadPatterns{Filters: []*Filter{&Filter{Pattern: "*"}},
			DestinationURLPrefix: "gs://mock"},
		Downloads: []*Download{&Download{SourceURL: "gs://mock/something/1",
			DestinationPath: "1"}},
		Command: []string{"cp", "1", "2"}}

	localizer := NewGCSMounter(rootDir, workDir)
	localizer.gcsfuseExecutable = mockGCSExecutable
	localizer.umountExecutable = mockUmountExecutable

	uploader := NewMockUploader(workDir)
	fmt.Printf("rootDir: %s", rootDir)
	err = Execute(rootDir, workDir, params, localizer, uploader)
	assert.Nil(t, err)

	assert.Equal(t, map[string]string{"gs://mock/2": "mock"}, uploader.uploaded)
}
