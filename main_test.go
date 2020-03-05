package shepherd

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
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

	err = Execute(workDir, workDir, params, NewMockLocalizer(workDir))
	assert.Nil(t, err)
	assertFileContent("out\nerr\n", "merged.txt")

	params = &Parameters{
		Command:    []string{"bash", "-c", "echo out && echo err 1>&2"},
		ResultPath: "split-results.json",
		StdoutPath: "out.txt",
		StderrPath: "err.txt"}

	err = Execute(workDir, workDir, params, NewMockLocalizer(workDir))
	assert.Nil(t, err)
	assertFileContent("out\n", "out.txt")
	assertFileContent("err\n", "err.txt")
}

// type Localizer interface {
// 	WasLocalized(path string) bool
// 	Prepare(downloads []*Download) error
// 	Upload(uploads []*Upload) error
// 	Clean()
// }

type MockLocalizer struct {
	workDir      string
	localized    map[string]bool
	urlToContent map[string]string // url -> content
	uploaded     map[string]string // url -> content
}

func NewMockLocalizer(workDir string) *MockLocalizer {
	return &MockLocalizer{workDir: workDir,
		localized:    make(map[string]bool),
		urlToContent: make(map[string]string),
		uploaded:     make(map[string]string)}
}

func (m *MockLocalizer) WasLocalized(path string) bool {
	return m.localized[path]
}

func (m *MockLocalizer) Prepare(downloads []*Download) error {
	for _, download := range downloads {
		content := m.urlToContent[download.SourceURL]
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

func (m *MockLocalizer) Upload(uploads []*Upload) error {
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
	defer os.RemoveAll(workDir)

	params := &Parameters{
		Uploads: &UploadPatterns{IncludePatterns: []string{"*"},
			DestinationURL: "gs://mock"},
		Downloads: []*Download{&Download{SourceURL: "gs://mock/1",
			DestinationPath: "1"}},
		Command: []string{"cp", "1", "2"}}

	localizer := NewMockLocalizer(workDir)
	localizer.urlToContent["1"] = "one"

	err = Execute(workDir, workDir, params, localizer)
	assert.Nil(t, err)

	assert.Equal(t, map[string]string{"gs://mock/2": "one"}, localizer.uploaded)
}

func TestDirTransfers(t *testing.T) {
	workDir, err := ioutil.TempDir("", t.Name())
	assert.Nil(t, err)
	defer os.RemoveAll(workDir)

	params := &Parameters{
		Uploads: &UploadPatterns{IncludePatterns: []string{"*"},
			DestinationURL: "gs://mock"},
		Command: []string{"bash", "-c", "\"mkdir subdir ; echo one > subdir/2 ; echo hello > subdir/hello.txt \""}}

	localizer := NewMockLocalizer(workDir)
	localizer.urlToContent["1"] = "one"

	err = Execute(workDir, workDir, params, localizer)
	assert.Nil(t, err)

	assert.Equal(t, map[string]string{"gs://mock/subdir/2": "one", "gs://mock/subdir/hello.txt": "hello"}, localizer.uploaded)
}
