package shepherd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

type Results struct {
	ExitCode int `json:"exit_code"`
}

type Download struct {
	SourceURL       string `json:"source_url"`
	DestinationPath string `json:"destination_path"`
	Executable      bool   `json:"executable"`
	SymlinkSafe     bool   `json:"symlink_safe"`
}

type Upload struct {
	SourcePath     string
	DestinationURL string
}

type Filter struct {
	Pattern string `json:"pattern"`
	Exclude bool   `json:"exclude"`
}

type UploadPatterns struct {
	Filters              []*Filter `json:"filters"`
	DestinationURLPrefix string    `json:"destination_url_prefix"`
}

type Parameters struct {
	Uploads     *UploadPatterns `json:"uploads"`
	Downloads   []*Download     `json:"downloads"`
	DockerImage string          `json:"docker_image"`
	Command     []string        `json:"command"`
	WorkingPath string          `json:"working_path"`
	ResultPath  string          `json:"result_path"`
	StdoutPath  string          `json:"stdout_path"`
	StderrPath  string          `json:"stderr_path"`
	// PreDownloadScript  string            `json:"pre-download-script,omitempty"`
	// PostDownloadScript string            `json:"post-download-script,omitempty"`
	// PostExecScript     string            `json:"post-exec-script,omitempty"`
	// PreExecScript      string            `json:"pre-exec-script,omitempty"`
	// Parameters map[string]string `json:"parameters,omitempty"`
}

func validateURL(url string) error {
	if !GSCPathExpr.MatchString(url) {
		return fmt.Errorf("%s did not start with gs://", url)
	}
	return nil
}

func validatePath(path string) error {
	if strings.HasPrefix(path, "/") {
		return fmt.Errorf("%s was not a relative path", path)
	}

	if strings.Contains(path, "..") {
		return fmt.Errorf("%s contained '..' but paths are not allowed to reference parent directories", path)
	}
	return nil
}

func validateParameters(params *Parameters) error {
	var err error

	if len(params.Command) == 0 {
		return errors.New("empty command")
	}

	if err == nil {
		if params.Uploads != nil {
			err = validateURL(params.Uploads.DestinationURLPrefix)
		}
	}

	for _, download := range params.Downloads {
		if err == nil {
			err = validateURL(download.SourceURL)
		}
		if err == nil {
			err = validatePath(download.DestinationPath)
		}
	}

	if err == nil {
		if params.WorkingPath != "" {
			err = validatePath(params.WorkingPath)
		}
	}

	if err == nil {
		if params.StdoutPath != "" {
			err = validatePath(params.StdoutPath)
		}
	}
	if err == nil {
		if params.StderrPath != "" {
			err = validatePath(params.StderrPath)
		}
	}
	if err == nil {
		if params.ResultPath != "" {
			err = validatePath(params.ResultPath)
		}
	}

	return err
}

func prepareCommand(workdir string, command []string, WorkingPath string, StdoutPath string, StderrPath string) (*exec.Cmd, error) {
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = WorkingPath

	if StdoutPath != "" {
		p := path.Join(workdir, StdoutPath)
		err := ensureParentDirExists(p)
		if err != nil {
			return nil, err
		}
		stdout, err := os.Create(p)
		if err != nil {
			return nil, err
		}
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = os.Stdout
	}

	if StdoutPath != "" {
		if StderrPath == StdoutPath {
			cmd.Stderr = cmd.Stdout
		} else {
			p := path.Join(workdir, StderrPath)
			err := ensureParentDirExists(p)
			if err != nil {
				return nil, err
			}

			stderr, err := os.Create(p)
			if err != nil {
				return nil, err
			}
			cmd.Stderr = stderr
		}
	} else {
		cmd.Stderr = os.Stderr
	}

	return cmd, nil
}

func writeResult(resultPath string, state *os.ProcessState) error {
	err := ensureParentDirExists(resultPath)
	if err != nil {
		return err
	}

	results := Results{ExitCode: state.ExitCode()}
	b, err := json.Marshal(&results)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(resultPath, b, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

const DockerWorkRoot = "/mnt/shepherd"

func Execute(workRoot string, workdir string, params *Parameters, localizer Localizer, uploader Uploader) error {
	log.Printf("Validating parameters...")
	err := validateParameters(params)
	if err != nil {
		return err
	}

	log.Printf("Preparing %s with %d files in GCS...", workdir, len(params.Downloads))
	err = localizer.Prepare(params.Downloads)
	if err != nil {
		return err
	}

	defer localizer.Clean()

	var fullWorkPath string
	if params.WorkingPath == "" {
		fullWorkPath = workdir
	} else {
		fullWorkPath = path.Join(workdir, params.WorkingPath)
	}
	err = ensureDirExists(fullWorkPath)
	if err != nil {
		return err
	}

	command := params.Command
	if params.DockerImage != "" {
		relWorkDir, err := filepath.Rel(workRoot, fullWorkPath)
		if err != nil {
			panic(err)
		}
		dockerWorkDir := path.Join(DockerWorkRoot, relWorkDir)
		absWorkRoot, err := filepath.Abs(workRoot)
		if err != nil {
			panic(err)
		}
		command = append([]string{"docker", "run", "-v", absWorkRoot + ":" + DockerWorkRoot, "-w", dockerWorkDir, "--interactive", "--rm", params.DockerImage}, command...)
	}

	cmd, err := prepareCommand(workdir, command, fullWorkPath, params.StdoutPath, params.StderrPath)
	if err != nil {
		return err
	}

	log.Printf("With working dir %s, running command: %v", cmd.Dir, cmd.Args)
	err = cmd.Start()
	if err != nil {
		return err
	}

	log.Printf("Waiting for command to complete")
	err = cmd.Wait()
	if _, isExitError := err.(*exec.ExitError); isExitError {
		log.Printf("Exited with failure: %s", err)
	} else if err != nil {
		return err
	}

	log.Printf("Command completed, writing exit code (%d) to %s", cmd.ProcessState.ExitCode(), params.ResultPath)
	if params.ResultPath != "" {
		err = writeResult(path.Join(workdir, params.ResultPath), cmd.ProcessState)
		if err != nil {
			return err
		}
	}

	err = uploadResults(workdir, params.Uploads, localizer, uploader)
	if err != nil {
		return err
	}

	return nil
}

func matchesInclusionPattern(name string, filters []*Filter) bool {
	exclude := true
	baseName := path.Base(name)
	for _, filter := range filters {
		fullNameMatched, _ := filepath.Match(filter.Pattern, name)
		baseNameMatched, _ := filepath.Match(filter.Pattern, baseName)
		if fullNameMatched || baseNameMatched {
			exclude = filter.Exclude
		}
	}
	return !exclude
}

func findNewFiles(workdir string, filters []*Filter, localizer HasLocalizedCheck) ([]string, error) {
	filenames := make([]string, 0, 100)
	err := filepath.Walk(workdir, func(_path string, info os.FileInfo, err error) error {
		relPath, err := filepath.Rel(workdir, _path)
		if err != nil {
			panic(err) // should not be possible
		}

		if info.IsDir() {
			if matchesInclusionPattern(relPath, filters) {
				return nil
			} else {
				return filepath.SkipDir
			}
		}

		// log.Printf("checking localizer.WasLocalized(%s)", relPath)
		if localizer.WasLocalized(relPath) {
			// log.Printf("true")
			return nil
		}
		// log.Printf("false")

		if matchesInclusionPattern(relPath, filters) {
			filenames = append(filenames, relPath)
		}
		return nil
	})
	return filenames, err
}

func uploadResults(workdir string, uploadPatterns *UploadPatterns, localizer Localizer, uploader Uploader) error {
	if uploadPatterns != nil {
		filenames, err := findNewFiles(workdir, uploadPatterns.Filters, localizer)
		if err != nil {
			return err
		}
		uploads := make([]*Upload, len(filenames))
		for i, filename := range filenames {
			uploads[i] = &Upload{SourcePath: filename, DestinationURL: joinURL(uploadPatterns.DestinationURLPrefix, filename)}
		}
		log.Printf("Uploading %d files to %s", len(uploads), uploadPatterns.DestinationURLPrefix)
		err = uploader.Upload(uploads)
		if err != nil {
			return err
		}
		log.Printf("Upload completed")
	}

	return nil
}

func joinURL(prefix string, suffix string) string {
	if strings.HasPrefix(suffix, "/") {
		panic(fmt.Sprintf("path %s should not start with /", suffix))
	}
	if strings.HasSuffix(prefix, "/") {
		return prefix + suffix
	}
	return prefix + "/" + suffix
}

// type ResultFile struct {
// 	Src    string `json:"src"`
// 	DstURL string `json:"dst_url"`
// }

// type ResourceUsage struct {
// 	UserCPUTime        syscall.Timeval `json:"user_cpu_time"`
// 	SystemCPUTime      syscall.Timeval `json:"system_cpu_time"`
// 	MaxMemorySize      int64           `json:"max_memory_size"`
// 	SharedMemorySize   int64           `json:"shared_memory_size"`
// 	UnsharedMemorySize int64           `json:"unshared_memory_size"`
// 	BlockInputOps      int64           `json:"block_input_ops"`
// 	BlockOutputOps     int64           `json:"block_output_ops"`
// }

// type ResultStruct struct {
// 	Command    string            `json:"command"`
// 	Parameters map[string]string `json:"parameters,omitempty"`
// 	ReturnCode string            `json:"return_code"`
// 	Files      []*ResultFile     `json:"files"`
// 	Usage      *ResourceUsage    `json:"resource_usage"`
// }

// type Parameters struct {
// 	Downloads
// }
