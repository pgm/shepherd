package shepherd

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

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

type UploadPatterns struct {
	IncludePatterns []string `json:"include_patterns"`
	ExcludePatterns []string `json:"exclude_patterns"`
	DestinationURL  string   `json:"destination_url"`
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
			err = validateURL(params.Uploads.DestinationURL)
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

func prepareCommand(command []string, StdoutPath string, StderrPath string) (*exec.Cmd, error) {
	cmd := exec.Command(command[0], command[1:]...)

	log.Printf("args: %v", cmd.Args)

	if StdoutPath != "" {
		stdout, err := os.Create(StdoutPath)
		if err != nil {
			return nil, err
		}
		cmd.Stdout = stdout
	}

	if StdoutPath != "" {
		if StderrPath == StdoutPath {
			cmd.Stderr = cmd.Stdout
		} else {
			stderr, err := os.Create(StderrPath)
			if err != nil {
				return nil, err
			}
			cmd.Stderr = stderr
		}
	}

	return cmd, nil
}

func writeResult(resultPath string, state *os.ProcessState) error {
	//	panic("unimp")
	log.Printf("Warning: writeResult unimplemented")
	return nil
}

func Execute(workRoot string, workdir string, params *Parameters, localizer Localizer) error {
	log.Printf("validate")
	err := validateParameters(params)
	if err != nil {
		return err
	}

	log.Printf("prepare")
	err = localizer.Prepare(params.Downloads)
	if err != nil {
		return err
	}

	defer localizer.Clean()

	command := params.Command
	if params.DockerImage != "" {
		command = append([]string{"docker", "run", "-v", workRoot + ":" + workRoot, "-w", workdir, "--interactive", "--rm", params.DockerImage}, command...)
	}

	log.Printf("prepare command")
	cmd, err := prepareCommand(params.Command, params.StdoutPath, params.StderrPath)
	if err != nil {
		return err
	}

	log.Printf("start command")
	err = cmd.Start()
	if err != nil {
		return err
	}

	log.Printf("wait command")
	err = cmd.Wait()
	if _, isExitError := err.(*exec.ExitError); isExitError {
		log.Printf("Exited with failure: %s", err)
	} else if err != nil {
		return err
	}

	log.Printf("write result")
	err = writeResult(params.ResultPath, cmd.ProcessState)
	if err != nil {
		return err
	}

	err = uploadResults(workdir, params.Uploads, localizer)
	if err != nil {
		return err
	}

	return nil
}

func uploadResults(workdir string, uploads *UploadPatterns, localizer Localizer) error {
	if uploads != nil {
		matchesUploadPattern := func(path string) bool {
			include := false
			for _, includePattern := range uploads.IncludePatterns {
				matched, _ := filepath.Match(includePattern, path)
				if matched {
					include = true
				}
			}
			for _, excludePattern := range uploads.ExcludePatterns {
				matched, _ := filepath.Match(excludePattern, path)
				if matched {
					include = false
				}
			}
			return include
		}

		panic("unimp")
		filepath.Walk(workdir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}

			if localizer.WasLocalized(path) {

			}

			if matchesUploadPattern(path) {

			}
			return nil
		})
	}
	return nil
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
