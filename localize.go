package shepherd

import (
	"context"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
)

type HasLocalizedCheck interface {
	WasLocalized(path string) bool
}

type Uploader interface {
	Upload(uploads []*Upload) error
}

type Localizer interface {
	HasLocalizedCheck
	Prepare(downloads []*Download) error
	Clean()
}

type Downloader struct {
	downloadTimestamps map[string]time.Time
	client             *storage.Client
	workdir            string
}

func NewDownloader(workdir string) *Downloader {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		panic(err)
	}
	return &Downloader{downloadTimestamps: make(map[string]time.Time),
		workdir: workdir,
		client:  client}
}

func (d *Downloader) WasLocalized(p string) bool {
	absPath := path.Join(d.workdir, p)
	fi, err := os.Stat(absPath)
	if err != nil {
		log.Printf("Warning: Got error on stat of %s: %s", absPath, err)
		return false
	}

	origTime, exists := d.downloadTimestamps[p]
	// log.Printf("p=%s, origTime=%v, exists=%v downloadTimestamps=%v", p, origTime, exists, d.downloadTimestamps)
	if exists && origTime.Equal(fi.ModTime()) {
		return true
	}

	return false
}

var GSCPathExpr = regexp.MustCompile("gs://([^/]+)/?(.*)$")

func splitGSCPath(url string) (string, string) {
	parts := GSCPathExpr.FindStringSubmatch(url)
	return parts[1], parts[2]
}

func ensureDirExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, os.ModePerm)
	}
	return nil
}

func ensureParentDirExists(filename string) error {
	dir := path.Dir(filename)
	return ensureDirExists(dir)
}

func (d *Downloader) download(ctx context.Context, workdir string, download *Download) (string, error) {
	bucketName, keyName := splitGSCPath(download.SourceURL)
	bucket := d.client.Bucket(bucketName)
	object := bucket.Object(keyName)

	dstPath := path.Join(workdir, download.DestinationPath)
	err := ensureParentDirExists(dstPath)
	if err != nil {
		return "", err
	}

	var mode os.FileMode = 0666
	if download.Executable {
		mode = 0777
	}

	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	reader, err := object.NewReader(ctx)
	if err != nil {
		return "", err
	}

	_, err = io.Copy(dst, reader)
	if err != nil {
		return "", err
	}

	return dstPath, nil
}

func upload(ctx context.Context, client *storage.Client, srcPath string, destURL string) error {
	bucketName, keyName := splitGSCPath(destURL)
	bucket := client.Bucket(bucketName)
	object := bucket.Object(keyName)

	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := object.NewWriter(ctx)
	_, err = io.Copy(writer, f)
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return err
}

func (d *Downloader) Upload(uploads []*Upload) error {
	ctx := context.Background()

	for _, uploadRec := range uploads {
		err := upload(ctx, d.client, path.Join(d.workdir, uploadRec.SourcePath), uploadRec.DestinationURL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Downloader) Prepare(downloads []*Download) error {
	ctx := context.Background()

	for _, download := range downloads {
		dstPath, err := d.download(ctx, d.workdir, download)
		if err != nil {
			return err
		}

		fi, err := os.Stat(dstPath)
		if err != nil {
			panic(err)
		}

		d.downloadTimestamps[download.DestinationPath] = fi.ModTime()
	}

	return nil
}

func (d *Downloader) Clean() {
}

type GCSMounter struct {
	workRootDir        string
	workdir            string
	mounts             []string
	downloadTimestamps map[string]time.Time
	umountExecutable   string
	gcsfuseExecutable  string
}

func NewGCSMounter(workRootDir string, workDir string) *GCSMounter {
	return &GCSMounter{workRootDir: workRootDir,
		workdir:            workDir,
		downloadTimestamps: make(map[string]time.Time),
		gcsfuseExecutable:  "gcsfuse",
		umountExecutable:   "umount"}
}

func (d *GCSMounter) Clean() {
	for _, mount := range d.mounts {
		log.Printf("umounting %s", mount)
		cmd := exec.Command(d.umountExecutable, mount)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
		err = cmd.Wait()
		if err != nil {
			panic(err)
		}
	}
}

func (d *GCSMounter) WasLocalized(p string) bool {
	absPath := path.Join(d.workdir, p)
	fi, err := os.Stat(absPath)
	if err != nil {
		log.Printf("Warning: Got error on stat of %s: %s", absPath, err)
		return false
	}

	origTime, exists := d.downloadTimestamps[p]
	if exists && origTime.Equal(fi.ModTime()) {
		return true
	}

	return false
}

func (d *GCSMounter) Prepare(downloads []*Download) error {
	// determine the unique bucket names
	buckets := make(map[string]bool)
	for _, download := range downloads {
		bucket, _ := splitGSCPath(download.SourceURL)
		buckets[bucket] = true
	}

	// for each bucket name, mount gcs bucket
	d.mounts = make([]string, 0, len(buckets))
	bucketToDir := make(map[string]string)
	for bucketName := range buckets {
		mountPath, err := mount(d.gcsfuseExecutable, d.workRootDir, bucketName)
		if err != nil {
			d.Clean()
			return err
		}
		d.mounts = append(d.mounts, mountPath)
		bucketToDir[bucketName] = mountPath
	}

	// Now that those directories are availible, get the files from the mount point
	for _, download := range downloads {
		if download.Executable {
			panic("unimp")
		}

		bucket, key := splitGSCPath(download.SourceURL)
		src := path.Join(bucketToDir[bucket], key)
		dest := path.Join(d.workdir, download.DestinationPath)
		err := ensureParentDirExists(dest)
		if err != nil {
			panic(err)
		}
		if download.SymlinkSafe {
			destDir := path.Dir(dest)
			relSrc, err := filepath.Rel(destDir, src)
			if err != nil {
				panic(err)
			}
			log.Printf("Creating symlink %s -> %s", relSrc, dest)
			err = os.Symlink(relSrc, dest)
			if err != nil {
				panic(err)
			}
		} else {
			log.Printf("Copying %s -> %s", src, dest)
			err := copyFile(src, dest)
			if err != nil {
				panic(err)
			}
		}

		fi, err := os.Stat(dest)
		if err != nil {
			panic(err)
		}

		d.downloadTimestamps[download.DestinationPath] = fi.ModTime()
	}

	return nil
}

func copyFile(src string, dest string) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()
	w, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	return nil
}

func mount(gcsfuseExecutable string, workRootDir string, bucketName string) (string, error) {
	gcsfusemounts := path.Join(workRootDir, "gcsfusemounts")
	gcsfusemountstmp := path.Join(workRootDir, "gcsfusemountstmp")
	ensureDirExists(gcsfusemountstmp)

	mountDir := path.Join(gcsfusemounts, bucketName)
	tempDir := path.Join(gcsfusemountstmp, bucketName)
	ensureDirExists(mountDir)
	ensureDirExists(tempDir)

	cmd := exec.Command(gcsfuseExecutable,
		"-o", "ro",
		"--stat-cache-ttl", "24h",
		"--type-cache-ttl", "24h",
		"--file-mode", "755",
		"--implicit-dirs",
		"--temp-dir", gcsfusemountstmp,
		bucketName,
		mountDir)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err := cmd.Start()
	if err != nil {
		return "", err
	}

	err = cmd.Wait()
	if err != nil {
		return "", err
	}

	return mountDir, nil
}

func (d *GCSMounter) Upload(uploads []*Upload) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	for _, uploadRec := range uploads {
		err := upload(ctx, client, path.Join(d.workdir, uploadRec.SourcePath), uploadRec.DestinationURL)
		if err != nil {
			return err
		}
	}
	return nil
}
