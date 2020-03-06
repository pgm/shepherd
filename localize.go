package shepherd

import (
	"context"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
)

type HasLocalizedCheck interface {
	WasLocalized(path string) bool
}

type Localizer interface {
	HasLocalizedCheck
	Prepare(downloads []*Download) error
	Upload(uploads []*Upload) error
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

func (d *Downloader) WasLocalized(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		log.Printf("Warning: Got error on stat of %s: %s", path, err)
		return false
	}

	origTime, exists := d.downloadTimestamps[path]
	if exists && origTime.Equal(fi.ModTime()) {
		return false
	}

	return true
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

func (d *Downloader) upload(ctx context.Context, srcPath string, destURL string) error {
	bucketName, keyName := splitGSCPath(destURL)
	bucket := d.client.Bucket(bucketName)
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

	for _, upload := range uploads {
		err := d.upload(ctx, path.Join(d.workdir, upload.SourcePath), upload.DestinationURL)
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

		d.downloadTimestamps[dstPath] = fi.ModTime()
	}

	return nil
}

func (d *Downloader) Clean() {
}
