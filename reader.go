package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fraugster/parquet-go/floor"
	_ "github.com/fraugster/parquet-go/floor"
	"github.com/segmentio/ksuid"
	log "github.com/sirupsen/logrus"
)

var ErrIllegalRow = errors.New("row not fully formed")

func DownloadFile(ctx context.Context, sess *session.Session, bucket string, key string) (string, error) {
	log.WithFields(log.Fields{
		"key":    key,
		"bucket": bucket,
	}).Debug("printing out the args")
	filePath := fmt.Sprintf("files/%s.parquet", ksuid.New().String())
	file, err := os.Create(filePath)

	if err != nil {
		return "", err
	}

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.DownloadWithContext(ctx, file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return "", err
	}

	return filePath, nil
}

func DeleteFile(fileName string) error {
	err := os.Remove(fileName)

	if err != nil {
		return err
	}

	return nil
}

func ParseFile(fileName string) ([]ParquetUser, error) {
	fr, err := floor.NewFileReader(fileName)
	var fileContent []ParquetUser
	if err != nil {
		return nil, err
	}

	for fr.Next() {
		rec := &ParquetUser{}
		if err := fr.Scan(rec); err != nil {
			// continue along is it's just a malformed row
			if errors.Is(err, ErrIllegalRow) {
				continue
			}
			return nil, err
		}

		fileContent = append(fileContent, *rec)
	}

	return fileContent, nil
}
