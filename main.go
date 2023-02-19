package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	log "github.com/sirupsen/logrus"
)

var sess *session.Session
var bucket string
var key string

func init() {
	sess, _ = session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewSharedCredentials("", "personal"),
	})
	log.SetFormatter(&log.JSONFormatter{PrettyPrint: true})
	log.SetLevel(log.DebugLevel)
	bucket = os.Getenv("SAMPLE_BUCKET")
	key = os.Getenv("SAMPLE_KEY")
}

func main() {
	file, err := DownloadFile(context.TODO(), sess, bucket, key)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error downloading the file")
	}

	contents, err := ParseFile(file)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error parsing the file")
	}

	err = DeleteFile(file)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error deleting the file")
	}

	for _, c := range contents {
		log.WithFields(log.Fields{
			"record": c,
		}).Debug("printing the record")
	}
}
