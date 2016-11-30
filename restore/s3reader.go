package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/errgroup"
)

// DynamoResotreConfig provides the configuration for archiving dynamo table to s3
type DynamoResotreConfig struct {
	Region      string
	TableName   string
	Workers     int
	Bucket      string
	RestoreFile string
}

// ToDyanmo restores the data from the file in the s3 bucket to the specified dynamo table
func ToDyanmo(c *DynamoResotreConfig) error {
	s := getNewAwsSession(c.Region)
	dl := s3manager.NewDownloader(s)

	localFile := fmt.Sprintf("restore-file-%s", time.Now().Format("2006-01-02"))
	file, err := os.Create(localFile)
	if err != nil {
		return err
	}

	defer file.Close()
	defer os.Remove(file.Name())

	log.Println("downloading restore file ....")
	_, err = dl.Download(file, &s3.GetObjectInput{
		Bucket: &c.Bucket,
		Key:    &c.RestoreFile,
	})

	if err != nil {
		return err
	}

	file.Seek(0, 0)
	dec := json.NewDecoder(file)
	itemsChan := make(chan map[string]interface{})

	log.Println("starting dynmo writer")
	//db := dynamodb.New(s)
	grp, ctx := errgroup.WithContext(context.Background())

	log.Println("workers ", c.Workers)
	for index := 0; index < c.Workers; index++ {
		grp.Go(func() error {
			return NewDynamoBatchWriter(dynamodb.New(s), c.TableName).Write(itemsChan)
		})
	}
	stop := false

	for {
		if stop {
			break
		}

		var items []map[string]interface{}
		err := dec.Decode(&items)
		if err == io.EOF {
			break
		}

		if err != nil {
			close(itemsChan)
			return err
		}
		for _, item := range items {
			select {
			case itemsChan <- item:
			case <-ctx.Done():
				stop = true
			}
		}
	}
	close(itemsChan)
	if err := grp.Wait(); err != nil {
		return err
	}
	log.Printf("completed restoring to %s", c.TableName)
	return nil
}

func getNewAwsSession(region string) *session.Session {
	awsconfig := defaults.Config().WithRegion(region) //.WithLogLevel(aws.LogDebug)
	awsconfig.Credentials = defaults.CredChain(awsconfig, defaults.Handlers())
	return session.New(awsconfig)
}
