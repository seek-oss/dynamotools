package restore

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type batchWriter struct {
	db    dynamodbiface.DynamoDBAPI
	table string
}

func (bw *batchWriter) writeBatch(requestsChan chan []*dynamodb.WriteRequest, delay time.Duration) error {
	for reqs := range requestsChan {
		resp, err := bw.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				bw.table: reqs,
			},
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		})
		if len(resp.UnprocessedItems) != 0 {
			log.Println("warning unprocessed items found")
			log.Printf("retrying unprocessed items after %v", delay)
			time.Sleep(delay)

			// create a temporary channel and recurse.
			queueWrites := make(chan []*dynamodb.WriteRequest, 1)
			queueWrites <- resp.UnprocessedItems[bw.table]
			close(queueWrites)
			bw.writeBatch(queueWrites, 2*delay)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (bw *batchWriter) createBatchWrites(in chan map[string]interface{}) chan []*dynamodb.WriteRequest {
	const flushSize = 25 // TODO: should be configurable
	out := make(chan []*dynamodb.WriteRequest)
	var chunk []*dynamodb.WriteRequest
	go func() {
		defer close(out)
		for obj := range in {
			av, err := dynamodbattribute.MarshalMap(obj)
			if err != nil {
				log.Printf("error convernting to *dynamodb.AttributeValue: %v", err)
				break
			}

			// build a chunk
			chunk = append(chunk, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: av,
				},
			})

			if len(chunk) == flushSize {
				out <- chunk
				chunk = nil
			}
		}
		// flush the remaining chunk.
		if len(chunk) != 0 {
			out <- chunk
		}
	}()

	return out
}

func (bw *batchWriter) Write(input chan map[string]interface{}) error {
	return bw.writeBatch(bw.createBatchWrites(input), 250*time.Millisecond)
}

// NewDynamoBatchWriter creates new dynamo writer which sends the data to dynamo in batches of 25 requests
func NewDynamoBatchWriter(db dynamodbiface.DynamoDBAPI, table string) DynamoWriter {
	return &batchWriter{
		db:    db,
		table: table,
	}
}

// DynamoWriter provides the interface to write data to dynamo
type DynamoWriter interface {
	Write(input chan map[string]interface{}) error
}
