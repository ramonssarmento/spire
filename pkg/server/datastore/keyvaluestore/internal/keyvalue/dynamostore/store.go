package dynamostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	streamsValues "github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/keyvalue"
	"github.com/spiffe/spire/pkg/server/datastore/keyvaluestore/internal/keyvalue/internal/watch"
)

type TableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      *string
	StreamClient   *dynamoDBStreamsClient
}

type LocalMetadata struct {
	CreatedAt time.Time `json:"CreatedAt"`
	UpdatedAt time.Time `json:"UpdatedAt"`
	Revision  int64     `json:"Revision"`
}

type LocalRecord struct {
	LocalMetadata `json:"Metadata"`
	Kind          string `json:"Kind"`
	Key           string `json:"Key"`
	Value         []byte `json:"Value"`
}

type Config struct {
	Now             func() time.Time
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Endpoint        string
	TableName       string
	StreamEnable    *bool
}

type Store struct {
	now      func() time.Time
	watches  watch.Watchers[watcher]
	awsTable *TableBasics
}

type dynamoDBStreamsClient struct {
	streamArn                  *string
	streamClient               *dynamodbstreams.Client
	targetShardCount           int64
	activationTargetShardCount int64
}

func (local *LocalRecord) ConvertToRecord() keyvalue.Record {
	return keyvalue.Record{
		Metadata: keyvalue.Metadata{
			CreatedAt: local.LocalMetadata.CreatedAt,
			UpdatedAt: local.LocalMetadata.UpdatedAt,
			Revision:  local.LocalMetadata.Revision,
		},
		Kind:  local.Kind,
		Key:   local.Key,
		Value: local.Value,
	}
}

func newAWSConfig(ctx context.Context, c *Config) (aws.Config, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(c.Region),
	)
	if err != nil {
		return aws.Config{}, err
	}

	if c.SecretAccessKey != "" && c.AccessKeyID != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, "")
	}

	return cfg, nil
}

func createClient(ctx context.Context, config Config) (*dynamodb.Client, error) {
	cfg, err := newAWSConfig(ctx, &config)

	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if config.Endpoint != "" {
			o.BaseEndpoint = aws.String(config.Endpoint)
		}
	}), nil
}

func createStreamClient(ctx context.Context, config Config, dbClient *dynamodb.Client) (*dynamoDBStreamsClient, error) {
	if !*config.StreamEnable {
		return &dynamoDBStreamsClient{}, nil
	}

	cfg, err := newAWSConfig(ctx, &config)

	if err != nil {
		return nil, err
	}

	streamClient := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		if config.Endpoint != "" {
			o.BaseEndpoint = aws.String(config.Endpoint)
		}
	})

	streamArn, err := getDynamoDBStreamsArn(ctx, dbClient, &config.TableName)
	if err != nil {
		return nil, fmt.Errorf("error getting dynamodb stream ARN: %w", err)
	}

	return &dynamoDBStreamsClient{
		streamClient: streamClient,
		streamArn:    streamArn,
	}, nil
}

func getDynamoDBStreamsArn(ctx context.Context, dbClient *dynamodb.Client, tableName *string) (*string, error) {
	tableOutput, err := dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: tableName,
	})
	if err != nil {
		return nil, err
	}
	if tableOutput.Table.LatestStreamArn == nil {
		return nil, fmt.Errorf("dynamodb stream ARN for the table %s is empty", *tableName)
	}
	return tableOutput.Table.LatestStreamArn, nil
}

func buildCreateTableInput(tableName string, streamEnabled *bool) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("Kind"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("Kind"),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  streamEnabled,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}
}

func tableExists(ctx context.Context, client *dynamodb.Client, name string) bool {
	tables, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		fmt.Printf("Dynamo list tables error %+v\n", err)
		return false
	}
	for _, n := range tables.TableNames {
		if n == name {
			return true
		}
	}
	return false
}

func Open(ctx context.Context, config Config) (*Store, error) {
	if config.Now == nil {
		config.Now = time.Now
	}

	if config.TableName == "" {
		config.TableName = "Spire"
	}

	dynamoClient, err := createClient(ctx, config)

	if err != nil {
		return nil, err
	}

	if tableExists(ctx, dynamoClient, config.TableName) {
		fmt.Printf("Dynamo table Spire exist\n")
	} else {
		fmt.Printf("Dynamo table Spire not exist, creating...\n")
		_, err := dynamoClient.CreateTable(ctx, buildCreateTableInput(config.TableName, config.StreamEnable))
		if err != nil {
			fmt.Printf("CreateTable failed", err)
		}
	}

	streamClient, err := createStreamClient(ctx, config, dynamoClient)

	if err != nil {
		return nil, err
	}

	return &Store{
		now: config.Now,
		awsTable: &TableBasics{
			DynamoDbClient: dynamoClient,
			TableName:      aws.String(config.TableName),
			StreamClient:   streamClient,
		},
	}, nil
}

func (s *Store) Close() error {
	s.watches.Close()
	return nil
}

func (s *Store) Get(ctx context.Context, kind string, key string) (keyvalue.Record, error) {
	//fmt.Printf("Dynamo Get kind: %s, key: %s\n", kind, key)

	tableKey := map[string]types.AttributeValue{
		"Key":  &types.AttributeValueMemberS{Value: key},
		"Kind": &types.AttributeValueMemberS{Value: kind},
	}

	input := &dynamodb.GetItemInput{
		TableName: s.awsTable.TableName,
		Key:       tableKey,
	}

	result, err := s.awsTable.DynamoDbClient.GetItem(ctx, input)
	if err != nil {
		fmt.Printf("Dynamo Failed to read item: %v\n", err)
		return keyvalue.Record{}, err // Return an empty record
	}

	if len(result.Item) == 0 {
		return keyvalue.Record{}, keyvalue.ErrNotFound
	}

	var record keyvalue.Record
	if err := attributevalue.UnmarshalMap(result.Item, &record); err != nil {
		return keyvalue.Record{}, err
	}

	return record, nil
}

func (s *Store) Create(ctx context.Context, kind string, key string, value []byte) error {
	//fmt.Printf("Dynamo Create %s %s\n", kind, key)

	now := s.now()

	record := encodeDynamoRecord(now, now, value, 1)
	record.Key = key
	record.Kind = kind

	attribs, err := attributevalue.MarshalMap(record)
	if err != nil {
		return err
	}

	condition, err := notExist().Build()
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		ExpressionAttributeNames: condition.Names(),
		ConditionExpression:      condition.Condition(),
		TableName:                s.awsTable.TableName,
		Item:                     attribs,
	}

	_, err = s.awsTable.DynamoDbClient.PutItem(ctx, input)

	if err != nil {
		fmt.Printf("Dynamo Failed to write item: %v\n", err)

		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			fmt.Printf("Create failed because entry exists, %v",
				conditionalCheckErr.ErrorMessage())
			return keyvalue.ErrExists
		}

		return err
	}

	return nil
}

func (s *Store) Update(ctx context.Context, kind string, key string, value []byte, revision int64) error {
	//fmt.Printf("Dynamo Update %s %s\n", kind, key)

	tableKey := map[string]types.AttributeValue{
		"Key":  &types.AttributeValueMemberS{Value: key},
		"Kind": &types.AttributeValueMemberS{Value: kind},
	}

	updateExpr := expression.Set(expression.Name("Value"), expression.Value(value)).
		Set(expression.Name("UpdatedAt"), expression.Value(s.now())).
		Add(expression.Name("Revision"), expression.Value(1))

	expr, err := atModRevision(revision).WithUpdate(updateExpr).Build()

	if err != nil {
		fmt.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return err
	}

	_, err = s.awsTable.DynamoDbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 s.awsTable.TableName,
		Key:                       tableKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		//ReturnValues:              types.ReturnValueUpdatedNew,
	})

	if err != nil {
		fmt.Printf("Couldn't update %s %s. Here's why: %v\n", key, kind, err)
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			fmt.Printf("update failed because condition failed, %v",
				conditionalCheckErr.ErrorMessage())
			return keyvalue.ErrConflict
		}
	}

	return nil

}

func (s *Store) Replace(ctx context.Context, kind string, key string, value []byte) error {
	//fmt.Printf("Dynamo Replace %s %s\n", kind, key)

	tableKey := map[string]types.AttributeValue{
		"Key":  &types.AttributeValueMemberS{Value: key},
		"Kind": &types.AttributeValueMemberS{Value: kind},
	}

	updateExpr := expression.Set(expression.Name("Value"), expression.Value(value)).
		Set(expression.Name("UpdatedAt"), expression.Value(s.now())).
		Add(expression.Name("Revision"), expression.Value(1))

	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()

	if err != nil {
		fmt.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return err
	}

	_, err = s.awsTable.DynamoDbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 s.awsTable.TableName,
		Key:                       tableKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		//ReturnValues:              types.ReturnValueUpdatedNew,
	})

	if err != nil {
		fmt.Printf("Couldn't replace %s %s. Here's why: %v\n", key, kind, err)
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, kind string, key string) error {
	//fmt.Printf("Dynamo Delete %+v\n", key)

	tableKey := map[string]types.AttributeValue{
		"Key":  &types.AttributeValueMemberS{Value: key},
		"Kind": &types.AttributeValueMemberS{Value: kind},
	}

	condition, err := exist().Build()
	if err != nil {
		return err
	}

	_, err = s.awsTable.DynamoDbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                s.awsTable.TableName,
		Key:                      tableKey,
		ExpressionAttributeNames: condition.Names(),
		ConditionExpression:      condition.Condition(),
	})
	if err != nil {
		fmt.Printf("Couldn't delete %v from the table. Here's why: %v\n", key, err)
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			fmt.Printf("update failed because condition failed, %v",
				conditionalCheckErr.ErrorMessage())
			return keyvalue.ErrNotFound
		}
	}

	return nil
}

func (s *Store) Batch(ctx context.Context, ops []keyvalue.Op) error {
	//fmt.Printf("Dynamo Batch\n")
	return errors.New("unimplemented")
}

func (s *Store) AtomicCounter(ctx context.Context, kind string) (uint, error) {
	//fmt.Printf("Dynamo AtomicCounter %s\n", kind)

	// The "kind" will be used as key to the counter
	tableKey := map[string]types.AttributeValue{
		"Key":  &types.AttributeValueMemberS{Value: kind},
		"Kind": &types.AttributeValueMemberS{Value: "atomicCounter"},
	}

	updateExpr := expression.Add(expression.Name("Count"), expression.Value(1))

	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()

	if err != nil {
		fmt.Printf("Couldn't build expression for update. Here's why: %v\n", err)
		return 0, err
	}

	response, err := s.awsTable.DynamoDbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 s.awsTable.TableName,
		Key:                       tableKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueUpdatedNew,
	})

	if err != nil {
		fmt.Printf("Couldn't update AtomicCounter %s. Here's why: %v\n", kind, err)
		return 0, err
	}

	var newValue uint
	err = attributevalue.Unmarshal(response.Attributes["Count"], &newValue)
	if err != nil {
		fmt.Printf("Couldn't unmarshal AtomicCounter. Here's why: %v\n", err)
		return 0, err
	}

	return newValue, nil

}

func (s *Store) Watch(ctx context.Context) keyvalue.WatchChan {
	obj := s.watches.New(ctx, watcher{
		awsTable: s.awsTable,
		client:   s.awsTable.StreamClient,
	})

	return obj
}

type watcher struct {
	awsTable *TableBasics
	client   *dynamoDBStreamsClient
}

func (w watcher) Watch(ctx context.Context, report watch.ReportFunc) error {
	var shardIterator *string

	err := w.loadAll(ctx, report)
	if err != nil {
		return err
	}

	report(nil, true)

	if w.client.streamClient != nil {
		_, lastShardID, StartingSequenceNumber, err := w.client.GetShardCount(ctx)
		if err != nil {
			return fmt.Errorf("Failed to get shard count: %v", err)
		}
		//fmt.Printf("Shard Count: %d\n", shardCount)

		if lastShardID == nil {
			return fmt.Errorf("No shards found.")
		}
		shardIterator, err = w.client.GetShardIterator(ctx, lastShardID, StartingSequenceNumber)
		if err != nil {
			return fmt.Errorf("Failed to get shard iterator: %v", err)
		}

	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-time.After(time.Second):
			if w.client.streamClient != nil {
				if shardIterator, err = w.client.PollStream(ctx, shardIterator, report); err != nil {
					fmt.Printf("Error polling DynamoDB stream: %v\n", err)
				}
			}
		}
	}

	return nil
}

func (w *watcher) loadAll(ctx context.Context, report watch.ReportFunc) error {

	var err error
	var response *dynamodb.ScanOutput

	projEx := expression.NamesList(
		expression.Name("Key"),
		expression.Name("Kind"),
	)

	expr, err := expression.NewBuilder().WithProjection(projEx).Build()
	if err != nil {
		fmt.Printf("Couldn't build expressions for scan. Here's why: %v\n", err)
		return err
	}

	scanPaginator := dynamodb.NewScanPaginator(w.awsTable.DynamoDbClient, &dynamodb.ScanInput{
		TableName:                w.awsTable.TableName,
		ExpressionAttributeNames: expr.Names(),
		ProjectionExpression:     expr.Projection(),
	})

	for scanPaginator.HasMorePages() {
		response, err = scanPaginator.NextPage(ctx)
		if err != nil {
			fmt.Printf("Couldn't scan for keys. Here's why: %v\n", err)
			return err
		}

		keys := new([]keyvalue.Key)
		if err = attributevalue.UnmarshalListOfMaps(response.Items, keys); err != nil {
			fmt.Printf("Couldn't unmarshal PK. Here's why: %v\n", err)
			return err
		}

		report(*keys, true)
	}

	return nil
}

func (c *dynamoDBStreamsClient) GetShardCount(ctx context.Context) (int64, *string, *string, error) {
	var shardCount int64
	var lastShardID *string
	var StartingSequenceNumber *string

	input := dynamodbstreams.DescribeStreamInput{
		StreamArn: c.streamArn,
	}
	for {

		if lastShardID != nil {
			input.ExclusiveStartShardId = lastShardID
		}

		output, err := c.streamClient.DescribeStream(ctx, &input)
		if err != nil {
			return 0, nil, nil, err
		}

		shardCount += int64(len(output.StreamDescription.Shards))
		lastShardID = output.StreamDescription.LastEvaluatedShardId

		if lastShardID == nil {
			lastShardID = output.StreamDescription.Shards[shardCount-1].ShardId
			StartingSequenceNumber = output.StreamDescription.Shards[shardCount-1].SequenceNumberRange.StartingSequenceNumber
			break
		}
	}

	return shardCount, lastShardID, StartingSequenceNumber, nil
}

func (c *dynamoDBStreamsClient) GetShardIterator(ctx context.Context, shardId *string, StartingSequenceNumber *string) (*string, error) {
	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           shardId,
		StreamArn:         c.streamArn,
		ShardIteratorType: "LATEST",
	}
	result, err := c.streamClient.GetShardIterator(ctx, shardIteratorInput)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator: %w", err)
	}
	return result.ShardIterator, nil
}

func (c *dynamoDBStreamsClient) PollStream(ctx context.Context, shardIterator *string, report watch.ReportFunc) (*string, error) {
	getRecordsInput := &dynamodbstreams.GetRecordsInput{
		ShardIterator: shardIterator,
	}
	result, err := c.streamClient.GetRecords(ctx, getRecordsInput)
	if err != nil {
		return shardIterator, fmt.Errorf("failed to get records: %w", err)
	}

	if len(result.Records) > 0 {
		var keys []keyvalue.Key
		seen := make(map[keyvalue.Key]struct{})

		for _, record := range result.Records {
			key := keyvalue.Key{}

			err = streamsValues.UnmarshalMap(record.Dynamodb.Keys, &key)
			if err != nil {
				return shardIterator, fmt.Errorf("failed to get records: %w", err)
			}

			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				keys = append(keys, key)
				//fmt.Printf("Watch %+v\n", key)
			}
		}

		report(keys, false)

	}

	shardIterator = result.NextShardIterator

	return shardIterator, nil
}

func encodeDynamoRecord(createdAt, updatedAt time.Time, data []byte, revision int64) keyvalue.Record {
	return keyvalue.Record{
		Metadata: keyvalue.Metadata{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
			Revision:  revision,
		},
		Value: data,
	}
}

func notExist() expression.Builder {
	return expression.NewBuilder().
		WithCondition(expression.And(expression.AttributeNotExists(expression.Name("Key")), expression.AttributeNotExists(expression.Name("Kind"))))
}
func exist() expression.Builder {
	return expression.NewBuilder().
		WithCondition(expression.And(expression.AttributeExists(expression.Name("Key")), expression.AttributeExists(expression.Name("Kind"))))
}

func atModRevision(revision int64) expression.Builder {
	return expression.NewBuilder().
		WithCondition(expression.Equal(expression.Name("Revision"), expression.Value(revision)))
}
