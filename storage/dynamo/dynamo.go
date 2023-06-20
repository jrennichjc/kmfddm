package dynamo

import (
	"context"
	"fmt"
	"hash"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cespare/xxhash"
)

type DSDynamoTable struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
	newHash func() hash.Hash
}

type dsPKSKOnly struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
}

type dsGenericItem struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
	Body string `dynamodbav:"body,omitempty"`
}

type dsUserAuthentication struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	UserAuth string `dynamodbav:"bstoken,omitempty"`
}

type dsTokenUpdateTally struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Tally    int    `dynamodbav:"tally,omitempty"`
}

type dsTokenUnlock struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Token    string `dynamodbav:"unlock_token,omitempty"`
}

type dsIdentityCert struct {
	Device   string `dynamodbav:"pk"`
	Category string `dynamodbav:"sk"`
	Cert     string `dynamodbav:"cert,omitempty"`
}

type config struct {
	driver string
	dsn    string
	db     DSDynamoTable
	logger log.Logger
	rm     bool
}

type Option func(*config)


func CreateTable(svc *dynamodb.Client, tn string, attributes []types.AttributeDefinition, schema []types.KeySchemaElement) {
	out, err := svc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: attributes,
		KeySchema:            schema,
		TableName:            aws.String(tn),
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	fmt.Println(out)
}

func New(tablename string) (*DSDynamoTable, error) {

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	svc := dynamodb.NewFromConfig(cfg)

	nanoTable := DSDynamoTable{
		DynamoDbClient: svc, 
		TableName: tablename,
		newHash: func() hash.Hash { return xxhash.New() },
	}

	exists, _ := nanoTable.TableExists()

	if !exists {

		attrs := []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		}

		schema := []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       types.KeyTypeRange,
			},
		}

		CreateTable(svc, nanoTable.TableName, attrs, schema)
		nanoTable.Wait()
	}

	return &nanoTable, nil
}

// TableExists determines whether a DynamoDB table exists.
func (dsynamotable DSDynamoTable) TableExists() (bool, error) {
	exists := true
	_, err := dsynamotable.DynamoDbClient.DescribeTable(
		context.TODO(), &dynamodb.DescribeTableInput{TableName: aws.String(dsynamotable.TableName)},
	)
	if err != nil {
		exists = false
	}
	return exists, err
}

// ListTables lists the DynamoDB table names for the current account.
func (dsynamotable DSDynamoTable) ListTables() ([]string, error) {
	var tableNames []string
	tables, err := dsynamotable.DynamoDbClient.ListTables(
		context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	} else {
		tableNames = tables.TableNames
	}
	return tableNames, err
}

func (dsdynamoTable DSDynamoTable) Wait() error {
	w := dynamodb.NewTableExistsWaiter(dsdynamoTable.DynamoDbClient)
	err := w.Wait(context.TODO(),
		&dynamodb.DescribeTableInput{
			TableName: aws.String(dsdynamoTable.TableName),
		},
		2*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 5 * time.Second
			o.MinDelay = 5 * time.Second
		})

	return err
}

func (dsdynamoTable DSDynamoTable) AddItem(item interface{}) error {
	newitem, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}
	_, err = dsdynamoTable.DynamoDbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(dsdynamoTable.TableName), Item: newitem,
	})
	if err != nil {
		return err
	}
	return err
}

func (s DSDynamoTable) deleteSingleItem(item interface{}) bool {
	deleteItem, err := attributevalue.MarshalMap(item)

	if err != nil {
		return false
	}

	_, err = s.DynamoDbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableName),
		Key:       deleteItem,
	})

	if err != nil {
		return false
	}
	return true
}

func (dsdynamoTable DSDynamoTable) GetSingleItemPKSK(pk, sk string, result interface{}) error {
	out, err := dsdynamoTable.DynamoDbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(dsdynamoTable.TableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
			"sk": &types.AttributeValueMemberS{Value: sk},
		},
	})

	if err != nil {
		return err
	} else {
		err = attributevalue.UnmarshalMap(out.Item, result)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *DSDynamoTable) GetReverseItems(sk, beginsWith string, result interface{}) error {

	response, err := s.DynamoDbClient.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(s.TableName),
		KeyConditionExpression: aws.String("#DDB_pk = :pkey AND begins_with(#DDB_sk, :begin)"),
		IndexName:              aws.String("reverse-sk-pk-index"),
		//FilterExpression: aws.String("begins_with(#DDB_sk, :begin)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pkey": &types.AttributeValueMemberS{Value: sk},
			":begin": &types.AttributeValueMemberS{Value: beginsWith},
		},
		ExpressionAttributeNames: map[string]string{
			"#DDB_pk": "sk",
			"#DDB_sk": "pk",
		},
	})

	if err != nil {
		return err
	} else {
		err = attributevalue.UnmarshalListOfMaps(response.Items, result)
		if err != nil {
			return err
		}
	}

	return err
}

func (s *DSDynamoTable) GetItemsSKBeginsWith(pk, beginsWith string, result interface{}) error {

	response, err := s.DynamoDbClient.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(s.TableName),
		KeyConditionExpression: aws.String("#DDB_pk = :pkey AND begins_with(#DDB_sk, :begin)" ),
		//FilterExpression: aws.String("begins_with(#DDB_sk, :begin)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pkey": &types.AttributeValueMemberS{Value: pk},
			":begin": &types.AttributeValueMemberS{Value: beginsWith},
		},
		ExpressionAttributeNames: map[string]string{
			"#DDB_pk": "pk",
			"#DDB_sk": "sk",
		},
	})

	if err != nil {
		return err
	} else {
		err = attributevalue.UnmarshalListOfMaps(response.Items, result)
		if err != nil {
			return err
		}
	}
	
	return err
}