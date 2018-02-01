package aws

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceAwsDynamoDbTableItem() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsDynamoDbTableItemCreate,
		Read:   resourceAwsDynamoDbTableItemRead,
		Update: resourceAwsDynamoDbTableItemUpdate,
		Delete: resourceAwsDynamoDbTableItemDelete,

		Schema: map[string]*schema.Schema{
			"table_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"hash_key": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"range_key": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"item": {
				Type:     schema.TypeString,
				Required: true,
			},

			"consumed_capacity": {
				Type:     schema.TypeFloat,
				Computed: true,
			},
			"last_modified": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceAwsDynamoDbTableItemCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).dynamodbconn

	tableName := d.Get("table_name").(string)
	hashKey := d.Get("hash_key").(string)
	item := d.Get("item").(string)
	attributes, err := expandDynamoDbTableItemAttributes(item)
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] DynamoDB item create: %s", tableName)

	_, err = retryDynamoDbTableItemOperation(func() (interface{}, error) {
		return conn.PutItem(&dynamodb.PutItemInput{
			Item: attributes,
			// Explode if item exists. We didn't create it.
			Expected: map[string]*dynamodb.ExpectedAttributeValue{
				hashKey: {
					Exists: aws.Bool(false),
				},
			},
			TableName: aws.String(tableName),
		})
	})
	if err != nil {
		return err
	}

	rangeKey := d.Get("range_key").(string)
	id := buildDynamoDbTableItemId(tableName, hashKey, rangeKey, attributes)

	d.SetId(id)

	return resourceAwsDynamoDbTableItemRead(d, meta)
}

func resourceAwsDynamoDbTableItemUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Updating DynamoDB table %s", d.Id())
	conn := meta.(*AWSClient).dynamodbconn

	if d.HasChange("item") {
		tableName := d.Get("table_name").(string)
		hashKey := d.Get("hash_key").(string)
		rangeKey := d.Get("range_key").(string)
		oldItem, newItem := d.GetChange("item")

		attributes, err := expandDynamoDbTableItemAttributes(newItem.(string))
		if err != nil {
			return err
		}
		newQueryKey := buildDynamoDbTableItemQueryKey(attributes, hashKey, rangeKey)

		updates := map[string]*dynamodb.AttributeValueUpdate{}
		for k, v := range attributes {
			// We shouldn't update the key values
			skip := false
			for qk := range newQueryKey {
				if skip = (qk == k); skip {
					break
				}
			}
			if skip {
				continue
			}

			updates[k] = &dynamodb.AttributeValueUpdate{
				Action: aws.String("PUT"),
				Value:  v,
			}
		}

		_, err = retryDynamoDbTableItemOperation(func() (interface{}, error) {
			return conn.UpdateItem(&dynamodb.UpdateItemInput{
				AttributeUpdates: updates,
				TableName:        aws.String(tableName),
				Key:              newQueryKey,
			})
		})

		if err != nil {
			return err
		}

		// If we finished successfully, delete the old record if the query key is different
		oItem := oldItem.(string)
		item, err := expandDynamoDbTableItemAttributes(oItem)
		if err != nil {
			return err
		}
		oldQueryKey := buildDynamoDbTableItemQueryKey(item, hashKey, rangeKey)
		if !reflect.DeepEqual(oldQueryKey, newQueryKey) {
			_, err := retryDynamoDbTableItemOperation(func() (interface{}, error) {
				return conn.DeleteItem(&dynamodb.DeleteItemInput{
					Key:       oldQueryKey,
					TableName: aws.String(tableName),
				})
			})
			if err != nil {
				return err
			}
		}

		id := buildDynamoDbTableItemId(tableName, hashKey, rangeKey, attributes)
		d.SetId(id)
	}

	return resourceAwsDynamoDbTableItemRead(d, meta)
}

func resourceAwsDynamoDbTableItemRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).dynamodbconn

	log.Printf("[DEBUG] Loading data for DynamoDB table item '%s'", d.Id())

	tableName := d.Get("table_name").(string)
	hashKey := d.Get("hash_key").(string)
	rangeKey := d.Get("range_key").(string)
	attributes, err := expandDynamoDbTableItemAttributes(d.Get("item").(string))
	if err != nil {
		return err
	}

	result, err := conn.GetItem(&dynamodb.GetItemInput{
		TableName:                aws.String(tableName),
		Key:                      buildDynamoDbTableItemQueryKey(attributes, hashKey, rangeKey),
		ProjectionExpression:     buildDynamoDbProjectionExpression(attributes),
		ExpressionAttributeNames: buildDynamoDbExpressionAttributeNames(attributes),
	})
	if err != nil {
		if isAWSErr(err, dynamodb.ErrCodeResourceNotFoundException, "") {
			log.Printf("[WARN] Dynamodb Table Item (%s) not found, error code (404)", d.Id())
			d.SetId("")
			return nil
		}

		return fmt.Errorf("Error retrieving DynamoDB table item: %s", err)
	}

	if result.Item == nil {
		log.Printf("[WARN] Dynamodb Table Item (%s) not found", d.Id())
		d.SetId("")
		return nil
	}

	// The record exists, now test if it differs from what is desired
	if !reflect.DeepEqual(result.Item, attributes) {
		itemAttrs, err := flattenDynamoDbTableItemAttributes(result.Item)
		if err != nil {
			return err
		}
		d.Set("item", itemAttrs)
		id := buildDynamoDbTableItemId(tableName, hashKey, rangeKey, result.Item)
		d.SetId(id)
	}

	d.Set("consumed_capacity", result.ConsumedCapacity)

	return nil
}

func resourceAwsDynamoDbTableItemDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).dynamodbconn

	attributes, err := expandDynamoDbTableItemAttributes(d.Get("item").(string))
	if err != nil {
		return err
	}
	hashKey := d.Get("hash_key").(string)
	rangeKey := d.Get("range_key").(string)
	queryKey := buildDynamoDbTableItemQueryKey(attributes, hashKey, rangeKey)

	_, err = retryDynamoDbTableItemOperation(func() (interface{}, error) {
		return conn.DeleteItem(&dynamodb.DeleteItemInput{
			Key:       queryKey,
			TableName: aws.String(d.Get("table_name").(string)),
		})
	})
	return err
}

// Helpers

func buildDynamoDbExpressionAttributeNames(attrs map[string]*dynamodb.AttributeValue) map[string]*string {
	names := map[string]*string{}
	for key, _ := range attrs {
		names["#a_"+key] = aws.String(key)
	}

	return names
}

func buildDynamoDbProjectionExpression(attrs map[string]*dynamodb.AttributeValue) *string {
	keys := []string{}
	for key, _ := range attrs {
		keys = append(keys, key)
	}
	return aws.String("#a_" + strings.Join(keys, ", #a_"))
}

func buildDynamoDbTableItemId(tableName string, hashKey string, rangeKey string, attrs map[string]*dynamodb.AttributeValue) string {
	hashVal := attrs[hashKey]

	id := []string{
		tableName,
		hashKey,
		base64Encode(hashVal.B),
	}

	if hashVal.S != nil {
		id = append(id, *hashVal.S)
	} else {
		id = append(id, "")
	}
	if hashVal.N != nil {
		id = append(id, *hashVal.N)
	} else {
		id = append(id, "")
	}
	if rangeKey != "" {
		rangeVal := attrs[rangeKey]

		id = append(id,
			rangeKey,
			base64Encode(rangeVal.B),
		)

		if rangeVal.S != nil {
			id = append(id, *rangeVal.S)
		} else {
			id = append(id, "")
		}

		if rangeVal.N != nil {
			id = append(id, *rangeVal.N)
		} else {
			id = append(id, "")
		}

	}

	return strings.Join(id, "|")
}

func buildDynamoDbTableItemQueryKey(attrs map[string]*dynamodb.AttributeValue, hashKey string, rangeKey string) map[string]*dynamodb.AttributeValue {
	queryKey := map[string]*dynamodb.AttributeValue{}

	flen := 1
	if rangeKey != "" {
		flen = 2
	}

	for k, value := range attrs {
		if k == hashKey || k == rangeKey {
			queryKey[k] = value
		}
		if len(queryKey) == flen {
			break
		}
	}

	return queryKey
}

func retryDynamoDbTableItemOperation(f func() (interface{}, error)) (interface{}, error) {
	var out interface{}
	rErr := resource.Retry(1*time.Minute, func() *resource.RetryError {
		var err error
		out, err = f()
		if err != nil {
			if isAWSErr(err, dynamodb.ErrCodeLimitExceededException, "Subscriber limit exceeded:") {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	return out, rErr
}
