//go:generate mockgen -source=aws.go -destination=mock_aws.go
package cavalier

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type RDSClient interface {
	rds.DescribeDBSnapshotsAPIClient
	rds.DescribeDBInstancesAPIClient

	DeleteDBInstance(context.Context, *rds.DeleteDBInstanceInput, ...func(*rds.Options)) (*rds.DeleteDBInstanceOutput, error)
	DeleteDBSnapshot(context.Context, *rds.DeleteDBSnapshotInput, ...func(*rds.Options)) (*rds.DeleteDBSnapshotOutput, error)
	CreateDBSnapshot(context.Context, *rds.CreateDBSnapshotInput, ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error)
	RestoreDBInstanceFromDBSnapshot(context.Context, *rds.RestoreDBInstanceFromDBSnapshotInput, ...func(*rds.Options)) (*rds.RestoreDBInstanceFromDBSnapshotOutput, error)
	ModifyDBInstance(context.Context, *rds.ModifyDBInstanceInput, ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error)
}

type SecretsManagerClient interface {
	CreateSecret(context.Context, *secretsmanager.CreateSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	DeleteSecret(context.Context, *secretsmanager.DeleteSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error)
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}
