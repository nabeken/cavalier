package cavalier

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleTerminate(t *testing.T) {
	ccfg := &Config{
		DBInstanceIdentifier: "test",
		SecretsManagerPrefix: "secrets-prefix",
	}

	type testCase struct {
		desc string
		tf   func(t *testing.T, ctx context.Context, rdsc *MockRDSClient, smc *MockSecretsManagerClient)
	}

	testCases := []testCase{
		{
			desc: "Error/DescribeDBInstances",
			tf: func(t *testing.T, ctx context.Context, rdsc *MockRDSClient, smc *MockSecretsManagerClient) {
				require := require.New(t)

				given := errors.New("describe db instances errors")

				rdsc.EXPECT().DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("test"),
				}).Return(
					nil, given,
				)

				cv := New(ccfg, rdsc, smc)

				actual := cv.HandleTerminate(ctx)

				require.ErrorIs(actual, given)
			},
		},
		{
			desc: "Error/DescribeDBInstances/NotFound",
			tf: func(t *testing.T, ctx context.Context, rdsc *MockRDSClient, smc *MockSecretsManagerClient) {
				require := require.New(t)

				given := &types.DBInstanceNotFoundFault{
					Message: aws.String("DB Instance Not Found"),
				}

				rdsc.EXPECT().DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("test"),
				}).Return(
					nil, given,
				)

				// will try to remove the secret
				smc.EXPECT().DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
					SecretId:                   aws.String("secrets-prefix/test"),
					ForceDeleteWithoutRecovery: true,
				}).Return(nil, nil)

				// will try to remove the corresponding DB snapshot
				rdsc.EXPECT().DescribeDBSnapshots(ctx, &rds.DescribeDBSnapshotsInput{
					DBSnapshotIdentifier: aws.String("test-cavalier"),
					SnapshotType:         aws.String("manual"),
				}).Return(&rds.DescribeDBSnapshotsOutput{
					DBSnapshots: []types.DBSnapshot{
						{
							DBSnapshotIdentifier: aws.String("test-cavalier"),
						},
					},
				}, nil)

				cv := New(ccfg, rdsc, smc)

				actual := cv.HandleTerminate(ctx)
				require.Nil(actual)
			},
		},
		{
			desc: "OK/DescribeDBInstances/DBInstanceFoundButCreatedbyCavalier",
			tf: func(t *testing.T, ctx context.Context, rdsc *MockRDSClient, smc *MockSecretsManagerClient) {
				require := require.New(t)

				rdsc.EXPECT().DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("test"),
				}).Return(
					&rds.DescribeDBInstancesOutput{
						DBInstances: []types.DBInstance{
							{
								DBInstanceStatus: aws.String("test"),
							},
						},
					}, nil,
				)

				cv := New(ccfg, rdsc, smc)

				actual := cv.HandleTerminate(ctx)
				require.ErrorIs(actual, errDBNotCreatedByCavalier)
			},
		},
		{
			desc: "OK/DescribeDBInstances/DBInstanceAndSnapshotFoundCreatedbyCavalier",
			tf: func(t *testing.T, ctx context.Context, rdsc *MockRDSClient, smc *MockSecretsManagerClient) {
				require := require.New(t)

				rdsc.EXPECT().DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
					DBInstanceIdentifier: aws.String("test"),
				}).Return(
					&rds.DescribeDBInstancesOutput{
						DBInstances: []types.DBInstance{
							{
								DBInstanceStatus: aws.String("test"),
								TagList: []types.Tag{
									{
										Key:   aws.String("CREATED_BY_CAVALIER"),
										Value: aws.String("true"),
									},
								},
							},
						},
					}, nil,
				)

				rdsc.EXPECT().DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
					DBInstanceIdentifier:   aws.String("test"),
					DeleteAutomatedBackups: aws.Bool(true),
					SkipFinalSnapshot:      true,
				}).Return(
					nil, nil,
				)

				rdsc.EXPECT().DescribeDBInstances(
					gomock.Any(),
					&rds.DescribeDBInstancesInput{
						DBInstanceIdentifier: aws.String("test"),
					},
					gomock.Any(), // via waiter
				).Return(
					nil, &types.DBInstanceNotFoundFault{
						Message: aws.String("DB Instance Not Found"),
					},
				)

				// will try to remove the secret
				smc.EXPECT().DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
					SecretId:                   aws.String("secrets-prefix/test"),
					ForceDeleteWithoutRecovery: true,
				}).Return(nil, nil)

				// will try to remove the corresponding DB snapshot
				rdsc.EXPECT().DescribeDBSnapshots(ctx, &rds.DescribeDBSnapshotsInput{
					DBSnapshotIdentifier: aws.String("test-cavalier"),
					SnapshotType:         aws.String("manual"),
				}).Return(&rds.DescribeDBSnapshotsOutput{
					DBSnapshots: []types.DBSnapshot{
						{
							DBSnapshotIdentifier: aws.String("test-cavalier"),
							TagList: []types.Tag{
								{
									Key: aws.String(tagCavalierDBInstanceID),
									// TODO: test for non-matching
									Value: aws.String("test"),
								},
							},
						},
					},
				}, nil)

				rdsc.EXPECT().DeleteDBSnapshot(ctx, &rds.DeleteDBSnapshotInput{
					DBSnapshotIdentifier: aws.String("test-cavalier"),
				}).Return(
					nil, nil,
				)

				cv := New(ccfg, rdsc, smc)

				actual := cv.HandleTerminate(ctx)
				require.Nil(actual)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mc := gomock.NewController(t)

			tc.tf(
				t,
				context.Background(),
				NewMockRDSClient(mc),
				NewMockSecretsManagerClient(mc),
			)
		})
	}
}
