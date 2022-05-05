package cavalier

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleTerminate(t *testing.T) {
	ccfg := &Config{
		DBInstanceIdentifier: "test",
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
