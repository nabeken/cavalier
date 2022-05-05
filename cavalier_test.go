package cavalier

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleTerminate(t *testing.T) {
	require := require.New(t)

	ccfg := &Config{}

	cv := newTestCavalier(t, ccfg)
	ctx := context.Background()

	actual := cv.HandleTerminate(ctx)

	require.NoError(actual)
}

func newTestCavalier(t *testing.T, ccfg *Config) *Cavalier {
	mc := gomock.NewController(t)

	return New(
		ccfg,
		NewMockRDSClient(mc),
		NewMockSecretsManagerClient(mc),
	)
}
