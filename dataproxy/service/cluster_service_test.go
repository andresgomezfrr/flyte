package service

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/flyteorg/flyte/v2/dataproxy/config"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/cluster"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/common"
)

func TestSelectCluster_PassthroughMode(t *testing.T) {
	svc := NewClusterService()

	tests := []struct {
		name     string
		host     string
		fwdProto string
		want     string
	}{
		{name: "https via forwarded proto", host: "app.example.com", fwdProto: "https", want: "https://app.example.com"},
		{name: "direct http (no forwarded proto)", host: "localhost:8090", want: "http://localhost:8090"},
		{name: "explicit http forwarded proto", host: "localhost:8090", fwdProto: "http", want: "http://localhost:8090"},
		{name: "takes first of proxy-chain list", host: "app.example.com", fwdProto: "https, http", want: "https://app.example.com"},
		{name: "normalizes uppercase forwarded proto", host: "app.example.com", fwdProto: "HTTPS", want: "https://app.example.com"},
		{name: "blank forwarded proto defaults to http", host: "localhost:8090", fwdProto: "  ", want: "http://localhost:8090"},
		{name: "empty host yields empty endpoint", host: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := connect.NewRequest(&cluster.SelectClusterRequest{})
			if tt.host != "" {
				req.Header().Set("Host", tt.host)
			}
			if tt.fwdProto != "" {
				req.Header().Set("X-Forwarded-Proto", tt.fwdProto)
			}
			resp, err := svc.SelectCluster(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, resp.Msg.GetClusterEndpoint())
			assert.Empty(t, resp.Msg.GetCluster(), "passthrough mode should not set cluster name")
		})
	}
}

func testClusterConfig() config.ClusterConfig {
	return config.ClusterConfig{
		Clusters: []config.ClusterEntry{
			{Name: "cluster-a", Endpoint: "https://a.example.com", Enabled: true, Weight: 0.5},
			{Name: "cluster-b", Endpoint: "https://b.example.com", Enabled: true, Weight: 0.5},
			{Name: "cluster-c", Endpoint: "https://c.example.com", Enabled: true, Weight: 0.3},
			{Name: "cluster-disabled", Endpoint: "https://disabled.example.com", Enabled: false, Weight: 1.0},
		},
		ClusterPoolAssignments: map[string][]config.PoolMember{
			"pool-primary": {
				{Name: "cluster-a", Weight: 0.7},
				{Name: "cluster-b", Weight: 0.3},
			},
			"pool-secondary": {
				{Name: "cluster-c", Weight: 1.0},
			},
		},
		DefaultClusterPool: "pool-primary",
	}
}

func TestNewClusterServiceFromConfig(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)
	assert.Len(t, svc.clustersByName, 3, "disabled cluster should be excluded")
	assert.Len(t, svc.poolSelectors, 2)
	assert.NotNil(t, svc.allSelector)
	assert.Equal(t, "pool-primary", svc.defaultPool)
}

func TestNewClusterServiceFromConfig_InvalidPoolReference(t *testing.T) {
	ctx := context.Background()
	cfg := config.ClusterConfig{
		Clusters: []config.ClusterEntry{
			{Name: "cluster-a", Endpoint: "https://a.example.com", Enabled: true, Weight: 1.0},
		},
		ClusterPoolAssignments: map[string][]config.PoolMember{
			"bad-pool": {
				{Name: "nonexistent", Weight: 1.0},
			},
		},
	}
	_, err := NewClusterServiceFromConfig(ctx, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestSelectCluster_DirectClusterLookup(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterId{
			ClusterId: &common.ClusterIdentifier{Name: "cluster-b"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})
	resp, err := svc.SelectCluster(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "https://b.example.com", resp.Msg.GetClusterEndpoint())
	assert.Equal(t, "cluster-b", resp.Msg.GetCluster())
}

func TestSelectCluster_DirectClusterNotFound(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterId{
			ClusterId: &common.ClusterIdentifier{Name: "nonexistent"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})
	_, err = svc.SelectCluster(ctx, req)
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestSelectCluster_DisabledClusterNotSelectable(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterId{
			ClusterId: &common.ClusterIdentifier{Name: "cluster-disabled"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})
	_, err = svc.SelectCluster(ctx, req)
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestSelectCluster_PoolRouting(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterPoolId{
			ClusterPoolId: &common.ClusterPoolIdentifier{Name: "pool-secondary"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	for i := 0; i < 10; i++ {
		resp, err := svc.SelectCluster(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, "https://c.example.com", resp.Msg.GetClusterEndpoint())
		assert.Equal(t, "cluster-c", resp.Msg.GetCluster())
	}
}

func TestSelectCluster_DefaultPoolUsedForProjectRequest(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ProjectId{
			ProjectId: &common.ProjectIdentifier{
				Organization: "myorg",
				Domain:       "production",
				Name:         "myproject",
			},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		resp, err := svc.SelectCluster(ctx, req)
		require.NoError(t, err)
		name := resp.Msg.GetCluster()
		assert.Contains(t, []string{"cluster-a", "cluster-b"}, name)
		seen[name] = true
	}
	assert.True(t, seen["cluster-a"] || seen["cluster-b"],
		"should select from the default pool's clusters")
}

func TestSelectCluster_UnknownPoolFallsBackToCatchAll(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterPoolId{
			ClusterPoolId: &common.ClusterPoolIdentifier{Name: "nonexistent-pool"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	resp, err := svc.SelectCluster(ctx, req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Msg.GetClusterEndpoint())
	assert.Contains(t, []string{"cluster-a", "cluster-b", "cluster-c"}, resp.Msg.GetCluster())
}

func TestSelectCluster_WeightDistribution(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ClusterPoolId{
			ClusterPoolId: &common.ClusterPoolIdentifier{Name: "pool-primary"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	counts := map[string]int{}
	iterations := 1000
	for i := 0; i < iterations; i++ {
		resp, err := svc.SelectCluster(ctx, req)
		require.NoError(t, err)
		counts[resp.Msg.GetCluster()]++
	}

	ratioA := float64(counts["cluster-a"]) / float64(iterations)
	assert.InDelta(t, 0.7, ratioA, 0.1, "cluster-a should be selected ~70%% of the time, got %.2f", ratioA)
}

func TestSelectCluster_EmptyConfigPassthrough(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, config.ClusterConfig{})
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_ProjectId{
			ProjectId: &common.ProjectIdentifier{
				Organization: "myorg",
				Domain:       "production",
				Name:         "myproject",
			},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})
	req.Header().Set("Host", "localhost:8090")

	resp, err := svc.SelectCluster(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8090", resp.Msg.GetClusterEndpoint())
	assert.Empty(t, resp.Msg.GetCluster(), "empty config should use passthrough")
}
