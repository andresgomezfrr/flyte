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
			req := connect.NewRequest(&cluster.SelectClusterRequest{
				Resource: &cluster.SelectClusterRequest_OrgId{
					OrgId: &common.OrgIdentifier{Name: "test"},
				},
				Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
			})
			if tt.host != "" {
				req.Header().Set("Host", tt.host)
			}
			if tt.fwdProto != "" {
				req.Header().Set("X-Forwarded-Proto", tt.fwdProto)
			}
			resp, err := svc.SelectCluster(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, resp.Msg.GetClusterEndpoint())
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

func TestSelectCluster_DefaultPoolRouting(t *testing.T) {
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

	// Default pool is pool-primary which has cluster-a and cluster-b.
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		resp, err := svc.SelectCluster(ctx, req)
		require.NoError(t, err)
		ep := resp.Msg.GetClusterEndpoint()
		assert.Contains(t, []string{"https://a.example.com", "https://b.example.com"}, ep)
		seen[ep] = true
	}
	assert.True(t, seen["https://a.example.com"] || seen["https://b.example.com"],
		"should select from the default pool's clusters")
}

func TestSelectCluster_WeightDistribution(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_OrgId{
			OrgId: &common.OrgIdentifier{Name: "test"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	counts := map[string]int{}
	iterations := 1000
	for i := 0; i < iterations; i++ {
		resp, err := svc.SelectCluster(ctx, req)
		require.NoError(t, err)
		counts[resp.Msg.GetClusterEndpoint()]++
	}

	// pool-primary has cluster-a (w:0.7) and cluster-b (w:0.3).
	ratioA := float64(counts["https://a.example.com"]) / float64(iterations)
	assert.InDelta(t, 0.7, ratioA, 0.1, "cluster-a should be selected ~70%% of the time, got %.2f", ratioA)
}

func TestSelectCluster_EmptyConfigPassthrough(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, config.ClusterConfig{})
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_OrgId{
			OrgId: &common.OrgIdentifier{Name: "test"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})
	req.Header().Set("Host", "localhost:8090")

	resp, err := svc.SelectCluster(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8090", resp.Msg.GetClusterEndpoint())
}

func TestSelectClusterByName(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	ep, err := svc.SelectClusterByName("cluster-b")
	require.NoError(t, err)
	assert.Equal(t, "https://b.example.com", ep)

	_, err = svc.SelectClusterByName("nonexistent")
	require.Error(t, err)

	_, err = svc.SelectClusterByName("cluster-disabled")
	require.Error(t, err)
}

func TestSelectClusterFromPool(t *testing.T) {
	ctx := context.Background()
	svc, err := NewClusterServiceFromConfig(ctx, testClusterConfig())
	require.NoError(t, err)

	// pool-secondary only has cluster-c.
	for i := 0; i < 10; i++ {
		ep, name, err := svc.SelectClusterFromPool(ctx, "pool-secondary")
		require.NoError(t, err)
		assert.Equal(t, "https://c.example.com", ep)
		assert.Equal(t, "cluster-c", name)
	}

	// Unknown pool falls back to catch-all.
	ep, name, err := svc.SelectClusterFromPool(ctx, "nonexistent-pool")
	require.NoError(t, err)
	assert.NotEmpty(t, ep)
	assert.Contains(t, []string{"cluster-a", "cluster-b", "cluster-c"}, name)
}

func TestSelectClusterFromPool_NoFallback(t *testing.T) {
	ctx := context.Background()
	cfg := config.ClusterConfig{
		Clusters: []config.ClusterEntry{
			{Name: "cluster-a", Endpoint: "https://a.example.com", Enabled: true, Weight: 1.0},
		},
		ClusterPoolAssignments: map[string][]config.PoolMember{
			"only-pool": {
				{Name: "cluster-a", Weight: 1.0},
			},
		},
	}
	svc, err := NewClusterServiceFromConfig(ctx, cfg)
	require.NoError(t, err)

	// Known pool works.
	ep, name, err := svc.SelectClusterFromPool(ctx, "only-pool")
	require.NoError(t, err)
	assert.Equal(t, "https://a.example.com", ep)
	assert.Equal(t, "cluster-a", name)
}

func TestSelectCluster_CatchAllWithoutDefaultPool(t *testing.T) {
	ctx := context.Background()
	cfg := config.ClusterConfig{
		Clusters: []config.ClusterEntry{
			{Name: "cluster-x", Endpoint: "https://x.example.com", Enabled: true, Weight: 1.0},
		},
	}
	svc, err := NewClusterServiceFromConfig(ctx, cfg)
	require.NoError(t, err)

	req := connect.NewRequest(&cluster.SelectClusterRequest{
		Resource: &cluster.SelectClusterRequest_OrgId{
			OrgId: &common.OrgIdentifier{Name: "test"},
		},
		Operation: cluster.SelectClusterRequest_OPERATION_CREATE_UPLOAD_LOCATION,
	})

	resp, err := svc.SelectCluster(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "https://x.example.com", resp.Msg.GetClusterEndpoint())
}
