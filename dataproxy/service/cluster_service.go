package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"connectrpc.com/connect"

	"github.com/flyteorg/flyte/v2/dataproxy/config"
	"github.com/flyteorg/flyte/v2/flytestdlib/logger"
	"github.com/flyteorg/flyte/v2/flytestdlib/random"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/cluster"
	"github.com/flyteorg/flyte/v2/gen/go/flyteidl2/cluster/clusterconnect"
)

const (
	headerXForwardedProto = "X-Forwarded-Proto"
	schemeHTTP            = "http"
	schemeHTTPS           = "https"
)

// clusterTarget is the weighted-random-compatible wrapper for a cluster entry.
type clusterTarget struct {
	name     string
	endpoint string
}

func (c clusterTarget) Compare(to random.Comparable) bool {
	other, ok := to.(clusterTarget)
	if !ok {
		return false
	}
	return c.name < other.name
}

// ClusterService implements the ClusterService RPC with support for
// multi-cluster routing via weighted cluster pools.
//
// When no clusters are configured, it falls back to returning the
// requesting host (single-cluster / passthrough mode).
type ClusterService struct {
	clusterconnect.UnimplementedClusterServiceHandler

	mu sync.RWMutex
	// clustersByName indexes configured clusters by name for direct lookups.
	clustersByName map[string]config.ClusterEntry
	// poolSelectors holds the weighted random selector per pool name.
	poolSelectors map[string]random.WeightedRandomList
	// defaultPool is the pool name to use when the request doesn't specify one.
	defaultPool string
	// allSelector is the fallback when no pools are configured.
	allSelector random.WeightedRandomList
}

func NewClusterService() *ClusterService {
	return &ClusterService{}
}

// NewClusterServiceFromConfig builds a ClusterService pre-loaded with
// the given cluster configuration. Returns an error if the config is
// internally inconsistent (e.g. a pool references a cluster that doesn't exist).
func NewClusterServiceFromConfig(ctx context.Context, cfg config.ClusterConfig) (*ClusterService, error) {
	svc := &ClusterService{
		clustersByName: make(map[string]config.ClusterEntry),
		poolSelectors:  make(map[string]random.WeightedRandomList),
		defaultPool:    cfg.DefaultClusterPool,
	}

	for _, c := range cfg.Clusters {
		if !c.Enabled {
			logger.Infof(ctx, "Cluster %q is disabled, skipping", c.Name)
			continue
		}
		svc.clustersByName[c.Name] = c
	}

	if len(svc.clustersByName) == 0 {
		logger.Infof(ctx, "No clusters configured; ClusterService will run in passthrough mode")
		return svc, nil
	}

	// Build per-pool selectors.
	for poolName, members := range cfg.ClusterPoolAssignments {
		entries, err := svc.buildPoolEntries(members)
		if err != nil {
			return nil, fmt.Errorf("pool %q: %w", poolName, err)
		}
		if len(entries) == 0 {
			continue
		}
		sel, err := random.NewWeightedRandom(ctx, entries)
		if err != nil {
			return nil, fmt.Errorf("pool %q: building weighted selector: %w", poolName, err)
		}
		svc.poolSelectors[poolName] = sel
	}

	// Build the catch-all selector from all enabled clusters.
	var allEntries []random.Entry
	for _, c := range svc.clustersByName {
		allEntries = append(allEntries, random.Entry{
			Item:   clusterTarget{name: c.Name, endpoint: c.Endpoint},
			Weight: c.Weight,
		})
	}
	if len(allEntries) > 0 {
		sel, err := random.NewWeightedRandom(ctx, allEntries)
		if err != nil {
			return nil, fmt.Errorf("building catch-all selector: %w", err)
		}
		svc.allSelector = sel
	}

	logger.Infof(ctx, "ClusterService initialized with %d clusters and %d pools",
		len(svc.clustersByName), len(svc.poolSelectors))
	return svc, nil
}

func (svc *ClusterService) buildPoolEntries(members []config.PoolMember) ([]random.Entry, error) {
	var entries []random.Entry
	for _, m := range members {
		c, ok := svc.clustersByName[m.Name]
		if !ok {
			return nil, fmt.Errorf("cluster %q not found in cluster registry", m.Name)
		}
		entries = append(entries, random.Entry{
			Item:   clusterTarget{name: c.Name, endpoint: c.Endpoint},
			Weight: m.Weight,
		})
	}
	return entries, nil
}

var _ clusterconnect.ClusterServiceHandler = (*ClusterService)(nil)

func (svc *ClusterService) SelectCluster(
	ctx context.Context,
	req *connect.Request[cluster.SelectClusterRequest],
) (*connect.Response[cluster.SelectClusterResponse], error) {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	// Direct cluster lookup — caller already knows which cluster.
	if cid := req.Msg.GetClusterId(); cid != nil {
		return svc.selectByName(ctx, cid.GetName(), req)
	}

	// Pool-based routing.
	if cpid := req.Msg.GetClusterPoolId(); cpid != nil {
		return svc.selectFromPool(ctx, cpid.GetName(), req)
	}

	// For all other resource-based lookups (org, project, domain, task, action, app),
	// use the default pool if configured; otherwise fall through to the catch-all.
	if svc.defaultPool != "" {
		return svc.selectFromPool(ctx, svc.defaultPool, req)
	}

	// Catch-all: select from all enabled clusters.
	if svc.allSelector != nil {
		target := svc.allSelector.Get().(clusterTarget)
		logger.Debugf(ctx, "SelectCluster catch-all selected cluster %q", target.name)
		return connect.NewResponse(&cluster.SelectClusterResponse{
			ClusterEndpoint: target.endpoint,
			Cluster:         target.name,
		}), nil
	}

	// Passthrough mode — no clusters configured.
	return svc.passthrough(req)
}

func (svc *ClusterService) selectByName(
	ctx context.Context,
	name string,
	req *connect.Request[cluster.SelectClusterRequest],
) (*connect.Response[cluster.SelectClusterResponse], error) {
	c, ok := svc.clustersByName[name]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("cluster %q not found or not enabled", name))
	}
	logger.Debugf(ctx, "SelectCluster resolved direct cluster %q", name)
	return connect.NewResponse(&cluster.SelectClusterResponse{
		ClusterEndpoint: c.Endpoint,
		Cluster:         c.Name,
	}), nil
}

func (svc *ClusterService) selectFromPool(
	ctx context.Context,
	poolName string,
	req *connect.Request[cluster.SelectClusterRequest],
) (*connect.Response[cluster.SelectClusterResponse], error) {
	sel, ok := svc.poolSelectors[poolName]
	if !ok {
		// Pool not found — fall back to catch-all if available.
		if svc.allSelector != nil {
			logger.Warnf(ctx, "Pool %q not found, falling back to catch-all selector", poolName)
			target := svc.allSelector.Get().(clusterTarget)
			return connect.NewResponse(&cluster.SelectClusterResponse{
				ClusterEndpoint: target.endpoint,
				Cluster:         target.name,
			}), nil
		}
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("cluster pool %q not found and no fallback available", poolName))
	}

	target := sel.Get().(clusterTarget)
	logger.Debugf(ctx, "SelectCluster pool %q selected cluster %q", poolName, target.name)
	return connect.NewResponse(&cluster.SelectClusterResponse{
		ClusterEndpoint: target.endpoint,
		Cluster:         target.name,
	}), nil
}

// passthrough returns the requesting host as the endpoint (single-cluster mode).
func (svc *ClusterService) passthrough(
	req *connect.Request[cluster.SelectClusterRequest],
) (*connect.Response[cluster.SelectClusterResponse], error) {
	host := strings.TrimSpace(req.Header().Get("Host"))
	var endpoint string
	if host != "" {
		scheme := schemeHTTP
		if proto := req.Header().Get(headerXForwardedProto); proto != "" {
			candidate := strings.ToLower(strings.TrimSpace(strings.Split(proto, ",")[0]))
			if candidate == schemeHTTP || candidate == schemeHTTPS {
				scheme = candidate
			}
		}
		endpoint = scheme + "://" + host
	}
	return connect.NewResponse(&cluster.SelectClusterResponse{
		ClusterEndpoint: endpoint,
	}), nil
}
