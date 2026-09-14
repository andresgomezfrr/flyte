package config

// ClusterConfig holds the multi-cluster routing configuration for the ClusterService.
type ClusterConfig struct {
	// Clusters is the list of known execution clusters.
	Clusters []ClusterEntry `json:"clusters" pflag:"-,List of execution clusters."`

	// ClusterPoolAssignments maps pool names to the clusters that belong to each pool.
	// Each pool entry lists cluster names with optional weight overrides.
	// When empty, all clusters are considered part of a single implicit default pool.
	ClusterPoolAssignments map[string][]PoolMember `json:"clusterPoolAssignments" pflag:"-,Map of pool names to cluster members with weights."`

	// DefaultClusterPool is the pool used when a request does not specify one.
	// If empty and ClusterPoolAssignments is also empty, the service falls back
	// to selecting from all configured clusters with equal weight.
	DefaultClusterPool string `json:"defaultClusterPool" pflag:",Default cluster pool name when none is specified."`
}

// ClusterEntry describes a single execution cluster.
type ClusterEntry struct {
	// Name is the unique identifier for this cluster.
	Name string `json:"name" pflag:",Unique cluster identifier."`

	// Endpoint is the base URL callers use to reach the cluster's data-plane
	// services (e.g. https://cluster1.example.com).
	Endpoint string `json:"endpoint" pflag:",Base URL of the cluster data-plane."`

	// Enabled controls whether this cluster is eligible for selection.
	// Disabled clusters are skipped during routing but kept in config
	// so they can be re-enabled without a config change.
	Enabled bool `json:"enabled" pflag:",Whether this cluster is eligible for selection."`

	// Weight is the default selection weight for this cluster (0.0–1.0).
	// A weight of 0 means the cluster is never selected unless overridden in a pool.
	// When all weights in a group are 0, clusters are selected with equal probability.
	Weight float32 `json:"weight" pflag:",Default selection weight (0.0-1.0)."`
}

// PoolMember associates a cluster with a weight override within a specific pool.
type PoolMember struct {
	// Name must match a ClusterEntry.Name.
	Name string `json:"name" pflag:",Cluster name (must match a ClusterEntry)."`

	// Weight overrides the cluster's default weight within this pool (0.0–1.0).
	// A value of 0 means the cluster is not selected in this pool.
	Weight float32 `json:"weight" pflag:",Weight override for this cluster in the pool (0.0-1.0)."`
}
