package toyRetention

import (
	"hash/fnv"
	"strings"
)

type block struct {
	id            int
	series        map[string]interface{}
	minT          int64
	maxT          int64
	appliedPolicy uint64
	// Just for testing to see how many times the block was retained
	retained int
}

type bucket struct {
	blocks []block
}

type perSeriesRetentionPolicy struct {
	// retentionPeriod WILL ALWAYS be longer than the retention period before
	retentionPeriod int64
	// matching (contains) string
	policy string
}

type userConfig struct {
	baseRetention int64
	policies      []perSeriesRetentionPolicy
}

func ApplyBucketRetention(policies userConfig, userBucket *bucket, currentTime int64) {
	for i, b := range userBucket.blocks {
		// If the block is outside the default retention tier it will need to be retained in some way
		if !checkBlockRetention(b.maxT, currentTime-policies.baseRetention) {
			return
		} else {
			// Apply all valid policies to the block and update the block
			toApply := buildPolicy(b.maxT, policies, currentTime)
			policyHash := hashPolicy(toApply)
			if policyHash != b.appliedPolicy {
				userBucket.blocks[i] = applyPolicy(toApply, policyHash, b)
			}
		}
	}
}

// Returns true if the maxTime is outside the retention threshold and would be expired
func checkBlockRetention(maxT int64, threshold int64) bool {
	// if max t is before threshold time return true
	if maxT <= threshold {
		return true
	}
	return false
}

func buildPolicy(blockMaxTime int64, config userConfig, currentTime int64) []perSeriesRetentionPolicy {
	// Figure out which policy matches
	// If the time is explicitly after the threshold apply any config that is still valid
	var toApply []perSeriesRetentionPolicy
	for _, p := range config.policies {
		// You want the inverse, so any policy that is still valid we would want to apply
		if !checkBlockRetention(blockMaxTime, currentTime-p.retentionPeriod) {
			toApply = append(toApply, p)
		}
	}
	return toApply
}

func hashPolicy(policies []perSeriesRetentionPolicy) uint64 {
	hash := fnv.New64()
	for _, p := range policies {
		_, err := hash.Write([]byte(p.policy))
		if err != nil {
			panic(err)
		}
	}
	return hash.Sum64()
}

func applyPolicy(toApply []perSeriesRetentionPolicy, policyHash uint64, b block) block {
	resultBlock := block{minT: b.minT, maxT: b.maxT}
	resultSeries := make(map[string]interface{}, len(b.series))

	for _, p := range toApply {
		for s := range b.series {
			// Basic matchers check I'm too lazy for regex
			if strings.Contains(p.policy, s) {
				resultSeries[s] = struct{}{}
			}
		}
	}
	resultBlock.series = resultSeries
	resultBlock.appliedPolicy = policyHash
	resultBlock.retained = b.retained + 1
	return resultBlock
}
