package toyRetention

import (
	"strings"
)

type block struct {
	id     int
	series map[string]interface{}
	minT   int64
	maxT   int64
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

// Returns true if the maxTime is after the threshold and would be expired
func checkBlockRetention(maxT int64, threshold int64) bool {
	// if max t is before at threshold time
	if maxT <= threshold {
		return true
	}
	return false
}

func ApplyBucketRetention(policies userConfig, userBucket *bucket, currentTime int64) {
	for i, b := range userBucket.blocks {
		if !checkBlockRetention(b.maxT, currentTime-policies.baseRetention) {
			return
		} else {
			userBucket.blocks[i] = applyPolicy(buildPolicy(b.maxT, policies, currentTime), b)
		}
	}

}

func buildPolicy(blockMaxTime int64, config userConfig, currentTime int64) []perSeriesRetentionPolicy {
	// Figure out which policy matches
	// If the time is explicitly after the threshold apply that config
	var toApply []perSeriesRetentionPolicy
	for _, p := range config.policies {
		// You want the inverse, so any poilcy that is still valid we would want to apply
		if !checkBlockRetention(blockMaxTime, currentTime-p.retentionPeriod) {
			toApply = append(toApply, p)
		}
	}
	return toApply
}

func applyPolicy(toApply []perSeriesRetentionPolicy, b block) block {
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
	return resultBlock
}
