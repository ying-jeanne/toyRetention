package toyRetention

import (
	"strings"
)

type block struct {
	id       int
	series   map[string]interface{}
	minT     int64
	maxT     int64
	retained int
	metaData MetaData
	deleted  bool
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

type MetaData struct {
	KeepPolicies []string
	DropPolicies []string
}

func ApplyBucketRetention(policies userConfig, userBucket *bucket, currentTime int64) {
	for i, b := range userBucket.blocks {
		// If the block is outside the min retention tier it will need to be checked and potentially rewrite
		minRetention, maxRetention := getRetentionPeriodRange(policies.policies, policies.baseRetention)
		if !isBlockRetentionPassed(b.maxT, currentTime, minRetention) {
			continue
		} else if isBlockRetentionPassed(b.maxT, currentTime, maxRetention) {
			// If the block is outside the max retention tier it will need to be dropped
			userBucket.blocks[i].deleted = true
		} else {
			// If the block is inside of the retention range, check the policy and rewrite when it is needed.
			dropPolicies, keepPolicies := buildPolicy(b, policies, currentTime)
			toBeDeleted, rewriteKeepPolicy, rewriteDropPolicy := needsRewrite(dropPolicies, keepPolicies, b, currentTime, policies.baseRetention)
			if toBeDeleted {
				userBucket.blocks[i].deleted = true
			}
			if rewriteKeepPolicy || rewriteDropPolicy {
				userBucket.blocks[i] = applyPolicy(dropPolicies, keepPolicies, rewriteKeepPolicy, rewriteDropPolicy, b)
			}
		}
	}
}

func buildPolicy(b block, config userConfig, currentTime int64) ([]string, []string) {
	// Figure out which policy matches
	// First check the base retention is already passed or not
	var dropPolicies, keepPolicy []string
	// Only when base retention is passed, we build keep policies

	keepPolicy = buildKeepPolicy(config.policies, config.baseRetention, currentTime, b.maxT)
	dropPolicies = buildDropPolicy(config.policies, config.baseRetention, currentTime, b.maxT)
	return dropPolicies, keepPolicy
}

func applyPolicy(dropPolicies []string, keepPolicies []string, rewriteKeepPolicy bool, rewriteDropPolicy bool, b block) block {
	resultBlock := b
	if rewriteDropPolicy {
		for _, dp := range dropPolicies {
			exist := false
			for _, dph := range b.metaData.DropPolicies {
				if dph == hashPolicy(dp) {
					exist = true
					continue
				}
			}
			if !exist {
				resultBlock.metaData.DropPolicies = append(resultBlock.metaData.DropPolicies, hashPolicy(dp))
			}
		}
	}

	if rewriteKeepPolicy {
		resultBlock.metaData.KeepPolicies = append(resultBlock.metaData.KeepPolicies, hashPolicy(strings.Join(keepPolicies, ";")))
	}

	resultBlock.retained = b.retained + 1
	return resultBlock
}
