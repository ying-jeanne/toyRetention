package toyRetention

import (
	"encoding/base64"
	"sort"
	"strings"
)

func getRetentionPeriodRange(policies []PerSeriesRetentionPolicy, baseRetention int64) (int64, int64) {
	minRetention, maxRetention := baseRetention, baseRetention
	for _, p := range policies {
		if p.RetentionPeriod < minRetention {
			minRetention = p.RetentionPeriod
		}
		if p.RetentionPeriod > maxRetention {
			maxRetention = p.RetentionPeriod
		}
	}
	return minRetention, maxRetention
}

// Returns true if the maxTime is outside the retention threshold and would be expired
func isBlockRetentionPassed(maxT int64, currentTime int64, retentionTier int64) bool {
	// if max t is before threshold time return true
	return maxT+retentionTier <= currentTime
}

func hashPolicy(policy string) string {
	return base64.StdEncoding.EncodeToString([]byte(policy))
}

func buildKeepPolicy(policies []PerSeriesRetentionPolicy, baseRetention int64, currentTime int64, maxT int64) []string {
	keepPolicies := []string{}
	// When base retention is not reached, we don't need to build keep policies, only drop policy counts.
	if !isBlockRetentionPassed(maxT, currentTime, baseRetention) {
		return keepPolicies
	}
	for _, p := range policies {
		if p.RetentionPeriod > baseRetention && !isBlockRetentionPassed(maxT, currentTime, p.RetentionPeriod) {
			keepPolicies = append(keepPolicies, p.Policy)
		}
	}
	// keep it in order
	sort.Slice(keepPolicies, func(i, j int) bool {
		return keepPolicies[i] < keepPolicies[j]
	})
	return keepPolicies
}

func buildDropPolicy(policies []PerSeriesRetentionPolicy, baseRetention int64, currentTime int64, maxT int64) []string {
	dropPolicies := []string{}
	for _, p := range policies {
		if p.RetentionPeriod <= baseRetention && isBlockRetentionPassed(maxT, currentTime, p.RetentionPeriod) {
			dropPolicies = append(dropPolicies, p.Policy)
		}
	}
	return dropPolicies
}

func isKeepPoliciesSame(keepPolicyHistory []string, keepPolicy []string) bool {
	if len(keepPolicyHistory) == 0 {
		return len(keepPolicy) == 0
	}
	if keepPolicyHistory[len(keepPolicyHistory)-1] != hashPolicy(strings.Join(keepPolicy, ";")) {
		return false
	}
	return true
}

func needsRewrite(dropPolicies []string, keepPolicies []string, b Block, currentTime int64, baseRetention int64) (bool, bool, bool) {
	// when base retention passed, we also need to consider keep policies
	rewriteKeepPolicy := false
	rewriteDropPolicy := false
	if isBlockRetentionPassed(b.MaxT, currentTime, baseRetention) {
		if len(keepPolicies) == 0 {
			return true, false, false
		}
		if !isKeepPoliciesSame(b.MetaData.KeepPolicies, keepPolicies) {
			rewriteKeepPolicy = true
		}
	}

	// always need to consider drop policies
	for _, dp := range dropPolicies {
		exist := false
		for _, dph := range b.MetaData.DropPolicies {
			if dph == hashPolicy(dp) {
				exist = true
				break
			}
		}
		if !exist {
			rewriteDropPolicy = true
		}
	}
	return false, rewriteKeepPolicy, rewriteDropPolicy
}
