package toyRetention

import (
	"strings"
)

type Block struct {
	ID       int
	Series   map[string]interface{}
	MinT     int64
	MaxT     int64
	Retained int
	MetaData MetaData
	Deleted  bool
}

type Bucket struct {
	Blocks []Block
}

type PerSeriesRetentionPolicy struct {
	RetentionPeriod int64
	Policy          string
}

type UserConfig struct {
	BaseRetention int64
	Policies      []PerSeriesRetentionPolicy
}

type MetaData struct {
	KeepPolicies []string
	DropPolicies []string
}

func ApplyBucketRetention(policies UserConfig, userBucket *Bucket, currentTime int64) {
	for i, b := range userBucket.Blocks {
		minRetention, maxRetention := getRetentionPeriodRange(policies.Policies, policies.BaseRetention)
		if !isBlockRetentionPassed(b.MaxT, currentTime, minRetention) {
			continue
		} else if isBlockRetentionPassed(b.MaxT, currentTime, maxRetention) {
			userBucket.Blocks[i].Deleted = true
		} else {
			dropPolicies, keepPolicies := buildPolicy(b, policies, currentTime)
			toBeDeleted, rewriteKeepPolicy, rewriteDropPolicy := needsRewrite(dropPolicies, keepPolicies, b, currentTime, policies.BaseRetention)
			if toBeDeleted {
				userBucket.Blocks[i].Deleted = true
			}
			if rewriteKeepPolicy || rewriteDropPolicy {
				userBucket.Blocks[i] = applyPolicy(dropPolicies, keepPolicies, rewriteKeepPolicy, rewriteDropPolicy, b)
			}
		}
	}
}

func buildPolicy(b Block, config UserConfig, currentTime int64) ([]string, []string) {
	keepPolicy := buildKeepPolicy(config.Policies, config.BaseRetention, currentTime, b.MaxT)
	dropPolicies := buildDropPolicy(config.Policies, config.BaseRetention, currentTime, b.MaxT)
	return dropPolicies, keepPolicy
}

func applyPolicy(dropPolicies []string, keepPolicies []string, rewriteKeepPolicy bool, rewriteDropPolicy bool, b Block) Block {
	if rewriteDropPolicy {
		for _, dp := range dropPolicies {
			exist := false
			for _, dph := range b.MetaData.DropPolicies {
				if dph == hashPolicy(dp) {
					exist = true
					break
				}
			}
			if !exist {
				b.MetaData.DropPolicies = append(b.MetaData.DropPolicies, hashPolicy(dp))
			}
		}
	}

	if rewriteKeepPolicy {
		b.MetaData.KeepPolicies = append(b.MetaData.KeepPolicies, hashPolicy(strings.Join(keepPolicies, ";")))
	}

	b.Retained++
	return b
}
