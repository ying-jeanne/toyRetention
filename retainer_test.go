package toyRetention

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithinDefaultRetention(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}}}
	ApplyBucketRetention(p, &bucket, 2)
	assert.Equal(t, 3, len(bucket.blocks[0].series))
}

func TestOutsideDefaultRetentionOneTier(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
}

func TestOutsideDefaultRetentionTwoTiers(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 4, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	ApplyBucketRetention(p, &bucket, 4)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
}

func TestOutsideDefaultRetentionTwoTiersWithChange(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 4, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	p2 := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}}}
	ApplyBucketRetention(p2, &bucket, 4)
	assert.Equal(t, 0, len(bucket.blocks[0].series))
}

func TestOutsideDefaultRetentionOverlap(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// Make the blocks into a map probably
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1,series2"}, {retentionPeriod: 4, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
}

func TestRetentionTierMoveTime(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// Make the blocks into a map probably
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	p2 := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 4, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p2, &bucket, 4)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	ApplyBucketRetention(p2, &bucket, 5)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
}
