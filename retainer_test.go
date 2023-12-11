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
	// Should only re-write when the block ages out of retention which won't happen
	assert.Equal(t, bucket.blocks[0].retained, 0)
}

func TestOutsideDefaultRetentionOneTier(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// policy = 1yr:{series1}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}}}
	// When retention is applied we will remove series 2 and 3
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 1)
}

func TestOutsideDefaultRetentionOneTierTwoBlocks(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	b2 := block{minT: 1, maxT: 2, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b, b2}}
	// policy = 1yr:{series1}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}}}
	// When retention is applied we will remove series 2 and 3
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 1)
	// Should not be re-written yet because it's still in retention
	assert.Equal(t, bucket.blocks[1].retained, 0)
	ApplyBucketRetention(p, &bucket, 4)
	// Now the other block should be re-written
	assert.Equal(t, 1, len(bucket.blocks[1].series))
	assert.Equal(t, bucket.blocks[1].retained, 1)
}

func TestOutsideDefaultRetentionTwoTiers(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 4, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	ApplyBucketRetention(p, &bucket, 4)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 2)

}

func TestOutsideDefaultRetentionTwoTiersMultiBlock(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	b2 := block{minT: 1, maxT: 2, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b, b2}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 4, policy: "series2"}}}

	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))

	ApplyBucketRetention(p, &bucket, 4)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 2)
	// Block 2 is behind block 1
	assert.Equal(t, 2, len(bucket.blocks[1].series))

	ApplyBucketRetention(p, &bucket, 5)
	assert.Equal(t, 1, len(bucket.blocks[1].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[1].retained, 2)

	ApplyBucketRetention(p, &bucket, 6)
	assert.Equal(t, 0, len(bucket.blocks[0].series))
	assert.Equal(t, 0, len(bucket.blocks[1].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[1].retained, 3)
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
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 2)
}

func TestOutsideDefaultRetentionOverlap(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// Make the blocks into a map probably
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1,series2"}, {retentionPeriod: 4, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 1)
}

func TestRetentionTierMoveTime(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	p2 := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 4, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p2, &bucket, 4)
	assert.Equal(t, 2, len(bucket.blocks[0].series))
	ApplyBucketRetention(p2, &bucket, 5)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	// Should only be re-written twice since policies are the same just different times
	assert.Equal(t, bucket.blocks[0].retained, 2)
}

func TestRetentionRewrites(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// Make the blocks into a map probably
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 3, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	for i := 1; i <= 5; i++ {
		ApplyBucketRetention(p, &bucket, int64(i))
	}
	// Should only re-write when the block ages out of retention
	assert.Equal(t, bucket.blocks[0].retained, 2)
}

func TestRetentionChangeFirstTierPolicy(t *testing.T) {
	b := block{minT: 0, maxT: 1, series: map[string]interface{}{"series1": struct{}{}, "series2": struct{}{}, "series3": struct{}{}}}
	bucket := bucket{blocks: []block{b}}
	// Make the blocks into a map probably
	p := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 4, policy: "series1"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p, &bucket, 3)
	assert.Equal(t, 2, len(bucket.blocks[0].series))

	p2 := userConfig{baseRetention: 2, policies: []perSeriesRetentionPolicy{{retentionPeriod: 4, policy: "series2"}, {retentionPeriod: 5, policy: "series2"}}}
	ApplyBucketRetention(p2, &bucket, 4)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	assert.Equal(t, 2, bucket.blocks[0].retained)

	ApplyBucketRetention(p2, &bucket, 5)
	assert.Equal(t, 1, len(bucket.blocks[0].series))
	assert.Equal(t, 3, bucket.blocks[0].retained)
}
