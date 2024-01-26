package toyRetention

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// block created one month ago
var blockCreationTime = theCurrentTime - 30*secondsInADay

func TestApplyBucketRetention(t *testing.T) {
	bucket := &Bucket{
		Blocks: []Block{
			{
				MaxT:     blockCreationTime,
				Retained: 0,
			}},
	}
	/*
		The policy setting is:
			6m Policy: service=h1
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("Run apply bucket retention at 1m time, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h1"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h2
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("6m policy modified at 1m time, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h2"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("6m policy deleted at 1m time, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h1
			13m Policy: default retention
			2y Policy: namespace=b2
			3y Policy: name=ying
	*/
	t.Run("2y policy modified at 1m time, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h1"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b2"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h1
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("6m policy expired at 6m time, add to drop policy", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h1"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(6*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 1, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])

		// rewrite
		assert.Equal(t, 1, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h2
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("6m policy modified at 8m time, add to drop policy", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h2"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+8*30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 2, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])

		// rewrite
		assert.Equal(t, 2, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("6m policy get deleted at 8m time, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+8*30*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 2, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])
		// no rewrite
		assert.Equal(t, 2, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h1
			13m Policy: default retention
			2y Policy: namespace=b2
			3y Policy: name=ying
	*/
	t.Run("default retention policy reached, start to write keep policy", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h1"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(13*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 2, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 1, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.Blocks[0].MetaData.KeepPolicies[0])

		// rewrite
		assert.Equal(t, 3, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h3
			13m Policy: default retention
			2y Policy: namespace=b2
			3y Policy: name=ying
	*/
	t.Run("6m policy changed at 14m time, add to drop policy", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b1"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 1, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.Blocks[0].MetaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.Blocks[0].MetaData.KeepPolicies[0])

		// rewrite
		assert.Equal(t, 4, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h3
			13m Policy: default retention
			2y Policy: namespace=b1
			3y Policy: name=ying
	*/
	t.Run("2y policy modified at 14m time, append current keep labels to keep policy list", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b2"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 2, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.Blocks[0].MetaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.Blocks[0].MetaData.KeepPolicies[0])
		assert.Equal(t, hashPolicy("name=ying;namespace=b2"), bucket.Blocks[0].MetaData.KeepPolicies[1])

		// rewrite
		assert.Equal(t, 5, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h3
			13m Policy: default retention
			3y Policy: name=ying
	*/
	t.Run("2y policy policy is deleted at 14m time, append current keep lables to keep policy list", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},     // ~= 6 months in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"}, // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.Blocks[0].MetaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.Blocks[0].MetaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.Blocks[0].MetaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.Blocks[0].MetaData.KeepPolicies[0])
		assert.Equal(t, hashPolicy("name=ying;namespace=b2"), bucket.Blocks[0].MetaData.KeepPolicies[1])
		assert.Equal(t, hashPolicy("name=ying"), bucket.Blocks[0].MetaData.KeepPolicies[2])

		// rewrite
		assert.Equal(t, 6, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h3
			2y Policy: namespace=b2
			35m Policy: default retention
			3y Policy: name=ying
	*/
	t.Run("default retention policy is changed to 35m at 25m time, drop policy would be updated", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b2"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(25*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 4, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.KeepPolicies))
		assert.Equal(t, hashPolicy("namespace=b2"), bucket.Blocks[0].MetaData.DropPolicies[3])

		// rewrite
		assert.Equal(t, 7, bucket.Blocks[0].Retained)
	})

	/*
		The policy setting is:
			6m Policy: service=h2
			2y Policy: namespace=b2
			35m Policy: default retention
			3y Policy: name=ying
	*/
	t.Run("when reach 35m time, since the keep policy is not changed, noop", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b2"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(35*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, false, bucket.Blocks[0].Deleted)
		assert.Equal(t, 4, len(bucket.Blocks[0].MetaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.Blocks[0].MetaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 7, bucket.Blocks[0].Retained)
	})

	t.Run("when reached 3y time, all Policies reached retention, block deleted", func(t *testing.T) {
		config := UserConfig{
			BaseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			Policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 6 * 30 * secondsInADay, Policy: "service=h3"},        // ~= 6 months in seconds
				{RetentionPeriod: 2 * 12 * 30 * secondsInADay, Policy: "namespace=b2"}, // ~= 2 years in seconds
				{RetentionPeriod: 3 * 12 * 30 * secondsInADay, Policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(3*12*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.Blocks))
		assert.Equal(t, true, bucket.Blocks[0].Deleted)
	})

}
