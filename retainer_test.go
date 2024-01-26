package toyRetention

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// block created one month ago
var blockCreationTime = theCurrentTime - 30*secondsInADay

func TestApplyBucketRetention(t *testing.T) {
	bucket := &bucket{
		blocks: []block{
			{
				maxT:     blockCreationTime,
				retained: 0,
			}},
	}
	/*
		The policy setting is:
			6m policy: service=h1
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("Run apply bucket retention at 1m time, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h1"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h2
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("6m policy modified at 1m time, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h2"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("6m policy deleted at 1m time, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h1
			13m policy: default retention
			2y policy: namespace=b2
			3y policy: name=ying
	*/
	t.Run("2y policy modified at 1m time, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h1"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b2"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 0, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h1
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("6m policy expired at 6m time, add to drop policy", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h1"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(6*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 1, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])

		// rewrite
		assert.Equal(t, 1, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h2
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("6m policy modified at 8m time, add to drop policy", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h2"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+8*30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 2, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])

		// rewrite
		assert.Equal(t, 2, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("6m policy get deleted at 8m time, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+8*30*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 2, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 0, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])
		// no rewrite
		assert.Equal(t, 2, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h1
			13m policy: default retention
			2y policy: namespace=b2
			3y policy: name=ying
	*/
	t.Run("default retention policy reached, start to write keep policy", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h1"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(13*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 2, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 1, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.blocks[0].metaData.KeepPolicies[0])

		// rewrite
		assert.Equal(t, 3, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h3
			13m policy: default retention
			2y policy: namespace=b2
			3y policy: name=ying
	*/
	t.Run("6m policy changed at 14m time, add to drop policy", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b1"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 1, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.blocks[0].metaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.blocks[0].metaData.KeepPolicies[0])

		// rewrite
		assert.Equal(t, 4, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h3
			13m policy: default retention
			2y policy: namespace=b1
			3y policy: name=ying
	*/
	t.Run("2y policy modified at 14m time, append current keep labels to keep policy list", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b2"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 2, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.blocks[0].metaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.blocks[0].metaData.KeepPolicies[0])
		assert.Equal(t, hashPolicy("name=ying;namespace=b2"), bucket.blocks[0].metaData.KeepPolicies[1])

		// rewrite
		assert.Equal(t, 5, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h3
			13m policy: default retention
			3y policy: name=ying
	*/
	t.Run("2y policy policy is deleted at 14m time, append current keep lables to keep policy list", func(t *testing.T) {
		config := userConfig{
			baseRetention: 13 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},     // ~= 6 months in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"}, // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(14*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("service=h1"), bucket.blocks[0].metaData.DropPolicies[0])
		assert.Equal(t, hashPolicy("service=h2"), bucket.blocks[0].metaData.DropPolicies[1])
		assert.Equal(t, hashPolicy("service=h3"), bucket.blocks[0].metaData.DropPolicies[2])
		assert.Equal(t, hashPolicy("name=ying;namespace=b1"), bucket.blocks[0].metaData.KeepPolicies[0])
		assert.Equal(t, hashPolicy("name=ying;namespace=b2"), bucket.blocks[0].metaData.KeepPolicies[1])
		assert.Equal(t, hashPolicy("name=ying"), bucket.blocks[0].metaData.KeepPolicies[2])

		// rewrite
		assert.Equal(t, 6, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h3
			2y policy: namespace=b2
			35m policy: default retention
			3y policy: name=ying
	*/
	t.Run("default retention policy is changed to 35m at 25m time, drop policy would be updated", func(t *testing.T) {
		config := userConfig{
			baseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b2"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(25*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 4, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.KeepPolicies))
		assert.Equal(t, hashPolicy("namespace=b2"), bucket.blocks[0].metaData.DropPolicies[3])

		// rewrite
		assert.Equal(t, 7, bucket.blocks[0].retained)
	})

	/*
		The policy setting is:
			6m policy: service=h2
			2y policy: namespace=b2
			35m policy: default retention
			3y policy: name=ying
	*/
	t.Run("when reach 35m time, since the keep policy is not changed, noop", func(t *testing.T) {
		config := userConfig{
			baseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b2"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(35*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, false, bucket.blocks[0].deleted)
		assert.Equal(t, 4, len(bucket.blocks[0].metaData.DropPolicies))
		assert.Equal(t, 3, len(bucket.blocks[0].metaData.KeepPolicies))

		// no rewrite
		assert.Equal(t, 7, bucket.blocks[0].retained)
	})

	t.Run("when reached 3y time, all policies reached retention, block deleted", func(t *testing.T) {
		config := userConfig{
			baseRetention: 35 * 30 * secondsInADay, // ~= 13 months in seconds
			policies: []perSeriesRetentionPolicy{
				{retentionPeriod: 6 * 30 * secondsInADay, policy: "service=h3"},        // ~= 6 months in seconds
				{retentionPeriod: 2 * 12 * 30 * secondsInADay, policy: "namespace=b2"}, // ~= 2 years in seconds
				{retentionPeriod: 3 * 12 * 30 * secondsInADay, policy: "name=ying"},    // ~= 3 years in seconds
			},
		}
		ApplyBucketRetention(config, bucket, blockCreationTime+(3*12*30+1)*secondsInADay)
		assert.Equal(t, 1, len(bucket.blocks))
		assert.Equal(t, true, bucket.blocks[0].deleted)
	})

}
