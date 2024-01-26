package toyRetention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var theCurrentTime = time.Now().Unix()
var secondsInADay = int64(24 * time.Hour / time.Second)

func TestGetRetentionPeriodRange(t *testing.T) {
	policies := []PerSeriesRetentionPolicy{
		{RetentionPeriod: 10, Policy: "Policy1"},
		{RetentionPeriod: 20, Policy: "Policy2"},
	}
	baseRetention := int64(5)

	minRetention, maxRetention := getRetentionPeriodRange(policies, baseRetention)

	assert.Equal(t, int64(5), minRetention)
	assert.Equal(t, int64(20), maxRetention)
}

func TestIsBlockRetentionPassed(t *testing.T) {
	testCases := []struct {
		name          string
		maxT          int64
		currentTime   int64
		retentionTier int64
		expected      bool
	}{
		{
			name:          "The block is passed retention period",
			maxT:          theCurrentTime - 10*secondsInADay,
			currentTime:   theCurrentTime,
			retentionTier: 8 * secondsInADay,
			expected:      true,
		},
		{
			name:          "The block is not passed retention period",
			maxT:          theCurrentTime - 6*secondsInADay,
			currentTime:   theCurrentTime,
			retentionTier: 8 * secondsInADay,
			expected:      false,
		},
	}
	for _, tc := range testCases {
		passed := isBlockRetentionPassed(tc.maxT, tc.currentTime, tc.retentionTier)
		assert.Equal(t, tc.expected, passed, tc.name)
	}
}

func TestBuildKeepPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		policies      []PerSeriesRetentionPolicy
		baseRetention int64
		currentTime   int64
		maxT          int64
		expected      []string
	}{
		{
			name: "no keep Policy returned when current time is less than base retention",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 7 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 6*secondsInADay,
			expected:      []string{},
		},
		{
			name: "only the keep Policy that are not expired yet returned, when the base retention is passed",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 7 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 10*secondsInADay,
			expected:      []string{"Policy2"},
		},
		{
			name: "no keep Policy returned when all retention policies are expired",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 7 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 30*secondsInADay,
			expected:      []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			keepPolicy := buildKeepPolicy(tc.policies, tc.baseRetention, tc.currentTime, tc.maxT)
			assert.Equal(t, tc.expected, keepPolicy)
		})
	}
}

func TestBuildDropPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		policies      []PerSeriesRetentionPolicy
		baseRetention int64
		currentTime   int64
		maxT          int64
		expected      []string
	}{
		{
			name: "When no Policy is expired, no drop Policy returned",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 10 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 5*secondsInADay,
			expected:      []string{},
		},
		{
			name: "When the Policy is shorter than base retention expired, drop Policy returned",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 10 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 15*secondsInADay,
			expected:      []string{"Policy1"},
		},
		{
			name: "When all polcies expried, only policies shorter than base retention returned in drop policies list",
			policies: []PerSeriesRetentionPolicy{
				{RetentionPeriod: 8 * secondsInADay, Policy: "Policy1"},
				{RetentionPeriod: 20 * secondsInADay, Policy: "Policy2"},
			},
			baseRetention: 10 * secondsInADay,
			currentTime:   theCurrentTime,
			maxT:          theCurrentTime - 30*secondsInADay,
			expected:      []string{"Policy1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dropPolicies := buildDropPolicy(tc.policies, tc.baseRetention, tc.currentTime, tc.maxT)
			assert.Equal(t, tc.expected, dropPolicies)
		})
	}
}

func TestIsKeepPoliciesSame(t *testing.T) {
	testCases := []struct {
		testName          string
		keepPolicyHistory []string
		keepPolicy        []string
		expected          bool
	}{
		{
			testName:          "Empty Keep Policy History and Empty Keep Policy",
			keepPolicyHistory: []string{},
			keepPolicy:        []string{},
			expected:          true,
		},
		{
			testName:          "Empty Keep Policy History and Non-empty Keep Policy",
			keepPolicyHistory: []string{},
			keepPolicy:        []string{"Policy1", "Policy2"},
			expected:          false,
		},
		{
			testName:          "Non-empty Keep Policy History and Empty Keep Policy",
			keepPolicyHistory: []string{"12345"},
			keepPolicy:        []string{},
			expected:          false,
		},
		{
			testName:          "Keep Policies Same",
			keepPolicyHistory: []string{hashPolicy("Policy1;Policy2")},
			keepPolicy:        []string{"Policy1", "Policy2"},
			expected:          true,
		},
		{
			testName:          "Keep Policies Different",
			keepPolicyHistory: []string{hashPolicy("Policy1;Policy2")},
			keepPolicy:        []string{"Policy1"},
			expected:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			result := isKeepPoliciesSame(tc.keepPolicyHistory, tc.keepPolicy)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestNeedsRewrite(t *testing.T) {
	testCases := []struct {
		name                string
		dropPolicies        []string
		keepPolicies        []string
		metaData            MetaData
		currentTime         int64
		baseRetention       int64
		blockMaxT           int64
		expectedToDelete    bool
		expectedRewriteKeep bool
		expectedRewriteDrop bool
	}{
		{
			name:                "Default retention Passed, but keep policies are empty, block needs to be deleted, no rewrite needed",
			dropPolicies:        []string{"dropPolicy1", "dropPolicy2"},
			keepPolicies:        []string{},
			metaData:            MetaData{DropPolicies: []string{hashPolicy("dropPolicy2")}},
			currentTime:         theCurrentTime,
			baseRetention:       10 * secondsInADay,
			blockMaxT:           theCurrentTime - 15*secondsInADay,
			expectedToDelete:    true,
			expectedRewriteKeep: false,
			expectedRewriteDrop: false,
		},
		{
			name:                "Default retention not passed, one new Policy (shorter than default) expired, drop policies needs to be rewritten",
			dropPolicies:        []string{"dropPolicy1"},
			keepPolicies:        []string{},
			metaData:            MetaData{DropPolicies: []string{}, KeepPolicies: []string{}},
			currentTime:         theCurrentTime,
			baseRetention:       10 * secondsInADay,
			blockMaxT:           theCurrentTime - 8*secondsInADay,
			expectedToDelete:    false,
			expectedRewriteKeep: false,
			expectedRewriteDrop: true,
		},
		{
			name:                "Default retention not passed, one Policy applied is modified (shorter than default), drop policies needs to be rewritten",
			dropPolicies:        []string{"dropPolicy2"},
			keepPolicies:        []string{},
			metaData:            MetaData{DropPolicies: []string{hashPolicy("dropPolicy1")}, KeepPolicies: []string{}},
			currentTime:         theCurrentTime,
			baseRetention:       10 * secondsInADay,
			blockMaxT:           theCurrentTime - 8*secondsInADay,
			expectedToDelete:    false,
			expectedRewriteKeep: false,
			expectedRewriteDrop: true,
		},
		{
			name:                "Default retention passed, one new Policy (longer than default) expired, keep policies needs to be rewritten",
			dropPolicies:        []string{"dropPolicy1"},
			keepPolicies:        []string{"keepPolicy1"},
			metaData:            MetaData{DropPolicies: []string{hashPolicy("dropPolicy1"), hashPolicy("dropPolicy2")}, KeepPolicies: []string{hashPolicy("keepPolicy1; keepPolicy2")}},
			currentTime:         theCurrentTime,
			baseRetention:       10 * secondsInADay,
			blockMaxT:           theCurrentTime - 15*secondsInADay,
			expectedToDelete:    false,
			expectedRewriteKeep: true,
			expectedRewriteDrop: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Call the function with test input
			toDelete, toRewriteKeep, toRewriteDrop := needsRewrite(tc.dropPolicies, tc.keepPolicies, Block{MaxT: tc.blockMaxT, MetaData: tc.metaData}, tc.currentTime, tc.baseRetention)

			// Compare the result with expected output
			assert.Equal(t, tc.expectedToDelete, toDelete)
			assert.Equal(t, tc.expectedRewriteKeep, toRewriteKeep)
			assert.Equal(t, tc.expectedRewriteDrop, toRewriteDrop)
		})
	}
}
