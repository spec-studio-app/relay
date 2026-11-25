// The search attributes of the spec workflows (Temporal store only).
import { defineSearchAttributeKey, SearchAttributeType } from "@temporalio/common";

export const SPEC_WORKFLOW_TYPE = "specWorkflow";
export const RELAY_CONFIG_WORKFLOW_TYPE = "relayConfigWorkflow";

export const SA = {
  RepoKey: defineSearchAttributeKey("RepoKey", SearchAttributeType.KEYWORD),
  SpecSlug: defineSearchAttributeKey("SpecSlug", SearchAttributeType.KEYWORD),
  SpecTitle: defineSearchAttributeKey("SpecTitle", SearchAttributeType.TEXT),
  Stage: defineSearchAttributeKey("Stage", SearchAttributeType.KEYWORD),
  Status: defineSearchAttributeKey("Status", SearchAttributeType.KEYWORD),
  Mode: defineSearchAttributeKey("Mode", SearchAttributeType.KEYWORD),
  NeedsYou: defineSearchAttributeKey("NeedsYou", SearchAttributeType.BOOL),
  PendingForMac: defineSearchAttributeKey("PendingForMac", SearchAttributeType.BOOL),
  PendingForCoordinator: defineSearchAttributeKey("PendingForCoordinator", SearchAttributeType.BOOL),
  RelayDeployId: defineSearchAttributeKey("RelayDeployId", SearchAttributeType.KEYWORD),
} as const;

export const SEARCH_ATTRIBUTE_KEYS = Object.values(SA);

