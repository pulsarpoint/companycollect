/** Internal type. DO NOT USE DIRECTLY. */
type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** Internal type. DO NOT USE DIRECTLY. */
export type Incremental<T> =
  | T
  | {
      [P in keyof T]?: P extends " $fragmentName" | "__typename" ? T[P] : never;
    };
export type AssetCheckHandleInput = {
  assetKey: AssetKeyInput;
  name: string;
};

/**
 * This type represents the fields necessary to identify
 *         an asset group.
 */
export type AssetGroupSelector = {
  groupName: string;
  repositoryLocationName: string;
  repositoryName: string;
};

export type AssetKeyInput = {
  path: Array<string>;
};

export type EvaluationErrorReason =
  | "FIELDS_NOT_DEFINED"
  | "FIELD_NOT_DEFINED"
  | "MISSING_REQUIRED_FIELD"
  | "MISSING_REQUIRED_FIELDS"
  | "RUNTIME_TYPE_MISMATCH"
  | "SELECTOR_FIELD_ERROR";

export type ExecutionMetadata = {
  /**
   * The ID of the run serving as the parent within the run group.
   *         For the first re-execution, this will be the same as the `rootRunId`. For
   *         subsequent runs, the root or a previous re-execution could be the parent run.
   */
  parentRunId?: string | null | undefined;
  /**
   * The ID of the run at the root of the run group. All partial /
   *         full re-executions should use the first run as the rootRunID so they are
   *         grouped together.
   */
  rootRunId?: string | null | undefined;
  tags?: Array<ExecutionTag> | null | undefined;
};

export type ExecutionParams = {
  /**
   * Defines run tags and parent / root relationships.
   *
   * Note: To
   *         'restart from failure', provide a `parentRunId` and pass the
   *         'dagster/is_resume_retry' tag. Dagster's automatic step key selection will
   *         override any stepKeys provided.
   */
  executionMetadata?: ExecutionMetadata | null | undefined;
  mode?: string | null | undefined;
  preset?: string | null | undefined;
  runConfigData?: Record<string, unknown> | null | undefined;
  /**
   * Defines the job / pipeline and solid subset that should be executed.
   *         All subsequent executions in the same run group (for example, a single-step
   *         re-execution) are scoped to the original run's selector and solid
   *         subset.
   */
  selector: JobOrPipelineSelector;
  /**
   * Defines step keys to execute within the execution plan defined
   *         by the pipeline `selector`. To execute the entire execution plan, you can omit
   *         this parameter, provide an empty array, or provide every step name.
   */
  stepKeys?: Array<string> | null | undefined;
};

export type ExecutionTag = {
  key: string;
  value: string;
};

export type InstigationStatus = "RUNNING" | "STOPPED";

/** This type represents the fields necessary to identify a job or pipeline */
export type JobOrPipelineSelector = {
  assetCheckSelection?: Array<AssetCheckHandleInput> | null | undefined;
  assetSelection?: Array<AssetKeyInput> | null | undefined;
  jobName?: string | null | undefined;
  pipelineName?: string | null | undefined;
  repositoryLocationName: string;
  repositoryName: string;
  solidSelection?: Array<string> | null | undefined;
};

export type PartitionDefinitionType =
  "DYNAMIC" | "MULTIPARTITIONED" | "STATIC" | "TIME_WINDOW";

/** This type represents the fields necessary to identify a repository. */
export type RepositorySelector = {
  repositoryLocationName: string;
  repositoryName: string;
};

/** The status of run execution. */
export type RunStatus =
  /** Runs that have been canceled before completion. */
  | "CANCELED"
  /** Runs that are in-progress and pending to be canceled. */
  | "CANCELING"
  /** Runs that have failed to complete. */
  | "FAILURE"
  /** Runs that are managed outside of the Dagster control plane. */
  | "MANAGED"
  /** Runs that have been created, but not yet submitted for launch. */
  | "NOT_STARTED"
  /** Runs waiting to be launched by the Dagster Daemon. */
  | "QUEUED"
  /** Runs that have been launched and execution has started. */
  | "STARTED"
  /** Runs that have been launched, but execution has not yet started. */
  | "STARTING"
  /** Runs that have successfully completed. */
  | "SUCCESS";

/** This type represents a filter on Dagster runs. */
export type RunsFilter = {
  createdAfter?: number | null | undefined;
  createdBefore?: number | null | undefined;
  mode?: string | null | undefined;
  pipelineName?: string | null | undefined;
  runIds?: Array<string | null | undefined> | null | undefined;
  snapshotId?: string | null | undefined;
  statuses?: Array<RunStatus> | null | undefined;
  tags?: Array<ExecutionTag> | null | undefined;
  updatedAfter?: number | null | undefined;
  updatedBefore?: number | null | undefined;
};

/** An enumeration. */
export type StaleStatus = "FRESH" | "MISSING" | "STALE";

export type BackofficeLaunchRunMutationVariables = Exact<{
  executionParams: ExecutionParams;
}>;

export type BackofficeLaunchRunMutation = {
  __typename: "Mutation";
  launchRun:
    | { __typename: "ConflictingExecutionParamsError"; message: string }
    | { __typename: "InvalidOutputError" }
    | { __typename: "InvalidStepError" }
    | { __typename: "InvalidSubsetError"; message: string }
    | {
        __typename: "LaunchRunSuccess";
        run: { __typename: "Run"; runId: string; status: RunStatus };
      }
    | { __typename: "NoModeProvidedError" }
    | { __typename: "PipelineNotFoundError"; message: string }
    | { __typename: "PresetNotFoundError"; message: string }
    | { __typename: "PythonError"; message: string }
    | {
        __typename: "RunConfigValidationInvalid";
        pipelineName: string;
        errors: Array<
          | {
              __typename: "FieldNotDefinedConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
          | {
              __typename: "FieldsNotDefinedConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
          | {
              __typename: "MissingFieldConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
          | {
              __typename: "MissingFieldsConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
          | {
              __typename: "RuntimeMismatchConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
          | {
              __typename: "SelectorTypeConfigError";
              message: string;
              path: Array<string>;
              reason: EvaluationErrorReason;
            }
        >;
      }
    | { __typename: "RunConflict" }
    | { __typename: "UnauthorizedError"; message: string };
};

export type BackofficeRunsQueryVariables = Exact<{
  filter: RunsFilter;
  limit: number;
}>;

export type BackofficeRunsQuery = {
  __typename: "Query";
  runsOrError:
    | { __typename: "InvalidPipelineRunsFilterError"; message: string }
    | { __typename: "PythonError"; message: string }
    | {
        __typename: "Runs";
        results: Array<{
          __typename: "Run";
          runId: string;
          status: RunStatus;
          jobName: string;
          startTime: number | null;
          endTime: number | null;
          runConfig: Record<string, unknown>;
          assetSelection: Array<{
            __typename: "AssetKey";
            path: Array<string>;
          }> | null;
          tags: Array<{
            __typename: "PipelineTag";
            key: string;
            value: string;
          }>;
        }>;
      };
};

export type BackofficeRunQueryVariables = Exact<{
  runId: string;
}>;

export type BackofficeRunQuery = {
  __typename: "Query";
  runOrError:
    | { __typename: "PythonError"; message: string }
    | {
        __typename: "Run";
        runId: string;
        status: RunStatus;
        jobName: string;
        startTime: number | null;
        endTime: number | null;
        runConfig: Record<string, unknown>;
        assetSelection: Array<{
          __typename: "AssetKey";
          path: Array<string>;
        }> | null;
        tags: Array<{ __typename: "PipelineTag"; key: string; value: string }>;
      }
    | { __typename: "RunNotFoundError"; message: string };
};

export type BackofficeAssetGroupQueryVariables = Exact<{
  group: AssetGroupSelector;
}>;

export type BackofficeAssetGroupQuery = {
  __typename: "Query";
  assetNodes: Array<{
    __typename: "AssetNode";
    id: string;
    groupName: string;
    description: string | null;
    jobNames: Array<string>;
    kinds: Array<string>;
    staleStatus: StaleStatus | null;
    assetKey: { __typename: "AssetKey"; path: Array<string> };
    dependencyKeys: Array<{ __typename: "AssetKey"; path: Array<string> }>;
    partitionDefinition: {
      __typename: "PartitionDefinition";
      type: PartitionDefinitionType;
    } | null;
    assetMaterializations: Array<{
      __typename: "MaterializationEvent";
      runId: string;
      timestamp: string;
    }>;
  }>;
};

export type BackofficeAssetMaterializationsQueryVariables = Exact<{
  assetKeys: Array<AssetKeyInput> | AssetKeyInput;
  limit: number;
}>;

export type BackofficeAssetMaterializationsQuery = {
  __typename: "Query";
  assetNodes: Array<{
    __typename: "AssetNode";
    id: string;
    assetMaterializations: Array<{
      __typename: "MaterializationEvent";
      runId: string;
      timestamp: string;
      metadataEntries: Array<
        | { __typename: "AssetMetadataEntry"; label: string }
        | { __typename: "BoolMetadataEntry"; label: string }
        | { __typename: "CodeReferencesMetadataEntry"; label: string }
        | { __typename: "FloatMetadataEntry"; label: string }
        | {
            __typename: "IntMetadataEntry";
            intValue: number | null;
            label: string;
          }
        | { __typename: "JobMetadataEntry"; label: string }
        | { __typename: "JsonMetadataEntry"; label: string }
        | { __typename: "MarkdownMetadataEntry"; label: string }
        | { __typename: "NotebookMetadataEntry"; label: string }
        | { __typename: "NullMetadataEntry"; label: string }
        | { __typename: "PathMetadataEntry"; label: string }
        | { __typename: "PipelineRunMetadataEntry"; label: string }
        | { __typename: "PoolMetadataEntry"; label: string }
        | { __typename: "PythonArtifactMetadataEntry"; label: string }
        | { __typename: "TableColumnLineageMetadataEntry"; label: string }
        | { __typename: "TableMetadataEntry"; label: string }
        | { __typename: "TableSchemaMetadataEntry"; label: string }
        | { __typename: "TextMetadataEntry"; label: string }
        | { __typename: "TimestampMetadataEntry"; label: string }
        | { __typename: "UrlMetadataEntry"; label: string }
      >;
    }>;
  }>;
};

export type BackofficeInstigatorsQueryVariables = Exact<{
  repositorySelector: RepositorySelector;
}>;

export type BackofficeInstigatorsQuery = {
  __typename: "Query";
  schedulesOrError:
    | { __typename: "PythonError"; message: string }
    | { __typename: "RepositoryNotFoundError" }
    | {
        __typename: "Schedules";
        results: Array<{
          __typename: "Schedule";
          name: string;
          cronSchedule: string;
          scheduleState: {
            __typename: "InstigationState";
            status: InstigationStatus;
          };
        }>;
      };
  sensorsOrError:
    | { __typename: "PythonError"; message: string }
    | { __typename: "RepositoryNotFoundError" }
    | {
        __typename: "Sensors";
        results: Array<{
          __typename: "Sensor";
          name: string;
          sensorState: {
            __typename: "InstigationState";
            status: InstigationStatus;
          };
        }>;
      };
};
