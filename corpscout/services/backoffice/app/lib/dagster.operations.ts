/**
 * GraphQL operations owned by the Backoffice Dagster boundary.
 *
 * `pnpm dagster:codegen` reads these tagged strings against the deployed
 * Dagster schema and regenerates `dagster.generated.ts`. They stay as plain
 * strings at runtime so the existing fetch transport needs no GraphQL client.
 */

export const BACKOFFICE_LAUNCH_RUN_MUTATION = /* GraphQL */ `
  mutation BackofficeLaunchRun($executionParams: ExecutionParams!) {
    launchRun(executionParams: $executionParams) {
      __typename
      ... on LaunchRunSuccess {
        run {
          runId
          status
        }
      }
      ... on RunConfigValidationInvalid {
        pipelineName
        errors {
          message
          path
          reason
        }
      }
      ... on PythonError {
        message
      }
      ... on PipelineNotFoundError {
        message
      }
      ... on InvalidSubsetError {
        message
      }
      ... on ConflictingExecutionParamsError {
        message
      }
      ... on PresetNotFoundError {
        message
      }
      ... on UnauthorizedError {
        message
      }
    }
  }
`;

export const BACKOFFICE_RUNS_QUERY = /* GraphQL */ `
  query BackofficeRuns($filter: RunsFilter!, $limit: Int!) {
    runsOrError(filter: $filter, limit: $limit) {
      __typename
      ... on Runs {
        results {
          runId
          status
          jobName
          startTime
          endTime
          tags {
            key
            value
          }
        }
      }
      ... on PythonError {
        message
      }
      ... on InvalidPipelineRunsFilterError {
        message
      }
    }
  }
`;

export const BACKOFFICE_RUN_QUERY = /* GraphQL */ `
  query BackofficeRun($runId: ID!) {
    runOrError(runId: $runId) {
      __typename
      ... on Run {
        runId
        status
        jobName
        startTime
        endTime
        tags {
          key
          value
        }
      }
      ... on RunNotFoundError {
        message
      }
      ... on PythonError {
        message
      }
    }
  }
`;

export const BACKOFFICE_ASSET_MATERIALIZATIONS_QUERY = /* GraphQL */ `
  query BackofficeAssetMaterializations(
    $assetKeys: [AssetKeyInput!]!
    $limit: Int!
  ) {
    assetNodes(assetKeys: $assetKeys) {
      id
      assetMaterializations(limit: $limit) {
        runId
        timestamp
        metadataEntries {
          label
          __typename
          ... on IntMetadataEntry {
            intValue
          }
        }
      }
    }
  }
`;

export const BACKOFFICE_INSTIGATORS_QUERY = /* GraphQL */ `
  query BackofficeInstigators($repositorySelector: RepositorySelector!) {
    schedulesOrError(repositorySelector: $repositorySelector) {
      __typename
      ... on Schedules {
        results {
          name
          cronSchedule
          scheduleState {
            status
          }
        }
      }
      ... on PythonError {
        message
      }
    }
    sensorsOrError(repositorySelector: $repositorySelector) {
      __typename
      ... on Sensors {
        results {
          name
          sensorState {
            status
          }
        }
      }
      ... on PythonError {
        message
      }
    }
  }
`;
