#!/bin/bash
# Deploy Lambda function to AWS. Handles both create and update scenarios.
# Usage: ./deploy-lambda.sh <lambda-role-arn>
#
# Required environment variables:
#   AWS_REGION, LAMBDA_FUNCTION_NAME, LAMBDA_TIMEOUT, LAMBDA_MEMORY,
#   LAMBDA_RUNTIME, LAMBDA_ARCH, FAIL_ON_PREVIOUS_FAILURE (optional, defaults to true)

set -euo pipefail

# Validate required argument
if [[ $# -lt 1 || -z "${1:-}" ]]; then
  echo "ERROR: Lambda execution role ARN must be provided as first argument." >&2
  echo "Usage: $0 <lambda-role-arn>" >&2
  exit 1
fi

LAMBDA_ROLE_ARN="$1"

wait_for_lambda_ready() {
  local function_name=$1
  local max_attempts=30
  local wait_seconds=10
  local attempt=1
  local total_timeout=$((max_attempts * wait_seconds))
  local status=""
  local state=""
  local stderr_output=""
  
  cleanup_temp_file() {
    if [[ -n "$stderr_output" && -f "$stderr_output" ]]; then
      rm -f "$stderr_output"
    fi
  }
  trap cleanup_temp_file RETURN
  
  echo "Checking if Lambda function is ready for updates..."
  
  while [[ $attempt -le $max_attempts ]]; do
    stderr_output=$(mktemp)
    function_details=$(aws lambda get-function \
      --function-name "${function_name}" \
      --region "${AWS_REGION}" \
      --query 'Configuration.{Status:LastUpdateStatus,State:State,Code:LastUpdateStatusReasonCode,Reason:LastUpdateStatusReason}' \
      --output json 2>"$stderr_output")
    local aws_exit_code=$?
    
    if [[ -z "$function_details" ]]; then
      if grep -q "ResourceNotFoundException" "$stderr_output"; then
        echo "ERROR: Lambda function '${function_name}' not found (ResourceNotFoundException)." >&2
        echo "The function may have been deleted between the existence check and this call." >&2
        echo "This is likely a race condition or the function was deleted externally." >&2
        return 1
      fi
      
      echo "ERROR: Unable to retrieve Lambda function details (empty response from aws lambda get-function)." >&2
      echo "DEBUG: function_name='${function_name}', region='${AWS_REGION}', exit_code=${aws_exit_code}" >&2
      if [[ -s "$stderr_output" ]]; then
        echo "DEBUG: stderr output:" >&2
        cat "$stderr_output" >&2
      fi
      echo "Attempt $attempt/$max_attempts - Unable to retrieve function details"
      sleep $wait_seconds
      attempt=$((attempt + 1))
      continue
    fi
    
    if ! echo "$function_details" | jq empty >/dev/null 2>&1; then
      echo "ERROR: Received invalid JSON when retrieving Lambda function details." >&2
      echo "DEBUG: Raw function_details payload:" >&2
      echo "$function_details" >&2
      echo "Attempt $attempt/$max_attempts - Invalid JSON response"
      sleep $wait_seconds
      attempt=$((attempt + 1))
      continue
    fi
    
    status=$(echo "$function_details" | jq -r '.Status // "Unknown"')
    state=$(echo "$function_details" | jq -r '.State // "Unknown"')
    
    echo "Attempt $attempt/$max_attempts - State: $state, LastUpdateStatus: $status"
    
    if [[ "$state" == "Failed" ]]; then
      echo "ERROR: Lambda function is in Failed state. Cannot proceed with deployment from this workflow."
      echo "This usually indicates a problem with the function configuration, VPC/network settings, or IAM permissions."
      echo "Next steps: Inspect the function's CloudWatch Logs, verify the Lambda configuration (runtime, handler, environment variables),"
      echo "           and review associated VPC subnets/security groups and IAM execution role in the AWS Console before retrying."
      return 1
    fi
    
    if [[ "$state" == "Active" ]]; then
      if [[ "$status" == "Successful" || "$status" == "Unknown" ]]; then
        if [[ "$status" == "Unknown" ]]; then
          echo "Lambda function is Active with no LastUpdateStatus (likely newly created). Treating as ready."
        else
          echo "Lambda function is ready!"
        fi
        return 0
      elif [[ "$status" == "Failed" ]]; then
        echo "========================================="
        echo "WARNING: Previous Lambda update FAILED (State: $state, Status: $status)"
        echo "========================================="
        
        error_code=$(echo "$function_details" | jq -r '.Code // "Not available"')
        error_reason=$(echo "$function_details" | jq -r '.Reason // "Not available"')
        
        echo "Error Code: ${error_code}"
        echo "Error Reason: ${error_reason}"
        echo ""
        echo "Risks of proceeding:"
        echo "  - The Lambda function may be in an inconsistent state"
        echo "  - Previous deployment issues may not be resolved"
        echo "  - New deployment could fail or behave unexpectedly"
        echo "  - Underlying infrastructure or permission issues may persist"
        echo ""
        
        if [[ "${FAIL_ON_PREVIOUS_FAILURE:-true}" = "true" ]]; then
          echo "FAIL_ON_PREVIOUS_FAILURE is set to 'true'. Aborting deployment."
          echo "To proceed despite previous failure, set FAIL_ON_PREVIOUS_FAILURE=false"
          echo "========================================="
          return 1
        else
          echo "FAIL_ON_PREVIOUS_FAILURE is set to 'false'. Proceeding with deployment."
          echo "NOTE: This may mask underlying issues. Consider investigating the failure."
          echo "========================================="
          return 0
        fi
      else
        echo "DEBUG: Active Lambda function has unexpected LastUpdateStatus (State: $state, Status: $status). Continuing to wait..."
      fi
    fi
    
    if [[ "$status" == "InProgress" ]]; then
      echo "Lambda update is in progress (State: $state, Status: $status). Waiting ${wait_seconds} seconds..."
    elif [[ "$state" == "Pending" ]]; then
      echo "Lambda is in Pending state (State: $state, Status: $status). Waiting ${wait_seconds} seconds..."
    elif [[ "$state" == "Inactive" ]]; then
      echo "INFO: Lambda function is currently Inactive (this is not a deployment blocker). Continuing to poll until the last update completes or times out..."
    else
      echo "WARNING: Unexpected state combination (State: $state, Status: $status). Continuing to wait..."
    fi
    
    sleep $wait_seconds
    attempt=$((attempt + 1))
  done
  
  echo "ERROR: Timeout waiting for Lambda to be ready after ${total_timeout} seconds."
  echo "Final observed Lambda state - State: $state, LastUpdateStatus: $status."
  echo "Next steps:"
  echo "  - Check the AWS Lambda console for function \"${function_name}\" in region \"${AWS_REGION}\" to confirm its current state."
  echo "  - Verify that the Lambda execution role has the correct permissions and that recent configuration changes are valid."
  echo "  - Inspect recent CloudWatch Logs for this function for initialization or deployment errors."
  echo "  - If the function is stuck in Pending or Failed states, resolve any configuration or permission issues and then retry the deployment."
  return 1
}

if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${AWS_REGION}" &>/dev/null; then
  echo "Function exists, checking readiness..."
  
  error_output=$(mktemp)
  trap 'rm -f "$error_output"' EXIT
  if ! wait_for_lambda_ready "${LAMBDA_FUNCTION_NAME}" 2>"$error_output"; then
    if grep -q "ResourceNotFoundException" "$error_output"; then
      echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' was deleted during deployment."
      echo "The function existed at the start but is now missing. This may indicate concurrent deployments or manual deletion."
      exit 1
    fi
    echo "Error: Lambda function is not ready for updates after waiting"
    exit 1
  fi
  trap - EXIT
  rm -f "$error_output"
  
  echo "Updating code..."
  error_output=$(mktemp)
  trap 'rm -f "$error_output"' EXIT
  if ! aws lambda update-function-code \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --zip-file fileb://function.zip \
    --region "${AWS_REGION}" 2>"$error_output"; then
    if grep -q "ResourceNotFoundException" "$error_output"; then
      echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' was deleted before code update."
      echo "The function was ready but is now missing. This may indicate concurrent deployments or manual deletion."
      exit 1
    fi
    echo "ERROR: Failed to update function code. Details:"
    cat "$error_output"
    exit 1
  fi
  trap - EXIT
  rm -f "$error_output"
  
  error_output=$(mktemp)
  trap 'rm -f "$error_output"' EXIT
  if ! wait_for_lambda_ready "${LAMBDA_FUNCTION_NAME}" 2>"$error_output"; then
    if grep -q "ResourceNotFoundException" "$error_output"; then
      echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' was deleted after code update."
      echo "The function completed code update but is now missing. This may indicate concurrent deployments or manual deletion."
      exit 1
    fi
    echo "Error: Lambda function is not ready after code update"
    exit 1
  fi
  trap - EXIT
  rm -f "$error_output"
  
  echo "Updating function configuration..."
  error_output=$(mktemp)
  trap 'rm -f "$error_output"' EXIT
  if ! aws lambda update-function-configuration \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --timeout "${LAMBDA_TIMEOUT}" \
    --memory-size "${LAMBDA_MEMORY}" \
    --runtime "${LAMBDA_RUNTIME}" \
    --handler bootstrap \
    --region "${AWS_REGION}" 2>"$error_output"; then
    if grep -q "ResourceNotFoundException" "$error_output"; then
      echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' was deleted before configuration update."
      echo "The function was ready after code update but is now missing. This may indicate concurrent deployments or manual deletion."
      exit 1
    fi
    echo "ERROR: Failed to update function configuration. Details:"
    cat "$error_output"
    exit 1
  fi
  trap - EXIT
  rm -f "$error_output"

  echo "Configuration update initiated, waiting for Lambda to become ready..."

  # Final readiness check after configuration update
  error_output=$(mktemp)
  trap 'rm -f "$error_output"' EXIT
  if ! wait_for_lambda_ready "${LAMBDA_FUNCTION_NAME}" 2>"$error_output"; then
    if grep -q "ResourceNotFoundException" "$error_output"; then
      echo "ERROR: Lambda function '${LAMBDA_FUNCTION_NAME}' was deleted after configuration update."
      echo "The function is now missing. This may indicate concurrent deployments or manual deletion."
      exit 1
    fi
    echo "Error: Lambda function is not ready after configuration update"
    exit 1
  fi
  trap - EXIT
  rm -f "$error_output"
else
  echo "Function does not exist, creating..."
  aws lambda create-function \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --runtime "${LAMBDA_RUNTIME}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler bootstrap \
    --timeout "${LAMBDA_TIMEOUT}" \
    --memory-size "${LAMBDA_MEMORY}" \
    --zip-file fileb://function.zip \
    --region "${AWS_REGION}" \
    --architectures "[\"${LAMBDA_ARCH}\"]"

  echo "Create operation completed, waiting for Lambda to become ready..."

  create_error_output="$(mktemp)"
  if ! wait_for_lambda_ready "${LAMBDA_FUNCTION_NAME}" 2> "${create_error_output}"; then
    echo "Error: Lambda function did not become ready after creation"

    if grep -q "ResourceNotFoundException" "${create_error_output}"; then
      echo "Detailed error: AWS reported ResourceNotFoundException while waiting for"
      echo "the newly created Lambda function to become ready. This may indicate:"
      echo "  - The function was deleted immediately after creation, or"
      echo "  - There is a temporary propagation delay in AWS Lambda APIs."
    fi

    echo "Full stderr output from wait_for_lambda_ready (create path):"
    cat "${create_error_output}"

    rm -f "${create_error_output}"
    exit 1
  fi

  rm -f "${create_error_output}"
fi

echo "Deployment successful!"