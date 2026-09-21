#!/usr/bin/env bash
set -euo pipefail

PROJECT="leon-demo-486305"
REGION="us-central1"
DATASET="retro_tech_revolution"
TOPIC="game_signals"
BQ_SUB="game-signals-bq-sub"
SCREENSHOT_BUCKET="rtr-screenshots-${PROJECT}"
BACKSTORY_BUCKET="rtr-backstories-${PROJECT}"

echo "=== 1. 启用 API ==="
gcloud services enable pubsub.googleapis.com bigquery.googleapis.com \
  storage.googleapis.com run.googleapis.com aiplatform.googleapis.com \
  --project=${PROJECT}

echo "=== 2. BigQuery dataset ==="
bq --project_id=${PROJECT} mk --dataset \
  --location=US \
  --description="Retro Tech Revolution game data" \
  ${PROJECT}:${DATASET} 2>/dev/null || echo "Dataset already exists"

echo "=== 3. raw_game_signals 表 ==="
bq --project_id=${PROJECT} mk --table \
  ${PROJECT}:${DATASET}.raw_game_signals \
  "subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,data:STRING,attributes:STRING" \
  2>/dev/null || echo "Table raw_game_signals already exists"

echo "=== 4. game_play_summaries 表 ==="
bq --project_id=${PROJECT} mk --table \
  ${PROJECT}:${DATASET}.game_play_summaries \
  "session_id:STRING,summary_text:STRING,created_at:TIMESTAMP" \
  2>/dev/null || echo "Table game_play_summaries already exists"

echo "=== 5. Pub/Sub topic ==="
gcloud pubsub topics create ${TOPIC} \
  --project=${PROJECT} 2>/dev/null || echo "Topic already exists"

echo "=== 6. PubSub 服务代理授权（dataset 级别）==="
# 注意：VM 上 compute SA 缺 project-level setIamPolicy，
# 改用 BigQuery dataset 级别的 access PATCH（compute SA 是 dataset OWNER）。
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT} --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
TOKEN=$(gcloud auth print-access-token)

echo "Granting BigQuery dataset WRITER to ${PUBSUB_SA} (dataset-level)"
DATASET_META=$(curl -s -X GET \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}")

NEW_META=$(echo "${DATASET_META}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
sa = 'service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com'
entry = {'role': 'WRITER', 'userByEmail': sa}
if entry not in d.get('access', []):
    d['access'].append(entry)
    print('Adding', sa, 'as WRITER', file=sys.stderr)
else:
    print('Already has WRITER access', file=sys.stderr)
print(json.dumps(d))
")

PATCH_RESULT=$(curl -s -X PATCH \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT}/datasets/${DATASET}" \
  -H "Content-Type: application/json" \
  -d "${NEW_META}")

echo "${PATCH_RESULT}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'error' in d:
    print('ERROR:', d['error']); exit(1)
else:
    print('Dataset IAM updated OK')
"

echo "=== 7. BigQuery Subscription ==="
# 用 REST API 以避免 gcloud 在 VM 环境中的权限问题
TOKEN=$(gcloud auth print-access-token)
SUB_RESULT=$(curl -s -X PUT \
  -H "Authorization: Bearer ${TOKEN}" \
  "https://pubsub.googleapis.com/v1/projects/${PROJECT}/subscriptions/${BQ_SUB}" \
  -H "Content-Type: application/json" \
  -d "{
    \"topic\": \"projects/${PROJECT}/topics/${TOPIC}\",
    \"bigqueryConfig\": {
      \"table\": \"${PROJECT}:${DATASET}.raw_game_signals\",
      \"writeMetadata\": true
    }
  }")

echo "${SUB_RESULT}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
if 'error' in d:
    code = d['error'].get('code')
    msg  = d['error'].get('message','')
    if code == 409 or 'already exists' in msg.lower():
        print('Subscription already exists')
    else:
        print('ERROR:', d['error']); exit(1)
else:
    state = d.get('bigqueryConfig', {}).get('state', 'UNKNOWN')
    print('Subscription created, bigqueryConfig.state =', state)
"

echo "=== 8. GCS buckets ==="
gcloud storage buckets create gs://${SCREENSHOT_BUCKET} \
  --project=${PROJECT} --location=${REGION} 2>/dev/null || echo "Screenshot bucket exists"
gcloud storage buckets create gs://${BACKSTORY_BUCKET} \
  --project=${PROJECT} --location=${REGION} 2>/dev/null || echo "Backstory bucket exists"

echo ""
echo "=== 完成！==="
echo "Screenshot bucket: gs://${SCREENSHOT_BUCKET}"
echo "Backstory bucket:  gs://${BACKSTORY_BUCKET}"
echo ""
echo "Subscription state:"
gcloud pubsub subscriptions describe ${BQ_SUB} \
  --project=${PROJECT} --format="value(bigqueryConfig.state)" 2>/dev/null
