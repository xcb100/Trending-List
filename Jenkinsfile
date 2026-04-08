pipeline {
  agent { label 'docker-kubectl' }

  options {
    disableConcurrentBuilds()
    timestamps()
  }

  parameters {
    string(name: 'IMAGE_REPO', defaultValue: 'jn6057/leaderboard-service', description: 'Docker image repository')
    string(name: 'IMAGE_TAG', defaultValue: '', description: 'Optional image tag override; defaults to BUILD_NUMBER')
    string(name: 'DEPLOY_NAMESPACE', defaultValue: 'leaderboard', description: 'Kubernetes namespace')
    string(name: 'DEPLOYMENT_NAME', defaultValue: 'leaderboard', description: 'Kubernetes deployment name')
    string(name: 'CONTAINER_NAME', defaultValue: 'leaderboard', description: 'Container name inside the deployment')
  }

  environment {
    GO_IMAGE = 'golang:1.25.6-bookworm'
    REDIS_IMAGE = 'redis:7-alpine'
    CI_NETWORK = 'leaderboard-ci'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Resolve Image') {
      steps {
        script {
          env.RESOLVED_IMAGE_TAG = params.IMAGE_TAG?.trim() ? params.IMAGE_TAG.trim() : env.BUILD_NUMBER
          env.IMAGE_REF = "${params.IMAGE_REPO}:${env.RESOLVED_IMAGE_TAG}"
          echo "Building image ${env.IMAGE_REF}"
        }
      }
    }

    stage('Test') {
      steps {
        sh '''#!/usr/bin/env bash
set -euo pipefail

docker rm -f redis-ci >/dev/null 2>&1 || true
docker network rm "${CI_NETWORK}" >/dev/null 2>&1 || true

docker network create "${CI_NETWORK}"
docker run -d --name redis-ci --network "${CI_NETWORK}" "${REDIS_IMAGE}"

trap 'docker rm -f redis-ci >/dev/null 2>&1 || true; docker network rm "${CI_NETWORK}" >/dev/null 2>&1 || true' EXIT

docker run --rm \
  --network "${CI_NETWORK}" \
  -e REDIS_ADDR=redis-ci:6379 \
  -e GOCACHE=/tmp/.gocache \
  -e GOTMPDIR=/tmp/.gotmp \
  -v "$WORKSPACE":/workspace \
  -w /workspace \
  "${GO_IMAGE}" \
  sh -c 'go test ./... -cover'
'''
      }
    }

    stage('Build Image') {
      steps {
        sh '''#!/usr/bin/env bash
set -euo pipefail
docker build -t "${IMAGE_REF}" .
'''
      }
    }

    stage('Push Image') {
      when {
        anyOf {
          branch 'main'
          branch 'master'
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'dockerhub-creds', usernameVariable: 'DOCKERHUB_USERNAME', passwordVariable: 'DOCKERHUB_PASSWORD')]) {
          sh '''#!/usr/bin/env bash
set -euo pipefail
echo "${DOCKERHUB_PASSWORD}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin
docker push "${IMAGE_REF}"
'''
        }
      }
    }

    stage('Deploy') {
      when {
        anyOf {
          branch 'main'
          branch 'master'
        }
      }
      steps {
        sh '''#!/usr/bin/env bash
set -euo pipefail
kubectl -n "${DEPLOY_NAMESPACE}" set image deployment/"${DEPLOYMENT_NAME}" "${CONTAINER_NAME}"="${IMAGE_REF}"
kubectl -n "${DEPLOY_NAMESPACE}" rollout status deployment/"${DEPLOYMENT_NAME}" --timeout=180s
'''
      }
    }
  }

  post {
    always {
      sh '''#!/usr/bin/env bash
docker rm -f redis-ci >/dev/null 2>&1 || true
docker network rm "${CI_NETWORK}" >/dev/null 2>&1 || true
docker logout >/dev/null 2>&1 || true
'''
    }
  }
}
