# Jenkins 安装与最小配置

本目录提供在 Linux VM 上运行 Jenkins 的最小文件集合。

## 文件说明

- `Dockerfile`：构建包含 `docker` CLI 与 `kubectl` 的 Jenkins 镜像
- `plugins.txt`：预装 Jenkins 插件列表
- `docker-compose.yml`：启动 Jenkins 容器
- `k8s-rbac.yaml`：Jenkins 发布 `leaderboard` Deployment 所需的最小 RBAC
- `generate-kubeconfig.sh`：为 `jenkins-deployer` 生成独立 kubeconfig

## 前置条件

- 已安装 Docker Engine
- 已安装 Docker Compose Plugin 或 `docker-compose`
- 当前主机可以执行 `kubectl`
- 当前主机的 `kubectl` 已连接到目标 Kubernetes 集群

## 安装步骤

### 1. 创建 Kubernetes 发布权限

```bash
kubectl apply -f jenkins/k8s-rbac.yaml
```

### 2. 生成 Jenkins 专用 kubeconfig

```bash
chmod +x jenkins/generate-kubeconfig.sh
./jenkins/generate-kubeconfig.sh
```

脚本默认生成：

```text
jenkins/kubeconfig
```

### 3. 构建并启动 Jenkins

在项目根目录执行：

```bash
docker compose -f jenkins/docker-compose.yml build
docker compose -f jenkins/docker-compose.yml up -d
```

### 4. 获取初始管理员密码

```bash
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

### 5. 访问 Jenkins

```text
http://<vm-ip>:8081
```

### 6. 配置 Jenkins 内置节点标签

在 Jenkins 管理界面中，将内置节点标签设置为：

```text
docker-kubectl
```

`Jenkinsfile` 默认使用该标签作为执行节点。

### 7. 配置 Docker Hub 凭据

在 Jenkins 中新增凭据：

- ID：`dockerhub-creds`
- 类型：`Username with password`

### 8. 创建 Pipeline Job

- Job 类型：`Pipeline`
- 定义方式：`Pipeline script from SCM`
- SCM：Git
- Script Path：`Jenkinsfile`

## 验证命令

Jenkins 容器启动后，可执行以下命令确认工具可用：

```bash
docker exec jenkins docker version
docker exec jenkins kubectl -n leaderboard get pods
```

## 停止与清理

停止 Jenkins：

```bash
docker compose -f jenkins/docker-compose.yml down
```

删除 Jenkins 数据卷：

```bash
docker compose -f jenkins/docker-compose.yml down -v
```
