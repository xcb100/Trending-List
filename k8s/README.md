# Kubernetes 部署说明

## 概览

本目录包含排行榜服务及其 Redis 依赖的 Kubernetes 部署清单。

部署前检查以下内容：

- 命名空间名称
- 业务镜像地址和标签
- Redis 部署方式
- 集群内访问方式
- 集群外暴露方式
- 节点是否需要代理拉取镜像
- 集群是否已经安装可用的 CNI 网络插件

## 文件说明

- `namespace.yaml`：命名空间
- `configmap.yaml`：运行时环境变量
- `secret.yaml`：敏感配置，例如 Redis 密码
- `service.yaml`：业务口和内部口 Service
- `deployment.yaml`：业务 Deployment
- `redis.yaml`：Redis ConfigMap、Service、StatefulSet 与持久化卷声明
- `kustomization.yaml`：`kubectl apply -k` 入口

当前清单覆盖排行榜服务、Redis 依赖、Service 暴露、健康检查及基础部署资源。Redis 使用单副本 `StatefulSet` 和 PVC 持久化 `/data` 目录。

## Redis 持久化说明

Redis 部署特性：

- 使用 `StatefulSet` 管理 Redis Pod 身份与卷绑定关系
- 使用 `volumeClaimTemplates` 为 `/data` 目录声明持久化存储
- 通过 `redis.conf` 开启 `AOF` 持久化，并保留 `RDB` 快照策略
- 同时提供 `redis` 普通 Service 和 `redis-headless` Headless Service

存储说明：

- 清单默认使用 `local-path` 作为 `storageClassName`
- 使用其他存储类时，需要把 `redis.yaml` 中的 `storageClassName` 改成对应名称
- 当前 Redis 为单副本持久化部署

## 部署前检查

### 1. 镜像地址

确认 `deployment.yaml` 中的业务镜像地址可访问且可拉取。


### 2. Redis 地址

`configmap.yaml` 中的 `REDIS_ADDR` 使用 Kubernetes Service 名称：

```text
redis:6379
```

### 2.1 StorageClass

Redis 依赖 PVC 持久化数据。部署前检查集群中是否存在 `local-path`，或将 `redis.yaml` 中的 `storageClassName` 改成目标存储类名称：

```bash
kubectl get storageclass
```

没有可用存储类时，Redis 对应的 PVC 会停留在 `Pending` 状态。

### 3. 集群网络

在多节点 Kubernetes 集群中，必须安装并正确运行 CNI 网络插件，例如 `flannel` 或 `calico`。

如果没有可用的 CNI，常见现象包括：

- Pod 无法跨节点通信
- Service 能创建，但访问失败
- 同一集群中 Pod IP 出现异常或冲突
- 应用就绪检查持续失败

可先检查：

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,PODCIDR:.spec.podCIDR,PODCIDRS:.spec.podCIDRs
kubectl -n kube-system get pods -o wide
```

如果节点实际拿到的 Pod IP 网段和 `podCIDR` 不一致，应优先排查 CNI 配置，而不是业务代码。

## 镜像拉取代理

如果 Kubernetes 节点拉取镜像时出现 `ImagePullBackOff`，且错误中包含无法访问镜像仓库，可以考虑给运行时配置代理。

对于 `containerd`，通常做法是在每个节点创建：

`/etc/systemd/system/containerd.service.d/http-proxy.conf`

示例：

```ini
[Service]
Environment="HTTP_PROXY=http://<proxy-host>:<proxy-port>"
Environment="HTTPS_PROXY=http://<proxy-host>:<proxy-port>"
Environment="NO_PROXY=127.0.0.1,localhost,<service-cidr>,<pod-cidr>,<node-network>,.svc,.cluster.local"
```

然后在每个节点执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart containerd
sudo systemctl restart kubelet
systemctl show containerd --property=Environment
```

注意：

- 每个节点都要配置，不只是控制平面节点
- `NO_PROXY` 要覆盖集群内部访问地址
- 如果代理地址写错，镜像拉取和部分外部请求会持续失败

## 部署步骤

在项目根目录执行：

```bash
kubectl apply -k k8s/
kubectl -n <namespace> get deploy,statefulset,svc,pod,pvc -o wide
kubectl -n <namespace> rollout status statefulset/redis
kubectl -n <namespace> rollout status deploy/<app-deployment-name>
```

如果某个工作负载一直无法完成 rollout，优先检查：

```bash
kubectl -n <namespace> describe pod <pod-name>
kubectl -n <namespace> logs <pod-name>
```

如果 Redis 长时间无法启动，也建议同时检查 PVC：

```bash
kubectl -n <namespace> get pvc
kubectl -n <namespace> describe pvc <pvc-name>
```

## 服务访问方式

### 集群内访问

微服务之间调用统一使用 Kubernetes Service 名称。

典型方式：

- 业务接口：`http://<business-service>:<port>`
- 内部接口：`http://<internal-service>:<port>`
- Redis：`<redis-service>:6379`

如果是跨命名空间调用，可以使用完整域名：

```text
<service-name>.<namespace>.svc.cluster.local
```

### 集群外访问

如果需要从集群外验证或联调，可以根据场景选择：

- `NodePort`
- `LoadBalancer`
- `Ingress`

虚拟机环境下可使用 `NodePort` 进行集群外访问。

## 功能验证

### 1. 健康检查

先确认业务接口和内部接口都能返回健康状态：

```bash
curl -i http://<access-address>/livez
curl -i http://<access-address>/readyz
```

### 2. 创建排行榜

```bash
curl -sS -X POST http://<access-address>/leaderboard \
  -H "Content-Type: application/json" \
  -d '{
    "id": "top_videos",
    "expression": "views * 0.5 + likes * 2 + bonus",
    "schema": {
      "views": 0,
      "likes": 0,
      "bonus": 0
    },
    "refresh_policy": "realtime"
  }'
```

### 3. 写入条目

```bash
curl -sS -X POST http://<access-address>/leaderboard/top_videos/item \
  -H "Content-Type: application/json" \
  -d '{
    "item_id": "video_1",
    "data": {
      "views": 1000,
      "likes": 50,
      "bonus": 3
    }
  }'
```

### 4. 查询排行榜

```bash
curl -sS "http://<access-address>/leaderboard/top_videos?n=10"
```

## 内部接口验证

内部接口未直接暴露到集群外时，可使用端口转发：

```bash
kubectl -n <namespace> port-forward svc/<internal-service-name> 9090:9090
```

然后在另一个终端验证：

```bash
curl -i http://127.0.0.1:9090/healthz
curl -i http://127.0.0.1:9090/metrics
```

## 常用操作

重启业务 Deployment：

```bash
kubectl -n <namespace> rollout restart deploy/<app-deployment-name>
kubectl -n <namespace> rollout status deploy/<app-deployment-name>
```

查看 Pod：

```bash
kubectl -n <namespace> get pods -o wide
```

查看日志：

```bash
kubectl -n <namespace> logs deploy/<app-deployment-name> --tail=100
kubectl -n <namespace> logs statefulset/redis --tail=100
```

查看 Service Endpoints：

```bash
kubectl -n <namespace> get endpoints
```

## 故障排查

### 1. `ImagePullBackOff`

优先检查：

- 节点是否能访问镜像仓库
- 容器运行时是否配置了正确代理
- 镜像地址和标签是否存在

常用命令：

```bash
kubectl -n <namespace> describe pod <pod-name>
systemctl show containerd --property=Environment
```

### 2. Pod 已运行，但一直不 Ready

优先检查：

- readiness probe 返回什么状态码
- 应用是否能连接 Redis
- Service 名称解析是否正常

常用命令：

```bash
kubectl -n <namespace> describe pod <pod-name>
kubectl -n <namespace> logs <pod-name> --tail=100
```

### 3. Redis 连不上

优先确认：

- Redis Pod 是否正常运行
- Redis Service 是否有正确 endpoints
- 配置里是否使用了正确的 Service 名称或端口
- Redis 对应 PVC 是否已经成功绑定

常用命令：

```bash
kubectl -n <namespace> get pods -o wide
kubectl -n <namespace> get svc
kubectl -n <namespace> get endpoints <redis-service-name> -o wide
kubectl -n <namespace> get pvc
```

## 数据恢复验证

完成部署并写入排行榜数据后，可执行以下持久化验证：

1. 记录当前 Redis Pod 名称
2. 删除该 Pod，等待 `StatefulSet` 自动重建
3. 再次查询排行榜数据，确认内容仍然存在

示例：

```bash
kubectl -n <namespace> get pods -l app=redis
kubectl -n <namespace> delete pod redis-0
kubectl -n <namespace> rollout status statefulset/redis
kubectl -n <namespace> get pvc
```

PVC 保持 `Bound`，且业务数据在 Pod 重建后仍可查询时，说明 Redis 持久化链路生效。

### 4. 多节点下网络异常

如果出现跨节点访问失败、Pod IP 冲突、Service 转发异常等情况，优先排查 CNI。

常用命令：

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,PODCIDR:.spec.podCIDR,PODCIDRS:.spec.podCIDRs
kubectl -n kube-system get pods -o wide
ls -l /etc/cni/net.d
ip addr show cni0
```
