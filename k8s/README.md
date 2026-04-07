# Kubernetes 部署说明

## 概览

本目录包含排行榜服务及其 Redis 依赖的 Kubernetes 部署清单。

建议先根据自己的环境确认以下内容：

- 命名空间名称
- 业务镜像地址和标签
- Redis 部署方式
- 集群内访问方式
- 集群外暴露方式
- 节点是否需要代理拉取镜像
- 集群是否已经安装可用的 CNI 网络插件

> 提醒：
> 这套服务在 Kubernetes 中更适合作为集群内微服务使用。集群内调用建议优先使用 Service 名称，例如业务服务使用 `leaderboard-service:8080`，Redis 使用 `redis:6379`，不要依赖节点 IP 或固定 ClusterIP。多节点环境在部署前还应先确认两件事：一是节点能正常拉取镜像，必要时为 `containerd` 配置代理或使用内网镜像仓库；二是 CNI 网络插件已经正确安装并生效，否则容易出现 `ImagePullBackOff`、服务名解析异常、Pod 跨节点不通或就绪检查持续失败等问题。

> 补充说明：
> 当前清单未包含 Redis 持久化所需的 PV、PVC 等资源。如果后续需要保留 Redis 数据，可以继续补充持久化存储相关配置并完成挂载。另外，CI/CD 工作流、Jenkins、Istio 等配套能力暂未纳入本目录，后续可根据实际需要再逐步接入和验证。

## 文件说明

- `namespace.yaml`：命名空间
- `configmap.yaml`：运行时环境变量
- `secret.yaml`：敏感配置，例如 Redis 密码
- `service.yaml`：业务口和内部口 Service
- `deployment.yaml`：业务 Deployment
- `redis.yaml`：Redis Deployment 和 Service
- `kustomization.yaml`：`kubectl apply -k` 入口

## 部署前检查

### 1. 镜像地址

先确认 `deployment.yaml` 中的业务镜像地址是当前可访问、可拉取的镜像。

如果节点无法直接访问公网镜像仓库，通常有三种处理方式：

- 使用企业内网镜像仓库
- 使用可访问的镜像代理源
- 先离线导入镜像到各节点

不建议长期依赖 `latest` 标签，正式环境建议改为固定版本号。

### 2. Redis 地址

在 `configmap.yaml` 中，`REDIS_ADDR` 建议优先写成 Kubernetes Service 名称，例如：

```text
redis:6379
```

这样更适合集群内部服务发现，不依赖固定 IP。

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
kubectl -n <namespace> get deploy,svc,pod -o wide
kubectl -n <namespace> rollout status deploy/<redis-deployment-name>
kubectl -n <namespace> rollout status deploy/<app-deployment-name>
```

如果某个 Deployment 一直无法完成 rollout，优先检查：

```bash
kubectl -n <namespace> describe pod <pod-name>
kubectl -n <namespace> logs <pod-name>
```

## 服务访问方式

### 集群内访问

微服务之间调用时，建议统一使用 Kubernetes Service 名称，而不是节点 IP。

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

如果只是虚拟机环境下快速验证，`NodePort` 往往最直接。

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

如果返回内容正常，说明业务链路已经打通。

## 内部接口验证

如果内部接口没有直接暴露到集群外，可以使用端口转发：

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
kubectl -n <namespace> logs deploy/<redis-deployment-name> --tail=100
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

常用命令：

```bash
kubectl -n <namespace> get pods -o wide
kubectl -n <namespace> get svc
kubectl -n <namespace> get endpoints <redis-service-name> -o wide
```

### 4. 多节点下网络异常

如果出现跨节点访问失败、Pod IP 冲突、Service 转发异常等情况，优先排查 CNI。

常用命令：

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,PODCIDR:.spec.podCIDR,PODCIDRS:.spec.podCIDRs
kubectl -n kube-system get pods -o wide
ls -l /etc/cni/net.d
ip addr show cni0
```
