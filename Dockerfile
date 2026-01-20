# 多阶段构建 Dockerfile
FROM golang:1.24-alpine AS builder

# 安装必要的工具
RUN apk add --no-cache git protobuf-dev

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建 TSO 和 Replica 二进制文件
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /tso ./cmd/tso/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /replica ./cmd/replica/main.go

# 运行时镜像
FROM alpine:latest

# 安装 ca-certificates 用于 HTTPS 连接
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# 从构建阶段复制二进制文件
COPY --from=builder /tso .
COPY --from=builder /replica .

# 复制启动脚本
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# 设置入口点
ENTRYPOINT ["/root/entrypoint.sh"]
