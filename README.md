# project-1

graph TB
    %% 用户层
    subgraph "用户层"
        U1[数字孪生管理员]
        U2[数据分析师]
        U3[运维工程师]
    end

    %% 前端应用层
    subgraph "前端应用层"
        WEB[Web管理界面]
        EDITOR[Workflow可视化编辑器]
        MONITOR[实时监控看板]
        API_DOC[API文档]
    end

    %% API网关层
    subgraph "API网关层"
        NGINX[Nginx反向代理]
        LB[负载均衡]
        SSL[SSL终端]
    end

    %% 核心服务层
    subgraph "核心微服务"
        subgraph "数据处理服务"
            DP_API[FastAPI应用]
            WF_MGR[Workflow管理器]
            WF_EXEC[执行引擎]
            FUNC_REG[函数注册表]
        end
        
        subgraph "任务调度服务"
            SCHEDULER[任务调度器]
            WORKER[工作进程池]
            QUEUE[任务队列]
        end
        
        subgraph "数据管理服务"
            DATA_API[数据API]
            FILE_MGR[文件管理]
            STREAM[流处理]
        end
    end

    %% 数据存储层
    subgraph "数据存储层"
        subgraph "关系数据库"
            PG_MAIN[(PostgreSQL主库)]
            PG_READ[(PostgreSQL读库)]
        end
        
        subgraph "缓存存储"
            REDIS[(Redis集群)]
            REDIS_CACHE[(Redis缓存)]
            REDIS_SESSION[(Redis会话)]
        end
        
        subgraph "对象存储"
            MINIO[(MinIO对象存储)]
            FILES[文件存储]
            RESULTS[结果存储]
        end
        
        subgraph "时序数据库"
            TSDB[(InfluxDB)]
            METRICS[指标数据]
        end
    end

    %% 监控观测层
    subgraph "监控观测"
        PROMETHEUS[Prometheus监控]
        GRAFANA[Grafana可视化]
        JAEGER[链路追踪]
        ELK[日志收集分析]
    end

    %% 外部系统
    subgraph "外部数据源"
        IOT[物联网设备]
        SENSORS[传感器网络]
        ERP[ERP系统]
        MES[MES系统]
        THIRD_API[第三方API]
    end

    %% 连接关系
    U1 --> WEB
    U2 --> EDITOR
    U3 --> MONITOR
    
    WEB --> NGINX
    EDITOR --> NGINX
    MONITOR --> NGINX
    API_DOC --> NGINX
    
    NGINX --> LB
    LB --> DP_API
    LB --> DATA_API
    
    DP_API --> WF_MGR
    DP_API --> WF_EXEC
    DP_API --> FUNC_REG
    
    WF_EXEC --> SCHEDULER
    SCHEDULER --> WORKER
    WORKER --> QUEUE
    
    WF_EXEC --> REDIS
    WF_MGR --> PG_MAIN
    DATA_API --> PG_READ
    
    WORKER --> MINIO
    RESULTS --> MINIO
    FILES --> MINIO
    
    DP_API --> REDIS_CACHE
    WEB --> REDIS_SESSION
    
    SCHEDULER --> TSDB
    METRICS --> TSDB
    
    %% 外部数据流
    IOT --> DATA_API
    SENSORS --> STREAM
    ERP --> DATA_API
    MES --> DATA_API
    THIRD_API --> DATA_API
    
    %% 监控连接
    DP_API -.-> PROMETHEUS
    SCHEDULER -.-> PROMETHEUS
    DATA_API -.-> PROMETHEUS
    PROMETHEUS --> GRAFANA
    
    DP_API -.-> JAEGER
    WF_EXEC -.-> JAEGER
    
    DP_API -.-> ELK
    WORKER -.-> ELK
    DATA_API -.-> ELK

    %% 样式定义
    classDef userClass fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
    classDef frontendClass fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef serviceClass fill:#e8f5e8,stroke:#2e7d32,stroke-width:2px
    classDef storageClass fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef monitorClass fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef externalClass fill:#f1f8e9,stroke:#558b2f,stroke-width:2px

    class U1,U2,U3 userClass
    class WEB,EDITOR,MONITOR,API_DOC frontendClass
    class DP_API,WF_MGR,WF_EXEC,FUNC_REG,SCHEDULER,WORKER,QUEUE,DATA_API,FILE_MGR,STREAM serviceClass
    class PG_MAIN,PG_READ,REDIS,REDIS_CACHE,REDIS_SESSION,MINIO,FILES,RESULTS,TSDB,METRICS storageClass
    class PROMETHEUS,GRAFANA,JAEGER,ELK monitorClass
    class IOT,SENSORS,ERP,MES,THIRD_API externalClass