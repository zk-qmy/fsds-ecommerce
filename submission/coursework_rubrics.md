# EDAI Coursework Rubric

This document defines the grading rubric for an EDAI (data engineering / MLOps) coursework, with two parts: a mini-coursework and a final coursework. Each item below lists the requirement, the required proof/evidence to submit, and its point value. Text is in Vietnamese/English as written by the instructor. Use this to understand exactly what deliverables are expected and how they are scored.


## rubic (mini-coursework)


> **General instructions:** Viết file README.md để giới thiệu về business domain mà mọi người đang làm và high-level system deployment diagram cho module này Mọi người lưu ý: + README.md cần có Repo Structure (thể hiện file/folder này làm gì),Table of Contents chia ra từng sections để người dùng dễ đọc! + Các hàm/class phải có docstring để mô tả hàm đó làm gì, đầu file phải có mô tả file đó làm những gì + mỗi thành phần chính của diagram phải là 1 deloyable unit, ví dụ Kafka là 1 deployable unit, còn Feast SDK không phải deployable unit vì không thể deploy - Xem ví dụ về diagram của các khoá trước ở đây. + mũi tên nên đi theo đường truyền của dữ liệu, ví dụ nếu data truyền từ A đến B thì mũi tên đi từ A đến B, và dữ liệu thể hiện bên trên mũi tên. Còn nếu A chỉ gọi tới B, không truyển dữ liệu gì thì mũi tên từ A đến B. + các mũi tên nên có description và đánh số thứ tự, nếu có nhiều user flow, ví dụ end user và developer thì có thể thể hiện các màu mũi tên khác nhau và đánh số thứ tự riêng cho từng flow (ví dụ cả 2 ông đều đi từ 1->n) + hạn chế sử dụng mũi tên nét đứt, thường dùng cho các luồng k phải luồng chính (tức luồng liên quan trực tiếp tới các user)


### Engineering Fundamentals


#### Docker & Docker Compose

- **Có sử dụng Docker & Docker Compose** — **1.0 pts**
  - *Proof required:* Document Docker image đã reduce từ bao nhiêu tới bao nhiêu thông qua phương pháp optimize gì
- **Optimize Dockerfile (ví dụ multistage build)** — **2.0 pts**

### Implement Data Generator


#### Simulate offline data problems

- **Simulate skew** — **2.0 pts**
  - *Proof required:* Capture màn hình output để summarize chất lượng dữ liệu đã generate - Skew distribution (city/category %) - Cardinality: approx_count_distinct by ID - Schema evolution: nulls in old partitions - Duplicate rate before/after dedup - Streaming burst/late/duplicate rates ) + Document data characteristic (ví dụ data volume, data format sau lưu trữ) + Document config generator
- **Simulate high cardinality** — **2.0 pts**
- **Simulate schema evolution** — **2.0 pts**
- **Simulate another offline data problem (Ví dụ: 2% duplicate rate)** — **2.0 pts**
- **Using generator configuration** — **2.0 pts**
- **Store data after simulating so that we can ingest to Bronze zone later (Ví dụ: Dùng MinIO/PostgreSQL để mô phỏng data đang ở department khác và mình cần kéo về)** — **2.0 pts**

#### Simulate streaming data problems

- **Simulate burst** — **2.0 pts**
  - *Proof required:* Tương tự như Simulate offline data
- **Simulate late arrivals** — **2.0 pts**
- **Simulate another streaming data problem (Ví dụ: 1.5% duplicate rate)** — **2.0 pts**
- **Using generator configuration ( Ví dụ: n_customers: 120000 n_products: 45000 days_history: 180 )** — **2.0 pts**

### Processing Jobs


#### Spark job to handle offline data problems

- **Baseline (without optimization)** — **2.0 pts**
  - *Proof required:* Document giải thích từng step optimize từ baseline như thế nào, dùng Spark UI như thế nào (với screenshots để thấy rõ vấn đề), và đã tích hợp vào Airflow pipeline ra sao
- **Handle skew with explanation (Ví dụ: Do data bị skew như thế này, tôi thấy từ Spark UI có vấn đề như này, tôi sẽ có các phương pháp thử nghiệm như thế này, và sau khi thử nghiệm thì nó tăng ntn so với baseline)** — **3.0 pts**
- **Handle high cardinality with explanation** — **3.0 pts**
- **Handle schema evolution with explanation** — **3.0 pts**
- **Handle other offline data problem with explanation** — **3.0 pts**
- **Spark job được tích hợp vào các data pipeline** — **2.0 pts**

#### Flink job to handle streaming data problems (later in final coursework, we will merge this streaming pipeline into the feature store)

- **Baseline (without optimization)** — **2.0 pts**
  - *Proof required:* Document giải thích từng step optimize từ baseline như thế nào, dùng Flink UI như thế nào (với screenshots để thấy rõ vấn đề)
- **Handle burst with explanation** — **3.0 pts**
- **Handle late arrival with explanation** — **3.0 pts**
- **Handle other streaming problem with explanation** — **3.0 pts**
- **Window processing** — **2.0 pts**
  - *Proof required:* Capture đoạn code thể hiện khả năng xử lý Window trong Flink

### Data Storage


#### How to optimize your data storage

- **Lakehouse (ví dụ compaction để gom nhiều file nhỏ thành file lớn, z-order hoặc partitioning)** — **2.0 pts**
  - *Proof required:* Capture đoạn code và phân tích đã optimize được những gì so với chưa optimize
- **Datawarehouse (ví dụ indexing)** — **2.0 pts**

### Data Pipeline Orchestration (all connection and variables should be put inside Airflow to reuse across pipelines!)


#### Pipeline to ingest raw data into bronze zone (DP1)

- **Ingest stage** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên Airflow UI thể hiện các stage và thứ tự trong pipeline
- **Validate stage** — **2.0 pts**

#### Pipeline to ingest data from bronze zone into silver and gold zone (or bronze -> gold only) (DP2)

- **Ingest stage** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên Airflow UI thể hiện các stage và thứ tự trong pipeline
- **Validate stage** — **2.0 pts**

#### Pipeline to compute offline feature table (DP3) (Ví dụ: f_customer_total_orders_90d)

- **Ingest stage** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên Airflow UI thể hiện các stage và thứ tự trong pipeline
- **Validate stage** — **2.0 pts**

### Data Governance


#### DP1 linked with related tables

- **Lineage between the pipeline and tables** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên DataHub UI thể hiện lineage, validation và data contract
- **Data validation and data contract** — **2.0 pts**

#### DP2 linked with related tables

- **Lineage between the pipeline and tables** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên DataHub UI thể hiện lineage, validation và data contract
- **Data validation and data contract** — **2.0 pts**

#### DP3 linked with related tables

- **Lineage between the pipeline and tables** — **2.0 pts**
  - *Proof required:* Capture màn hình pipeline trên DataHub UI thể hiện lineage, validation và data contract
- **Data validation and data contract** — **2.0 pts**

### Documentation (tất cả documents để trong folder `docs/`, và ở README.md thì mọi người link tới mấy document này nhé. README.md chỉ summarize thôi, và nếu ai muốn đọc chi tiết các document optimize hoặc schema design thì đọc thêm ở trong docs/) (các capture màn hình cũng nên đính ở trong document để mô tả rõ mục đích từng cái muốn thể hiện điều gì, thay vì chỉ để một đống ảnh trong folder và để reviewer tự hiểu, tốt nhất nên có 1 file doc trong mỗi phần to trong folder docs/, ví dụ IaC.md thể hiện các output/document của step này, hoặc Security.md, etc.)


#### Schema design

- **Visualize tables on all zones** — **2.0 pts**
  - *Proof required:* Capture màn hình trên DBeaver
- **Dim table with SCD 2 (valid_from_ts, valid_to_ts, is_current)** — **2.0 pts**
- **Feature tables (feat_ tables) with 2 columns event_timestamp and created** — **2.0 pts**
- **Relationship between dim & fact tables (You can simply export via DBeaver or similar tools)** — **2.0 pts**
- **Naming convention (- Gold layer: `dim_`, `fact_`, `obt_`, `feat_`, `raw`, prefix or similar) (- Bronze & Silver layer: `raw_`, `stg_` prefix or similar)** — **2.0 pts**

### Novel ideas (không nhất thiết tự sáng tạo ra cái gì, có thể nghiên cứu dùng thêm các công cụ, hoặc kỹ thuật gì đó không được dạy ở EDAI) (Ví dụ: Nghiên cứu thêm Airbyte job to handle data ingestion)


#### Idea 1

- (see above) — **5.0 pts**
  - *Proof required:* Document idea + proof it worked!

#### Idea 2

- (see above) — **5.0 pts**
  - *Proof required:* Document idea + proof it worked!

**Total Points: 100.0**


## rubic final-coursework (final -


> **General instructions:** Viết file README.md để giới thiệu về business domain mà mọi người đang làm và high-level system deployment diagram (diagram tổng thể, kết hợp kiến thức cả khoá nha) Mọi người lưu ý: + README.md cần có Repo Structure (thể hiện file/folder này làm gì),Table of Contents chia ra từng sections để người dùng dễ đọc! + Các hàm/class phải có docstring để mô tả hàm đó làm gì, đầu file phải có mô tả file đó làm những gì + mỗi thành phần chính của diagram phải là 1 deloyable unit, ví dụ Kafka là 1 deployable unit, còn Feast SDK không phải deployable unit vì không thể deploy - Xem ví dụ về diagram của các khoá trước ở đây. + mũi tên nên đi theo đường truyền của dữ liệu, ví dụ nếu data truyền từ A đến B thì mũi tên đi từ A đến B, và dữ liệu thể hiện bên trên mũi tên. Còn nếu A chỉ gọi tới B, không truyển dữ liệu gì thì mũi tên từ A đến B. + các mũi tên nên có description và đánh số thứ tự, nếu có nhiều user flow, ví dụ end user và developer thì có thể thể hiện các màu mũi tên khác nhau và đánh số thứ tự riêng cho từng flow (ví dụ cả 2 ông đều đi từ 1->n) + hạn chế sử dụng mũi tên nét đứt, thường dùng cho các luồng k phải luồng chính (tức luồng liên quan trực tiếp tới các user)


### Web API kéo dữ liệu (Web API này sẽ dùng để kéo dữ liệu từ Online Feature store dựa trên ID (user_id, hoặc customer_id, etc.), sau đó gửi tới 1 ML inference engine)


#### Có sử dụng FastAPI + data validation (với pydantic)

- **Có sử dụng FastAPI + data validation (với pydantic) + healthcheck for k8s** — **2.0 pts**
  - *Proof required:* Capture màn hình cách mọi người handle rolling update và fall back
- **Sử dụng async** — **2.0 pts**
- **Deploy to k8s with helm + rollingupdate + auto fallback (see --atomic)** — **2.0 pts**

### Web API cho Real-time Drift Detection


#### Có sử dụng FastAPI + data validation (với pydantic)

- **Có sử dụng FastAPI + data validation (với pydantic) + healthcheck for k8s** — **2.0 pts**
  - *Proof required:* Capture màn hình cách mọi người handle rolling update và fall back
- **Sử dụng async** — **2.0 pts**
- **Deploy to k8s with helm + rollingupdate + auto fallback (see --atomic)** — **2.0 pts**

### Autoscale


#### Autoscale Web API kéo dữ liệu và drift detection (ví dụ dùng với KEDA để scale theo req)

- **Web API kéo dữ liệu** — **2.0 pts**
  - *Proof required:* Capture màn hình cách mọi người handle autoscale và demonstrate it worked!
- **Web API cho drift detection** — **2.0 pts**

### Validation & Verification


#### Autoscale Web API kéo dữ liệu và drift detection (ví dụ dùng với KEDA để scale theo req)

- **Unit test với Test Coverage > 90% (với proof bằng screenshot thể hiện khả năng test Web API, và sử dụng fixture, mock)** — **2.0 pts**
  - *Proof required:* Capture màn hình đã chạy test với test coverage thoả mãn yêu cầu
- **Có sử dụng kỹ thuật equivalence partitioning vs boundary value analysis khi thiết kế test cases để parametrize. (Cái này chưa được dạy, nhưng đơn giản lắm, cả nhà coi ở đây)** — **2.0 pts**
  - *Proof required:* Capture màn hình để thể hiện đã sử dụng các kỹ thuật này
- **Có sử dụng mutation testing để đánh giá hiệu quả của các test (cái này đã dạy ở lớp rồi nha cả nhà, là cái https://mutmut.readthedocs.io/). Note that you mutate only code changed, not the whole code base everytime you push your commit! Mutation score > 80%.** — **2.0 pts**
  - *Proof required:* Capture màn hình để thể hiện đã sử dụng các kỹ thuật này
- **Idempotency testing sử dụng property-based testing (chuyên để tìm ra cái test case có thể tạo ra bug), ví dụ, để kiểm tra xem model prediction có generate ra kết quả consistent sau nhiều lần chạy không. You can use the library hypothesis for property-based testing, combine with crosshair backend to find bugs easier.** — **2.0 pts**
  - *Proof required:* Capture màn hình để thể hiện đã sử dụng các kỹ thuật này
- **Load test the Web API (cái Web API kéo dữ liệu) to understand throughput (req/s) and latency (generate a html file with locust as SLA)** — **2.0 pts**
  - *Proof required:* Capture màn hình HTML output

### Improve the Data Generator


#### Simulate data drift

- **Simulate data drift** — **2.0 pts**
  - *Proof required:* Capture màn hình thể hiện generator configuration và bảng sau khi merge id và label
- **Using generator configuration** — **2.0 pts**

#### Tạo bảng label có 2 cột id và label. Bảng này sẽ dùng để join với các bảng feature cho training. (cột id có thể đổi thành user_id, customer_id hay gì đó tuỳ mọi người)

- (see above) — **2.0 pts**

### Feature Store


#### Build materialize pipeline & jobs for push streaming data into feature stores

- **Build data pipeline (ví dụ Airflow) để incremental materialize dữ liệu mới nhất từ offline qua online store** — **2.0 pts**
  - *Proof required:* + Capture màn hình thể hiện các data pipeline và thứ tự các stage trên Airflow + Capture màn hình thể hiện 2 job đang chạy, và thể hiện output đã chạy thành công + Document tại sao chọn TTL như thế
- **Deploy job chịu trách nhiệm push streaming feature (ở mini-coursework) vào OFFLINE store** — **1.0 pts**
- **Deploy job chịu trách nhiệm push streaming feature (ở mini-coursework) vào ONLINE store** — **1.0 pts**
- **Define TTL cho từng bảng feature, giải thích tại sao chọn TTL như thế** — **2.0 pts**

### ML


#### Jupyter notebook to demonstrate basic understanding of ML/DL

- **Jupyter notebook để kéo dữ liệu từ offline store (thông qua Feast) để tạo ra model (dùng ML/DL). Jupyter notebook cần đảm bảo ít nhất các step sau: + Load data từ offline store thông qua Feast (nhớ merge với bảng label đã generate ở bước Improve the Data Generator nhé cả nhà!) + Chia data thành trainining set và validation set + Train model trên training dataset (không cần độ chính xác cao, tuy nhiên cần phù hợp task, ví dụ bài regression lại dùng model classification!) + Đánh giá trên validation dataset + Save model (ví dụ lưu ra dạng .joblib)** — **2.0 pts**
  - *Proof required:* Document các step chính đã làm trong Jupyter notebook

### ML Pipelines


#### Training Pipeline

- **Chuyển đổi code từ Jupyter notebook thành training pipeline với số lượng step giống ở phần notebook bên trên (sử dụng Kubeflow pipeline, cái này có thể deploy riêng mà không cần deploy cả Kubeflow).** — **2.0 pts**
  - *Proof required:* Capture training pipeline đã run thành công + Capture màn hình thể hiện có sử dụng distributed training
- **Trong training pipeline, ở step train, thay bằng distributed training, ví dụ nếu mọi người dùng model XGBoost thì có thể coi tutorial này.** — **2.0 pts**

### Versioning


#### Model Versioning

- **Khi training pipeline hoàn tất, thông tin của MODEL (weight, hyperparam) cần được lưu trữ lại ở Model Registry (qua MLFlow)** — **2.0 pts**
  - *Proof required:* Model đã được version và lưu trữ lại + Data tương ứng cũng được version theo cơ chế incremental

#### Data Versioning

- **Mỗi lần kéo dữ liệu từ Feast về để training, cần version lại DATA theo cơ chế incremental (ví dụ lần train thứ 2 data chỉ có 1 chút thay đổi thì chỉ lưu trữ lại phần thay đổi này thôi, tránh việc lưu full data của cả lần 1 và lần 2)** — **2.0 pts**

### CI/CD (CI/CD = test + build + auto-deploy) (all secrets should be saved in Jenkins or similar tools instead of putting it inside your code!)


#### CI/CD cho pipelines

- **Materialize Pipeline** — **2.0 pts**
  - *Proof required:* Capture màn hình từng CI/CD pipeline đã run thành công
- **Training Pipeline** — **2.0 pts**
- **DP 1 (xem ở sheet mini-coursework)** — **2.0 pts**
- **DP 2 (xem ở sheet mini-coursework)** — **2.0 pts**
- **DP 3 (xem ở sheet mini-coursework)** — **2.0 pts**

#### CI/CD cho API (CI/CD cần kéo model ở trạng thái production hoặc được gắn tag production từ model registry về, đóng gói và deploy)

- **Web API (với FastAPI)** — **2.0 pts**
- **Inference Engine (ví dụ KServe)** — **1.0 pts**
- **Cho real-time drift detection Web API (sử dụng KNative Eventing kết hợp với KServe)** — **1.0 pts**

#### CI/CD cho jobs

- **Job 1: Push stream feature to OFFLINE store** — **1.0 pts**
- **Job 2: Push stream feature to ONLINE store** — **1.0 pts**

### Routing & Gateway (NGINX Ingress Controller)


#### Các service cần được hide đằng sau gateway

- **Service để coi metric (ví dụ Grafana)** — **2.0 pts**
  - *Proof required:* Capture màn hình thể hiện từng setup đã thành công
- **Service để coi log (ví dụ Kibana)** — **2.0 pts**
- **Service để coi trace (ví dụ Jaeger)** — **2.0 pts**
- **Web API kéo dữ liệu** — **2.0 pts**

#### Basic authentication & rate limit ở gateway (username/password)

- **Setup authentication & rate limit cho Web API kéo dữ liệu** — **2.0 pts**

#### Setup domain & enable HTTPS

- **Làm cái này cho Web API kéo dữ liệu. Mọi người có thể tham khảo 2 đồ án sau: cái này và cái này** — **1.0 pts**

### IaC


#### Dùng Terraform để setup GKE hoặc các cloud services (để ý cách chia folder theo từng service nếu có, ví dụ như sau)

- (see above) — **2.0 pts**
  - *Proof required:* Capture màn hình thể hiện từng setup đã chạy thành công

#### Dùng Ansible để configure và deploy các service lên VM (cần chia thành các role để code clean hơn)

- (see above) — **2.0 pts**

### Observability


#### Web API metrics

- **Web API monitoring metrics such as req/s, num. of reqs, num. of failures, etc.** — **2.0 pts**
  - *Proof required:* Capture màn hình thể hiện các data đã được capture, có thể coi trên các dashboard

#### Computing telemetry data (such as CPU, RAM, disk, network, etc.)

- **Collect và visualize metrics với Prometheus + Grafana (hoặc tool tương tự)** — **2.0 pts**
- **Tương tự cho logs** — **2.0 pts**
- **Tương tự cho traces** — **2.0 pts**

#### ML-related telemetry data

- **Airflow data drift pipeline to periodically pull data from offline feature store and compare with groundtruth and update to Grafana dashboard (via PushGateway). Assumption: we don't have groundtruth to monitor drift here!** — **1.0 pts**
- **Trigger retrain by calling Kubeflow API (you can design by adding one step at the end of the Airflow data drift pipeline)** — **1.0 pts**

### A/B Testing


#### When you deploy a new model, don't replace the old one directly, you should do A/B test, monitor it, and deploy. It's simple, take a look here

- **Perform A/B traffic split for 2 versions of inference service (deploy via CI/CD)** — **1.0 pts**
  - *Proof required:* Capture màn hình thể hiện cách mọi người thực hiện A/B test và dashboard để đánh giá kết quả A/B test
- **Setup monitoring dashboard to monitor 2 versions. Assumption: we also don't have groundtruth to monitor drift here!** — **1.0 pts**

### Security


#### Centralize secret management

- **Centralize secret management via Hashi Corp Vault (or similar tools) so it will be used across the whole organization.** — **1.0 pts**
  - *Proof required:* Capture màn hình thể hiện cách mọi người thực hiện centrailize secret management

#### Service to service authentication

- **Using service mesh to authorize access from service to service** — **1.0 pts**
  - *Proof required:* Capture màn hình thể hiện cách mọi người thực hiện authorize service-to-service

### Repository Design


#### Clean Code + clean repo + demonstrate the use of Design Pattern

- (see above) — **2.0 pts**
  - *Proof required:* Capture màn hình thể hiện Design Pattern đã được sử dụng trong code

### Documentation (tất cả documents để trong folder `docs/`, và ở README.md thì mọi người link tới mấy document này nhé. README.md chỉ summarize thôi, và nếu ai muốn đọc chi tiết các document optimize hoặc schema design thì đọc thêm ở trong docs/) (các capture màn hình cũng nên đính ở trong document để mô tả rõ mục đích từng cái muốn thể hiện điều gì, thay vì chỉ để một đống ảnh trong folder và để reviewer tự hiểu, tốt nhất nên có 1 file doc trong mỗi phần to trong folder docs/, ví dụ IaC.md thể hiện các output/document của step này, hoặc Security.md, etc.)


#### Low-level ML Design

- **Propose 5 key classes to present our main focus: Example: class TrainingDataService: def read_training_table(self) -> DataFrame: ... def validate_schema(self, df: DataFrame) -> None: ... def dedup_by_created_ts(self, df: DataFrame) -> DataFrame: ... class SplitService: def get_split_boundaries(self, df: DataFrame) -> dict: ... def split_by_time(self, df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]: ...** — **1.0 pts**
  - *Proof required:* Document 5 key classes

### Novel ideas (không nhất thiết tự sáng tạo ra cái gì, có thể nghiên cứu dùng thêm các công cụ, hoặc kỹ thuật gì đó không được dạy ở EDAI)


#### Idea 1

- (see above) — **2.0 pts**
  - *Proof required:* Document idea + proof it worked!

#### Idea 2

- (see above) — **2.0 pts**
  - *Proof required:* Document idea + proof it worked!

**Total Points: 100.0**