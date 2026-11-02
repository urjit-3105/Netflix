# Netflix Distributed Resilience Microservice

A production-ready Spring Boot microservice designed for high-throughput distributed resilience, fault tolerance, and load protection using **Resilience4j** and **Spring Boot Actuator**.

---

## 🚀 Key Features

1. **Spring Boot 3.3.x & Java 17+**: Modern microservice baseline with high-performance concurrent processing.
2. **Resilience4j Circuit Breaker**:
   - Sliding window count-based failure tracking (10-request window, 50% threshold).
   - Fast fallback execution to prevent cascading failures in downstream streaming/catalog dependencies.
   - Automatic transition from `OPEN` to `HALF-OPEN` after 10 seconds.
3. **Sliding-Window Rate Limiter**:
   - Limits traffic surges to 5 requests per 10-second refresh period.
   - Smooth request shedding with automated user-friendly fallback response.
4. **Health Check & Actuator Monitoring**:
   - Exposes detailed health indicators and metrics (`/actuator/health`, `/actuator/circuitbreakers`, `/actuator/prometheus`).
5. **Load Simulation & Testing API**:
   - Built-in concurrent stress-testing endpoint (`/api/v1/catalog/simulate-load`) to evaluate circuit state transitions in real time.

---

## 🛠️ Tech Stack & Dependencies

- **Java**: 17+ (Compatible with JDK 21 / 25)
- **Framework**: Spring Boot 3.3.4
- **Modules**:
  - `spring-boot-starter-web`
  - `spring-boot-starter-actuator`
  - `spring-boot-starter-aop`
  - `resilience4j-spring-boot3` (2.2.0)
  - `micrometer-registry-prometheus`

---

## 📡 REST API Endpoints

### 1. Fetch Video Metadata
```http
GET /api/v1/catalog/video/{id}?fail={true/false}&delay={ms}
```
- Normal call: `GET /api/v1/catalog/video/movie-101`
- Simulate downstream failure: `GET /api/v1/catalog/video/movie-101?fail=true`
- Simulate downstream latency: `GET /api/v1/catalog/video/movie-101?delay=2500`

### 2. Resilience Status & Metrics
```http
GET /api/v1/catalog/status
```
Returns live circuit state (`CLOSED`, `OPEN`, `HALF_OPEN`), failure rate %, buffered call count, and rate limiter permission availability.

### 3. Concurrent Load & State Transition Simulation
```http
POST /api/v1/catalog/simulate-load?totalRequests=20&failRatePercent=70&concurrency=5
```
Simulates high-concurrency traffic with customizable failure injection to observe circuit opening and recovery.

### 4. Actuator Health & Metrics
- Health Check: `GET /actuator/health`
- Circuit Breaker Events: `GET /actuator/circuitbreakerevents`
- Prometheus Metrics: `GET /actuator/prometheus`

---

## 🏃 Running the Application

### Build with Maven
```bash
mvn clean package
```

### Run
```bash
mvn spring-boot:run
```
Service will be available on `http://localhost:8080`.
