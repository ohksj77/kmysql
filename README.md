# KMySQL 🚀

**Kotlin으로 구현한 MySQL 프로젝트**

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9+-blue.svg)](https://kotlinlang.org/)
[![Java](https://img.shields.io/badge/Java-17+-green.svg)](https://openjdk.org/)
[![Gradle](https://img.shields.io/badge/Gradle-8.0+-orange.svg)](https://gradle.org/)

KMySQL은 MySQL을 모티브로 만들어진 Kotlin 기반 DBMS입니다. 트랜잭션 관리, 동시성 제어, 복구 시스템 등 실제 데이터베이스의 핵심 기능들을 구현했습니다.

## 실행 예시

![](https://github.com/user-attachments/assets/70719c82-5073-4ef8-a3ae-11540c0e73b5)

## 🏗️ 아키텍처
```
┌─────────────────────────────────────────────────────────────┐
│                    KMySQL Database Engine                   │
├─────────────────────────────────────────────────────────────┤
│  JDBC Interface  │  SQL Parser  │  Query Planner  │  Index  │
├─────────────────────────────────────────────────────────────┤
│                    Transaction Manager                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ Concurrency │  │   Recovery  │  │   Version   │          │
│  │  Manager    │  │   Manager   │  │   Manager   │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
├─────────────────────────────────────────────────────────────┤
│                    Buffer Manager                           │
├─────────────────────────────────────────────────────────────┤
│                    File Manager                             │
└─────────────────────────────────────────────────────────────┘

```

## ✨ 주요 기능

### 🔄 트랜잭션 관리

- **ACID 속성 지원**: 원자성, 일관성, 격리성, 지속성
- **다중 격리 수준**: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- **세이브포인트 지원**: 트랜잭션 내 중간 상태 저장 및 복원
- **자동 커밋**: DDL/DML 명령어 후 자동 커밋 (테스트용)
- **수동 커밋**: `commit`, `rollback` 명령어 지원

### 🔒 동시성 제어

- **버전 기반 동시성 제어**: MVCC(Multi-Version Concurrency Control)
- **RID 기반 락킹**: 레코드 수준의 세밀한 동시성 제어
- **공유 락(S-Lock)과 배타 락(X-Lock)** 지원
- **데드락 감지**: 데드락 상황 감지 및 해결
- **락 타임아웃**: 무한 대기 방지

### 💾 버퍼 관리

- **LRU 기반 버퍼 교체 정책**
- **핀/언핀 메커니즘**: 버퍼 참조 카운트 관리
- **지연 쓰기**: 성능 최적화를 위한 지연 디스크 쓰기
- **버퍼 플러시**: 강제 디스크 쓰기 지원

### 📝 로그 기반 복구

- **WAL(Write-Ahead Logging)**: 데이터 일관성 보장
- **체크포인트**: 복구 시간 단축
- **롤백 복구**: 트랜잭션 실패 시 안전한 복구
- **시스템 크래시 복구**: 시스템 장애 시 데이터 복구

### 🗂️ 인덱스 지원

- **해시 인덱스**: 등호 검색 최적화
- **B-Tree 인덱스**: 범위 검색 및 정렬 지원
- **단일 컬럼 인덱스**: 기본 인덱스 기능

### 🔍 쿼리 처리

- **SQL 파서**: 기본 SQL 구문 파싱 (CREATE, INSERT, SELECT, UPDATE, DELETE)
- **기본 쿼리 플래너**: 단순한 실행 계획 생성
- **테이블 스캔**: 기본 테이블 스캔 연산
- **JDBC 인터페이스**: 임베디드 JDBC 드라이버

## 🛠️ 기술 스택

- **언어**: Kotlin 1.9+
- **런타임**: Java 17+
- **빌드 도구**: Gradle 8.0+

## 📋 시스템 요구사항

- **Java**: OpenJDK 17 이상
- **메모리**: 최소 512MB RAM
- **디스크**: 최소 100MB 여유 공간

## 🚀 빠른 시작

### 1. 프로젝트 클론

```bash
git clone https://github.com/ohksj77/kmysql.git
cd kmysql
```

### 2. 서버 빌드 & 실행

```bash
sh run.sh
```

## 💻 사용법

### 대화형 SQL 클라이언트

```bash
Connect> kmysql_db
```

#### 기본 SQL 명령어 예제

```sql
-- 테이블 생성
CREATE TABLE student
(
    sid     INT,
    sname   VARCHAR(20),
    majorid INT,
    gpa     INT
);

-- 데이터 삽입
INSERT INTO student
VALUES (1, '홍길동', 10, 38);
INSERT INTO student
VALUES (2, '김철수', 10, 35);
INSERT INTO student
VALUES (3, '이영희', 20, 39);

-- 데이터 조회
SELECT *
FROM student;
SELECT sname, gpa
FROM student;
SELECT sname
FROM student
WHERE majorid = 10;

-- 데이터 수정
UPDATE student
SET gpa = 40
WHERE sid = 1;

-- 데이터 삭제
DELETE
FROM student
WHERE sid = 3;

-- 트랜잭션 명령어
commit; -- 트랜잭션 커밋
rollback;
-- 트랜잭션 롤백

-- 종료
exit;
```

### 프로그래밍 방식 사용 (JDBC)

```kotlin
import kmysql.jdbc.embedded.EmbeddedDriver
import java.sql.*

fun main() {
    val driver = EmbeddedDriver()
    val connection: Connection = driver.connect("/tmp/kmysql_db", null)

    try {
        val statement: Statement = connection.createStatement()

        // 테이블 생성
        statement.executeUpdate(
            """
            CREATE TABLE users (
                id INT,
                name VARCHAR(20),
                email VARCHAR(50)
            )
        """
        )

        // 데이터 삽입
        val insertStmt = connection.prepareStatement(
            "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"
        )

        insertStmt.setInt(1, 1)
        insertStmt.setString(2, "홍길동")
        insertStmt.setString(3, "hong@example.com")
        insertStmt.executeUpdate()

        // 데이터 조회
        val resultSet = statement.executeQuery("SELECT * FROM users WHERE id = 1")
        while (resultSet.next()) {
            println("ID: ${resultSet.getInt("id")}")
            println("Name: ${resultSet.getString("name")}")
            println("Email: ${resultSet.getString("email")}")
        }

        // 트랜잭션 커밋
        connection.commit()

    } catch (e: Exception) {
        // 오류 발생 시 롤백
        connection.rollback()
        e.printStackTrace()
    } finally {
        connection.close()
    }
}
```

## 🧪 테스트

### 전체 테스트 실행

```bash
./gradlew test
```

### 특정 테스트 카테고리 실행

```bash
# 버퍼 및 동시성 테스트
./gradlew test --tests "kmysql.BufferAndConcurrencyTest"

# 인덱스 및 쿼리 테스트
./gradlew test --tests "kmysql.IndexAndQueryTest"

# 성능 및 스트레스 테스트
./gradlew test --tests "kmysql.PerformanceAndStressTest"

# 종합 트랜잭션 테스트
./gradlew test --tests "kmysql.ComprehensiveTransactionTest"
```

## 📁 프로젝트 구조

```
src/
├── main/kotlin/kmysql/
│   ├── buffer/              # 버퍼 관리 시스템
│   │   ├── Buffer.kt       # 버퍼 구현
│   │   ├── BufferManager.kt # 버퍼 관리자
│   │   └── BufferList.kt   # LRU 버퍼 리스트
│   ├── file/               # 파일 관리
│   │   ├── FileManager.kt  # 파일 관리자
│   │   ├── BlockId.kt      # 블록 ID
│   │   └── Page.kt         # 페이지 구현
│   ├── index/              # 인덱스 시스템
│   │   ├── hash/           # 해시 인덱스
│   │   │   └── HashIndex.kt
│   │   ├── btree/          # B-Tree 인덱스
│   │   │   ├── BTreeIndex.kt
│   │   │   ├── BTreeDir.kt
│   │   │   └── BTreeLeaf.kt
│   │   └── Index.kt        # 인덱스 인터페이스
│   ├── jdbc/               # JDBC 인터페이스
│   │   └── embedded/       # 임베디드 드라이버
│   │       ├── EmbeddedDriver.kt
│   │       ├── EmbeddedConnection.kt
│   │       └── EmbeddedStatement.kt
│   ├── log/                # 로그 관리
│   │   ├── LogManager.kt   # 로그 관리자
│   │   └── LogIterator.kt  # 로그 반복자
│   ├── metadata/           # 메타데이터 관리
│   │   ├── MetadataManager.kt # 메타데이터 관리자
│   │   ├── TableManager.kt # 테이블 관리자
│   │   └── IndexManager.kt # 인덱스 관리자
│   ├── parse/              # SQL 파서
│   │   ├── Parser.kt       # SQL 파서
│   │   ├── Lexer.kt        # 어휘 분석기
│   │   └── QueryData.kt    # 쿼리 데이터
│   ├── plan/               # 실행 계획
│   │   ├── Plan.kt         # 실행 계획 인터페이스
│   │   ├── TablePlan.kt    # 테이블 스캔 계획
│   │   ├── BasicQueryPlanner.kt # 기본 쿼리 플래너
│   │   └── Planner.kt      # 플래너
│   ├── query/              # 쿼리 처리
│   │   ├── Scan.kt         # 스캔 인터페이스
│   │   ├── TableScan.kt    # 테이블 스캔
│   │   └── SelectScan.kt   # 선택 스캔
│   ├── record/             # 레코드 관리
│   │   ├── RecordPage.kt   # 레코드 페이지
│   │   ├── Layout.kt       # 레이아웃
│   │   ├── Schema.kt       # 스키마 정의
│   │   └── Rid.kt          # 레코드 ID
│   ├── server/             # 서버 구현
│   │   ├── KMySQL.kt       # 메인 서버
│   │   └── RunServer.kt    # 실행 서버
│   ├── transaction/        # 트랜잭션 관리
│   │   ├── Transaction.kt  # 트랜잭션 구현
│   │   ├── concurrency/    # 동시성 제어
│   │   │   ├── ConcurrencyManager.kt
│   │   │   ├── VersionManager.kt
│   │   │   └── RecordLockManager.kt
│   │   └── recovery/       # 복구 시스템
│   │       ├── RecoveryManager.kt
│   │       └── LogRecord.kt
│   └── util/               # 유틸리티
│       └── ConsoleLogger.kt # 콘솔 로거
└── test/kotlin/kmysql/     # 테스트 코드
    ├── BufferAndConcurrencyTest.kt
    ├── IndexAndQueryTest.kt
    ├── PerformanceAndStressTest.kt
    └── ComprehensiveTransactionTest.kt
```

## ⚙️ 설정 및 튜닝

### 기본 설정값

```kotlin
// 버퍼 설정
val BUFFER_SIZE = 8                    // 버퍼 풀 크기
val BLOCK_SIZE = 400                   // 블록 크기 (bytes)

// 트랜잭션 설정
val LOCK_TIMEOUT = 10000L              // 락 타임아웃 (ms)
val DEFAULT_ISOLATION = REPEATABLE_READ // 기본 격리 수준

// 로그 설정
val LOG_FILE = "kmysql.log"            // 로그 파일명
val CHECKPOINT_INTERVAL = 1000         // 체크포인트 간격

// 자동 커밋 설정
val AUTO_COMMIT_DDL = true             // DDL 명령어 자동 커밋
val AUTO_COMMIT_DML = true             // DML 명령어 자동 커밋 (테스트용)
```

### 성능 튜닝 가이드

1. **버퍼 크기 조정**: 메모리 사용량과 성능의 균형
2. **락 타임아웃 설정**: 동시성과 응답성의 조절
3. **격리 수준 선택**: 일관성과 성능의 트레이드오프
4. **인덱스 전략**: 쿼리 패턴에 맞는 인덱스 설계

## 🔧 개발 환경 설정

### IntelliJ IDEA 설정

1. 프로젝트 열기
2. Gradle 프로젝트 동기화
3. Kotlin 플러그인 활성화
4. JUnit 테스트 실행 설정

### 디버깅

```bash
# 디버그 모드로 실행
./gradlew run --debug-jvm

# 특정 테스트 디버깅
./gradlew test --tests "kmysql.BufferAndConcurrencyTest.testConcurrentTransactions" --debug-jvm
```

## 📊 성능 벤치마크

### 기본 성능 지표

- **트랜잭션 처리량**: 기본 트랜잭션 처리 지원
- **동시 사용자**: 단일 사용자 모드 (임베디드)
- **데이터 크기**: 메모리 기반 테이블 지원
- **복구 시간**: 로그 기반 복구 시스템

### 벤치마크 실행

```bash
./gradlew test --tests "kmysql.PerformanceAndStressTest"
```

## 🐛 문제 해결

### 일반적인 문제들

1. **메모리 부족 오류**
    - 버퍼 크기 줄이기
    - JVM 힙 크기 증가: `-Xmx2g`

2. **락 타임아웃 오류**
    - 락 타임아웃 값 증가
    - 트랜잭션 크기 줄이기

3. **디스크 공간 부족**
    - 로그 파일 정리
    - 불필요한 데이터 삭제

### 로그 분석

```bash
# 로그 파일 확인
tail -f kmysql.log

# 오류 로그 필터링
grep "ERROR" kmysql.log
```
