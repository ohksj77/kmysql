#!/bin/bash

# KMySQL 실행 스크립트
# 빌드 후 JAR 파일로 실행

echo " KMySQL 빌드 및 실행 스크립트"
echo "================================"

# 현재 디렉토리가 프로젝트 루트인지 확인
if [ ! -f "build.gradle.kts" ]; then
    echo " 오류: build.gradle.kts 파일을 찾을 수 없습니다."
    echo " 프로젝트 루트 디렉토리에서 실행해주세요."
    exit 1
fi

if [ ! -x "./gradlew" ]; then
    echo " 오류: gradlew 파일이 실행 가능하지 않습니다."
    echo " 실행 권한을 부여해주세요: chmod +x gradlew"
    exit 1
fi

echo " 프로젝트 빌드 중..."
./gradlew build -x test --quiet

if [ $? -eq 0 ]; then
    echo " 빌드 성공!"
    
    JAR_FILE="build/libs/kmysql-1.0-SNAPSHOT.jar"
    if [ -f "$JAR_FILE" ]; then
        echo " JAR 파일 실행 중..."
        echo "================================"
        java -jar "$JAR_FILE"
    else
        echo " 오류: JAR 파일을 찾을 수 없습니다: $JAR_FILE"
        exit 1
    fi
else
    echo " 빌드 실패!"
    exit 1
fi
