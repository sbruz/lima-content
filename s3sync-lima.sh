#!/bin/bash

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Абсолютный путь к папке, где лежит скрипт
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Источник (относительно папки проекта)
SRC_RELATIVE="${1:-export/}"
SRC_DIR="$PROJECT_DIR/$SRC_RELATIVE"

# Подпапка назначения в бакете
S3_SUBPATH="${2:-}"

# S3 параметры
DEST_BUCKET="lima-storage"
ENDPOINT="https://fra1.digitaloceanspaces.com"
S3_TARGET="s3://$DEST_BUCKET/$S3_SUBPATH"

# Флаги sync
S3_SYNC_FLAGS="--acl public-read --exclude \".*\" --exclude \"**/.*\""

# Флаг перезаписи
if [[ "$3" == "--overwrite" ]]; then
    echo -e "${YELLOW}⚠️  Перезапись включена.${NC}"
else
    S3_SYNC_FLAGS="$S3_SYNC_FLAGS --exact-timestamps"
    echo -e "${YELLOW}📁 Без перезаписи: будут загружены только новые или изменённые файлы.${NC}"
fi

# Время начала
START_TIME=$(date +%s)

echo -e "${GREEN}🚀 Начинаю синхронизацию...${NC}"
echo "📁 Источник:     $SRC_DIR"
echo "☁️  Назначение:  $S3_TARGET"
echo "⚙️  Флаги:       $S3_SYNC_FLAGS"
echo

# Выполняем sync
eval aws --profile lima_credentials --endpoint-url "$ENDPOINT" s3 sync "\"$SRC_DIR\"" "$S3_TARGET" $S3_SYNC_FLAGS | tee "$PROJECT_DIR/s3sync.log"

# Время окончания
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo
echo -e "${GREEN}✅ Завершено за $DURATION секунд.${NC}"
echo "📄 Лог: $PROJECT_DIR/s3sync.log"
