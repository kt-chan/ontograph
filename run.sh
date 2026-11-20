# 1. 启动基础设施
docker compose up -d

# 2. 建表 + 灌数据
PGPASSWORD=antifraud psql -h localhost -U antifraud -d postgres -f ./scripts/init.sql
python ./scripts/seed.py

psql -h localhost -U antifraud -d antifraud -c "SELECT count(*) FROM transactions;"
# 预期 ≥ 2000

# 3. 安装依赖
pip install fastapi uvicorn pydantic-settings gqlalchemy neo4j psycopg2-binary structured2graph

# 4. 配置 .env（填写 OPENAI_API_KEY）
cp .env.example .env && vi .env

# 5. 启动 FastAPI
uvicorn app.main:app --reload --reload-dir app --port 8000

# 6. 触发 LLM 迁移
curl -X POST localhost:8000/migrate \
  -H "Content-Type: application/json" \
  -d '{"strategy":"llm","mode":"automatic"}'

# 7. 查看图本体
curl localhost:8000/schema | python -m json.tool
# 预期出现 Bank、Account、Transaction 等节点标签，
# 以及 SENT_TO / RECEIVED_BY / FLAGGED_WITH / IN_BATCH 等边类型（LLM 策略下命名可能略有不同，但语义应覆盖）

# 8. 导出 Neo4j CSV
curl "localhost:8000/export/neo4j"

# 9. 检查 CSV 头部
head -1 exports/<ts>/nodes.csv          # 应以 id:ID 开头，末尾 :LABEL
head -1 exports/<ts>/relationships.csv  # 应以 :START_ID,...,:END_ID,:TYPE 开头

# 10. 加载到 Neo4j
python scripts/load_to_neo4j.py --export-dir exports/<ts> --force
