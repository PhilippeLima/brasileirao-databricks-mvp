# MVP – Pipeline de Dados do Campeonato Brasileiro (Brasileirão) no Databricks

## Descrição
Este trabalho constrói um pipeline de dados utilizando tecnologias na nuvem (Databricks Community Edition).  
O pipeline envolve **busca → coleta → modelagem → carga → análise** de dados do Campeonato Brasileiro (Série A).

---

## Objetivo do MVP (problema)
Construir um pipeline ponta a ponta para avaliar desempenho dos clubes no Brasileirão e responder perguntas sobre:
- vantagem de mando de campo
- tendência de gols ao longo do tempo
- desempenho por clube e por temporada

### Perguntas que desejo responder (não remover)
1. Existe vantagem de jogar em casa? Como ela varia por temporada?
2. Quais clubes têm maior aproveitamento de pontos por temporada?
3. Como evoluiu a média de gols por partida ao longo dos anos?
4. Quais clubes empatam mais e como isso muda por temporada?
5. Quais são os placares mais comuns e como variam por temporada?
6. Existe relação entre saldo de gols e pontos por temporada?

---

## Plataforma
Databricks Community Edition.

---

## 1) Busca pelos dados (dataset escolhido)
Dataset público do Brasileirão mantido por **adaoduque**, com cobertura de **2003 até 2024**.

- GitHub (repositório do dataset):
  - https://github.com/adaoduque/Brasileirao_Dataset
- Kaggle (descrição do dataset):
  - https://www.kaggle.com/datasets/adaoduque/campeonato-brasileiro-de-futebol

Arquivo usado no MVP:
- `campeonato-brasileiro-full.csv`

> Licença: validar e registrar no README conforme os termos exibidos na página do Kaggle/GitHub no momento da entrega (print em `/docs/screenshots/`).

---

## 2) Coleta (ingestão na nuvem)
- Download do CSV via `curl` dentro do notebook do Databricks.
- Armazenamento no DBFS em: `dbfs:/FileStore/brasileirao/raw/`

---

## 3) Modelagem (Esquema Estrela)
### Camadas
- **Bronze**: dados brutos em Delta – `brasileirao_bronze.matches_raw`
- **Silver**: dados limpos/enriquecidos – `brasileirao_silver.matches`
- **Gold**: Data Warehouse (Esquema Estrela)
  - `brasileirao_gold.dim_team`
  - `brasileirao_gold.dim_date`
  - `brasileirao_gold.dim_season`
  - `brasileirao_gold.fact_match`

### Linhagem (data lineage)
`GitHub/Kaggle CSV → DBFS (raw) → Bronze (Delta) → Silver (limpeza + regras) → Gold (dimensões + fatos) → análises SQL`

---

## 4) Carga (ETL) – ordem de execução
1. `notebooks/01_ingest_bronze.py`
2. `notebooks/02_transform_silver.py`
3. `notebooks/03_load_gold.py`
4. `notebooks/04_analysis.sql`

---

## 5) Análise
### a) Qualidade de dados (por atributo)
- nulos por coluna
- faixas esperadas (gols >= 0)
- consistência (mandante != visitante)
- checagens simples de coerência (ex.: vencedor vs placar)

### b) Solução do problema
As respostas são obtidas com SQL a partir de `brasileirao_gold.fact_match` + dimensões.

---

## Evidências (prints obrigatórios)
Salvar em `docs/screenshots/`:
- DBFS com o CSV baixado
- `SHOW DATABASES` e `SHOW TABLES` (bronze/silver/gold)
- amostras `display(...)`
- queries de qualidade
- queries respondendo as perguntas do objetivo
- página do dataset (licença/termos)

---

## Como reproduzir (passo a passo)
1. Criar conta no Databricks Community Edition
2. Criar cluster
3. Importar os notebooks da pasta `/notebooks`
4. Executar na ordem indicada

---

## Autoavaliação (preencher)
- Perguntas respondidas: (listar)
- Perguntas não respondidas (e por quê): (listar)
- Limitações do dataset e melhorias futuras: (listar)
