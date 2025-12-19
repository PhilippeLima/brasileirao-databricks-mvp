# Linhagem de Dados (Data Lineage) – MVP Brasileirão

## Visão geral
Este MVP constrói um pipeline ponta a ponta (ingestão → bronze → silver → gold → análise) usando Databricks Community Edition.

---

## 1) Fonte (dataset público)
- Origem: repositório público / Kaggle (adaoduque)
- Arquivo: `campeonato-brasileiro-full.csv`
- Licença/termos: conforme a página do dataset (registrar o link no README)

---

## 2) Coleta e armazenamento (DBFS)
- Upload do CSV para o Databricks FileStore:
  - `dbfs:/FileStore/brasileirao/raw/campeonato-brasileiro-full.csv`

---

## 3) Bronze (Delta / camada bruta)
- Tabela: `brasileirao_bronze.matches_raw`
- Conteúdo: dados do CSV sem transformações profundas (apenas leitura e gravação em Delta)
- Objetivo: manter histórico fiel do dado original

---

## 4) Silver (Delta / camada tratada)
- Tabela: `brasileirao_silver.matches`
- Transformações principais:
  - padronização de strings (trim)
  - conversão de tipos (placares/rodada para inteiro)
  - conversão de data (`data` → `match_date`)
  - criação de colunas derivadas:
    - `season_year`, `goal_diff`, `result_home`, `home_points`, `away_points`

---

## 5) Gold (DW / modelo analítico)
Modelo sugerido: Esquema Estrela

### Dimensões
- `brasileirao_gold.dim_team` (times)
- `brasileirao_gold.dim_date` (datas)
- `brasileirao_gold.dim_season` (temporadas)

### Fato
- `brasileirao_gold.fact_match` (partidas)
  - chaves para time mandante, visitante, data e temporada
  - métricas: gols, pontos, resultado, diferença de gols

---

## 6) Consumo (análises)
- Consultas SQL e/ou notebooks para:
  - checagens de qualidade
  - responder perguntas do objetivo (man
