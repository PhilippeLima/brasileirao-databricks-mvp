# Catálogo de Dados – Brasileirão (campeonato-brasileiro-full.csv)

## Origem
Dataset público do Brasileirão (Série A) de 2003 a 2024, mantido por adaoduque.

Fontes:
- https://github.com/adaoduque/Brasileirao_Dataset
- https://www.kaggle.com/datasets/adaoduque/campeonato-brasileiro-de-futebol

Arquivo utilizado: `campeonato-brasileiro-full.csv`

---

## Tabelas do pipeline
- **Bronze:** `brasileirao_bronze.matches_raw` (Delta, bruto)
- **Silver:** `brasileirao_silver.matches` (Delta, limpo/enriquecido)
- **Gold:** `brasileirao_gold.*` (dimensões + fato)

---

## Dicionário de dados (CSV → Silver)

| Coluna (CSV) | Tipo (Silver) | Descrição | Domínio / Regras esperadas |
|---|---|---|---|
| ID | BIGINT | Identificador da partida/registro | > 0; único |
| rodata | INT | Rodada | 1..38 (típico); pode variar por temporada |
| data | DATE (match_date) | Data do jogo | conversão `dd/MM/yyyy`; ideal não nulo |
| hora | STRING | Horário do jogo | HH:MM; pode ser nulo |
| mandante | STRING | Time mandante | não nulo; trim/padronização |
| visitante | STRING | Time visitante | não nulo; trim/padronização |
| vencedor | STRING | Vencedor ou "-" | {mandante, visitante, "-"} |
| arena | STRING | Estádio | pode ser nulo |
| mandante_Placar | INT | Gols do mandante | >= 0 |
| visitante_Placar | INT | Gols do visitante | >= 0 |
| mandante_Estado | STRING | UF do mandante | 2 letras; pode ser nulo |
| visitante_Estado | STRING | UF do visitante | 2 letras; pode ser nulo |

---

## Colunas derivadas (Silver/Gold)

| Coluna | Tipo | Regra / Cálculo |
|---|---|---|
| season_year | INT | `year(match_date)` |
| goal_diff | INT | `mandante_Placar - visitante_Placar` |
| result_home | STRING | W/D/L conforme placar |
| home_points | INT | 3(W) / 1(D) / 0(L) |
| away_points | INT | 0(W mandante) / 1(D) / 3(L mandante) |

---

## Regras de Qualidade (checks)
- placares `>= 0`
- `mandante != visitante`
- `result_home` consistente com o placar
- pontos consistentes com `result_home`
