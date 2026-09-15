# JusExpertia — Análise Jurisprudencial de Danos Morais

## MVP de Engenharia de Dados em Nuvem — Databricks Lakehouse

**Tema:** análise jurisprudencial de indenizações por dano moral em casos de negativação indevida  
**Tribunais:** TJAM, TJPA, TJPE e TJSE  
**Período analítico principal:** 2020–2025  
**Plataforma:** Databricks Free Edition  
**Tecnologias:** Apache Spark / PySpark, SQL, Delta Lake, Unity Catalog, Databricks Dashboards e IA generativa para validação semântica

---

## Resumo executivo

O **JusExpertia** implementa um pipeline de dados de ponta a ponta em nuvem para transformar jurisprudência pública em informação analítica estruturada, rastreável e auditável.

O problema de negócio parte de uma dificuldade prática: decisões judiciais sobre **negativação indevida e indenização por dano moral** contêm informações relevantes dispersas em textos não estruturados, como valor da indenização, resultado recursal, fundamento jurídico, órgão julgador, data e referência oficial. A leitura manual de milhares de decisões é lenta, pouco escalável e sujeita a inconsistências.

O MVP foi estruturado em arquitetura Lakehouse/Medalhão, com ingestão, tratamento, classificação, auditoria de qualidade, enriquecimento por IA e visualização em dashboard. O trabalho partiu de um corpus local de **1.224.133 registros**, realizou pré-seleção temática de **21.371 candidatos**, produziu uma camada analítica qualificada com **1.617 julgados de 2020 a 2025** e aplicou validação semântica por IA em uma **amostra estratificada de 677 casos**.

O resultado final inclui tabelas Delta persistidas no Databricks, controles de qualidade, catálogo/linhagem e um dashboard publicado denominado **JusExpertia — Análise Jurisprudencial de Danos Morais**.

> A solução não substitui a análise jurídica humana. O objetivo é demonstrar, em escala de MVP, como Engenharia de Dados pode transformar documentos jurídicos públicos em um ativo analítico reproduzível e útil para apoio à decisão.

---

# 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

## 1.1 Problema de negócio

Profissionais jurídicos precisam responder perguntas como: qual o valor típico do dano moral, como esse valor varia entre tribunais, quais fundamentos aparecem com maior frequência, quando a indenização é majorada/reduzida/mantida/reconhecida e como os valores evoluem no tempo.

O problema central foi:

> **Como construir um pipeline de dados em nuvem capaz de organizar, qualificar e analisar jurisprudência sobre negativação indevida e dano moral, permitindo comparar valores, resultados recursais, fundamentos jurídicos e comportamento dos tribunais de forma reproduzível e auditável?**

## 1.2 Objetivo geral

Construir um pipeline no Databricks que:

1. receba dados jurisprudenciais previamente coletados de fontes públicas oficiais;
2. preserve a rastreabilidade da origem;
3. realize limpeza, padronização, deduplicação e classificação temática;
4. extraia indicadores jurídicos e monetários;
5. produza tabelas analíticas em Delta Lake;
6. aplique controles de qualidade;
7. utilize uma amostra estratificada para validação semântica por IA;
8. disponibilize os resultados em dashboard.

## 1.3 Perguntas de negócio

**P1. Qual é o valor típico da indenização por dano moral?**  
Usar média, mediana, P25, P75 e faixas de valor.

**P2. Existem diferenças relevantes entre TJAM, TJPA, TJPE e TJSE?**  
Comparar quantidade de casos, média, mediana, faixas, resultado recursal e fundamentos.

**P3. Como os valores evoluíram entre 2020 e 2025?**  
Analisar a mediana anual por tribunal.

**P4. Qual é a distribuição dos resultados recursais?**  
Categorias: `MAJORADO`, `MANTIDO`, `RECONHECIDO`, `REDUZIDO` e, na camada de IA, `NAO_DETERMINADO` quando necessário.

**P5. Quais fundamentos jurídicos aparecem com maior frequência?**

**P6. Qual é a cobertura de rastreabilidade por URL oficial?**

**P7. Até que ponto regras heurísticas de classificação/extração divergem de uma análise semântica por IA?**

## 1.4 Contexto dos dados brutos

Corpus de origem:

| Tribunal | Registros |
|---|---:|
| TJSE | 393.078 |
| TJPA | 145.465 |
| TJAM | 101.746 |
| TJPE | 583.844 |
| **Total** | **1.224.133** |

Pré-seleção temática:

| Tribunal | Candidatos |
|---|---:|
| TJSE | 11.971 |
| TJPA | 1.277 |
| TJAM | 2.629 |
| TJPE | 5.494 |
| **Total** | **21.371** |

A pré-seleção foi tratada como candidatura temática, e não como caso confirmado. Isso evita, por exemplo, interpretar a expressão “não houve negativação” como negativação positiva.

## 1.5 Licença e uso

Os dados derivam de decisões disponibilizadas publicamente em portais oficiais dos tribunais. Não foi assumida uma licença aberta única e padronizada para todos os portais. O uso foi acadêmico/demonstrativo, com preservação de proveniência e URL oficial quando disponível.

O repositório público disponibiliza **código e documentação**, não sendo necessário republicar o corpus bruto.

---

# 2. Carga dos Dados (Etapa 4.2)

## 2.1 Fluxo de entrada

```text
Portais públicos dos tribunais
        ↓
Coleta e consolidação local
        ↓
SQLite — corpus jurisprudencial
        ↓
Pré-seleção temática
        ↓
Carga no Databricks
        ↓
Tabelas Delta / Unity Catalog
```

## 2.2 Carga para a nuvem

O ambiente utilizado foi o **Databricks Free Edition**. Os dados foram persistidos no catálogo:

```text
workspace.default
```

Para o enriquecimento por IA, o CSV final com 677 resultados foi enviado a um Managed Volume:

```text
/Volumes/workspace/default/jus_expertia_mvp/resultados_deepseek_677.csv
```

A carga foi validada antes da persistência:

```text
TOTAL_LINHAS = 677
TOTAL_COLUNAS = 81
IDS_UNICOS = 677
TJAM = 138
TJPA = 139
TJPE = 200
TJSE = 200
```

## 2.3 Gates de carga

```text
GATE_TABELA_FINAL = PASS
GATE_ANALYTICS = PASS
GATE_COMPARACAO_DEEPSEEK = PASS
GATE_KPIS_DEEPSEEK = PASS
```

## 2.4 Referência ao código

> Ajustar para os nomes finais do GitHub.

```text
/notebooks/
  01_JusExpertia_Diagnostico_Dados.py
  02_JusExpertia_Transformacao.py
  03_JusExpertia_Gold.py
  04_JusExpertia_Qualidade.py
  05_JusExpertia_DeepSeek_Analytics.py

/scripts/
  processar_deepseek_677.py
  diagnosticar_4_faltantes.py
  reprocessar_4_faltantes.py
```

**Screenshots nesta seção:** Managed Volume; validação 677/677; Catalog Explorer com tabelas persistidas.

---

# 3. Modelagem e Catálogo de Dados (Etapa 4.3)

## 3.1 Arquitetura

```text
FONTES PÚBLICAS
      ↓
CORPUS LOCAL SQLITE
      ↓
BRONZE — preservação + proveniência
      ↓
SILVER — limpeza + normalização + classificação + qualidade
      ↓
GOLD — analytics + amostra IA + KPIs + dashboard
```

O modelo é predominantemente **flat analítico por conceito**, adequado ao Lakehouse e às perguntas do MVP.

## 3.2 Principais tabelas

| Tabela | Camada | Granularidade | Finalidade |
|---|---|---|---|
| `bronze_jurisprudencia_negativacao` | Bronze | 1 linha por candidato | Preservar dados jurisprudenciais e proveniência |
| `gold_judicial_analytics_base` | Gold | 1 linha por julgado qualificado | Base analítica principal |
| `gold_amostra_llm_2020_2025` | Gold | 1 linha por caso da amostra | Amostra estratificada |
| `gold_deepseek_677_final` | Gold | 1 linha por caso analisado por IA | Resultado completo da inferência |
| `gold_deepseek_677_analytics` | Gold | 1 linha por caso | Versão tipada para análise |
| `gold_deepseek_677_kpis` | Gold | 1 linha agregada | KPIs da validação por IA |
| `gold_dashboard_*` | Gold | agregada | Fontes específicas do dashboard |

## 3.3 Campos centrais da Bronze

| Campo | Tipo lógico | Descrição / domínio |
|---|---|---|
| `id` / `id_origem` | inteiro/string | Identificador na origem |
| `tribunal` | string | TJAM, TJPA, TJPE, TJSE |
| `processo` | string | Número do processo |
| `numero_acordao` | string | Número do acórdão, quando disponível |
| `classe` | string | Classe processual |
| `relator` | string | Relator |
| `data_julgamento` | data/string | Data do julgamento |
| `data_publicacao` | data/string | Data da publicação |
| `orgao` | string | Órgão julgador |
| `ementa` | string | Ementa |
| `url_oficial` | string | URL oficial, quando disponível |
| `fonte` | string | Proveniência |
| `provider` | string | Provedor/coletor |
| `content_hash` | string | Hash para identidade/deduplicação |
| `texto_busca` | string | Texto preparado para busca |

## 3.4 Campos centrais da Gold principal

| Campo | Tipo | Descrição |
|---|---|---|
| `tribunal` | string | Tribunal |
| `id_origem` | string | Identificador da origem |
| `processo` | string | Processo |
| `data_referencia` | date | Data normalizada |
| `ano` | int | Ano |
| `ementa` | string | Ementa |
| `url_oficial` | string | URL conhecida |
| `tem_url_oficial` | boolean/int | Indicador de cobertura |
| `resultado_dano_moral` | string | Resultado recursal |
| `valor_final_dano_moral_v5` | double | Valor final extraído |
| `flag_valor_v5` | string | Status de qualidade |
| `faixa_valor` | string | Faixa monetária |
| `fundamentos_detectados` | string/array | Fundamentos jurídicos |

Domínio principal de `resultado_dano_moral`:

```text
MAJORADO
MANTIDO
RECONHECIDO
REDUZIDO
```

## 3.5 Campos centrais da Gold com IA

| Campo | Tipo | Descrição |
|---|---|---|
| `id_llm` | string | Identificador determinístico |
| `tribunal` | string | Tribunal |
| `processo` | string | Processo |
| `ano` | int | Ano |
| `fonte_analise_llm` | string | EMENTA ou EMENTA_E_INTEIRO_TEOR |
| `inteiro_teor_obtido_bool` | boolean | Inteiro teor recuperado |
| `resultado_regra` | string | Classificação heurística |
| `resultado_llm` | string | Classificação semântica |
| `valor_regra` | double | Valor da regra |
| `valor_final_dano_moral_llm` | double | Valor da IA |
| `confianca_llm` | double | Confiança declarada |
| `revisao_humana_bool` | boolean | Necessidade de revisão |
| `divergencia_resultado_bool` | boolean | Divergência de resultado |
| `divergencia_valor_bool` | boolean | Divergência de valor |
| `fundamentos_decisao_llm` | string/JSON | Fundamentos estruturados |
| `fundamentos_quantum_llm` | string/JSON | Fundamentos do quantum |
| `qtd_evidencias_removidas` | int | Evidências rejeitadas pela auditoria |
| `qtd_fontes_corrigidas` | int | Fontes corrigidas |
| `total_tokens` | long | Tokens utilizados |
| `tempo_llm_segundos` | double | Tempo de inferência |

## 3.6 Linhagem

```text
Fonte oficial
  ↓
SQLite local
  ↓
pré-seleção temática
  ↓
bronze_jurisprudencia_negativacao
  ↓
limpeza + classificação + extração
  ↓
gold_judicial_analytics_base
  ↓
filtro 2020–2025 + gates V5
  ↓
1.617 casos qualificados
  ↓
amostra estratificada
  ↓
gold_amostra_llm_2020_2025 (677)
  ↓
DeepSeek + auditoria literal
  ↓
gold_deepseek_677_final
  ↓
gold_deepseek_677_analytics
  ↓
gold_deepseek_677_kpis
  ↓
Dashboard publicado
```

> **Para nota máxima em Modelagem:** anexar no README um apêndice com **todas as colunas de todas as tabelas finais**, incluindo nome, tipo, descrição, domínio e linhagem. Esse apêndice deve ser gerado a partir dos schemas reais do Databricks para evitar qualquer campo inventado.

**Screenshots nesta seção:** Unity Catalog, schema das tabelas, descrições e lineage.

---

# 4. Pipeline de Dados (Etapa 4.4)

## 4.1 Estágios

### 1. Ingestão
Persistência dos candidatos temáticos na camada Bronze.

### 2. Padronização
Normalização de texto, datas, tribunal, campos vazios, URL e metadados.

### 3. Classificação temática
Uso de termos fortes/contextuais e regras para evitar falsos positivos como “não houve negativação”.

### 4. Extração monetária
Identificação e classificação de valores, distinguindo valor final de dano moral de pedido, honorários, dano material, valor da causa etc.

### 5. Gold analítica
Conjunto 2020–2025 com **1.617 casos**:

| Tribunal | Casos |
|---|---:|
| TJAM | 138 |
| TJPA | 139 |
| TJPE | 556 |
| TJSE | 784 |
| **Total** | **1.617** |

Todos os 1.617 utilizados no dashboard principal estavam com `flag_valor_v5 = OK`.

### 6. Amostragem estratificada

```text
TJAM 138
TJPA 139
TJPE 200
TJSE 200
TOTAL 677
```

A amostra buscou equilíbrio por tribunal, resultado, faixa de valor e ano, sem duplicação artificial.

### 7. Enriquecimento semântico

Modelo:

```text
deepseek-ai/DeepSeek-V4-Flash-0731
```

Entrada: ementa em todos os casos e inteiro teor quando recuperável. Saída: JSON estruturado por schema.

Resultado:

```text
677 / 677 casos processados
GATE_677 = PASS
```

### 8. Auditoria de evidências
Evidências literais retornadas pela IA foram verificadas contra o texto efetivamente enviado. Evidências não encontradas foram removidas e fontes inconsistentes foram corrigidas.

### 9. Persistência final

```text
workspace.default.gold_deepseek_677_final
workspace.default.gold_deepseek_677_analytics
workspace.default.gold_deepseek_677_kpis
```

Gates:

```text
TOTAL_TABELA = 677
GATE_TABELA_FINAL = PASS
TOTAL_ANALYTICS = 677
IDS_UNICOS_ANALYTICS = 677
GATE_ANALYTICS = PASS
```

### 10. Dashboard
Dashboard publicado como:

> **JusExpertia — Análise Jurisprudencial de Danos Morais**

Seções:

1. **Análise Jurisprudencial — Universo de 1.617 casos qualificados**
2. **Validação por IA — Amostra estratificada de 677 casos**

---

# 5. Qualidade de Dados (Etapa 4.5)

## 5.1 Completude
Foram avaliados nulos/ausências em ementa, processo, data, valor, URL e inteiro teor.

Cobertura de URL oficial no universo principal:

```text
65,62%
```

URLs ausentes não foram inventadas.

## 5.2 Consistência
Padronização de datas, tribunais, categorias de resultado, valores e booleanos.

## 5.3 Unicidade
Na amostra de IA:

```text
TOTAL_LINHAS = 677
IDS_UNICOS = 677
```

## 5.4 Acurácia contextual
Problemas detectados:

- falso positivo semântico: “não houve negativação”;
- valor incorreto: honorários, dano material, pedido ou valor anterior;
- resultado recursal incorreto: `RECONHECIDO` versus `MANTIDO`.

Tratamento: regras refinadas, validação semântica por IA, auditoria literal e revisão humana.

## 5.5 Outliers
Foram utilizados média, mediana, P25, P75 e faixas de valor para reduzir distorções.

## 5.6 Qualidade da IA

```text
337 casos com inteiro teor
340 somente ementa
9 casos para revisão humana
224 divergências de resultado
71 divergências de valor
```

## 5.7 Limitações

- cobertura desigual de inteiro teor entre tribunais;
- URLs genéricas/ausentes em algumas fontes;
- documentos nem sempre extraíveis;
- IA aplicada a 677 e não aos 1.617;
- confiança declarada pelo modelo não é probabilidade calibrada;
- uso jurídico real exige revisão humana.

---

# 6. Análise de Dados (Etapa 4.5)

## 6.1 Visão geral

```text
Casos analisados: 1.617
Média geral: R$ 5.686,48
Mediana geral: R$ 5.000
P25: R$ 4.000
P75: R$ 7.000
Cobertura com URL oficial: 65,62%
```

A mediana inferior à média indica influência de valores superiores na cauda da distribuição. Metade dos casos se concentra aproximadamente entre R$ 4.000 e R$ 7.000.

## 6.2 P1 — Valor típico
**Resposta:** aproximadamente **R$ 5.000** como valor central, com intervalo interquartil de R$ 4.000 a R$ 7.000.

## 6.3 P2 — Diferenças entre tribunais

| Tribunal | Média aprox. | Mediana aprox. |
|---|---:|---:|
| TJAM | R$ 6,79 mil | R$ 5 mil |
| TJPA | R$ 8,26 mil | R$ 5 mil |
| TJPE | R$ 6,38 mil | R$ 5 mil |
| TJSE | R$ 4,54 mil | R$ 4 mil |

O TJPA tem a maior média, enquanto o TJSE apresenta valores centrais inferiores.

## 6.4 P3 — Evolução temporal
A mediana anual por tribunal mostra comportamento não uniforme entre 2020 e 2025. Não há evidência, no conjunto analisado, de uma trajetória única de alta ou queda comum aos quatro tribunais.

## 6.5 P4 — Resultado recursal

| Tribunal | MAJORADO | MANTIDO | RECONHECIDO | REDUZIDO |
|---|---:|---:|---:|---:|
| TJAM | 47 | 27 | 39 | 25 |
| TJPA | 27 | 31 | 37 | 44 |
| TJPE | 87 | 150 | 246 | 73 |
| TJSE | 271 | 84 | 187 | 242 |

Há padrões diferentes entre os tribunais, com forte presença de `RECONHECIDO` no TJPE e de `MAJORADO`/`REDUZIDO` no TJSE.

## 6.6 P5 — Fundamentos
Foram observados fundamentos recorrentes relacionados a dano in re ipsa, razoabilidade/proporcionalidade, responsabilidade objetiva, falha do serviço, fraude de terceiro, enriquecimento sem causa, caráter pedagógico e Súmula 385/STJ.

## 6.7 P6 — URL oficial
**65,62%** do universo principal possui URL oficial registrada. Esse indicador foi tratado como métrica de proveniência/auditabilidade.

## 6.8 P7 — Regra versus IA

```text
Divergências de resultado: 224 / 677 = 33,09%
Divergências de valor: 71 / 677 = 10,49%
Casos para revisão humana: 9
```

Divergência de resultado por tribunal:

| Tribunal | Total | Divergências | % |
|---|---:|---:|---:|
| TJAM | 138 | 40 | 28,99% |
| TJPA | 139 | 49 | 35,25% |
| TJPE | 200 | 98 | 49,00% |
| TJSE | 200 | 37 | 18,50% |

Médias regra × IA:

| Tribunal | Regra | IA |
|---|---:|---:|
| TJAM | R$ 6.789,59 | R$ 6.488,72 |
| TJPA | R$ 8.256,01 | R$ 6.471,01 |
| TJPE | R$ 6.047,07 | R$ 5.993,64 |
| TJSE | R$ 4.542,50 | R$ 4.389,45 |

A análise mostrou que regras baseadas apenas em padrões textuais podem capturar valor pedido, honorários, dano material ou valor anterior. Também podem confundir `RECONHECIDO` com `MANTIDO`.

## 6.9 Discussão geral
O pipeline respondeu ao problema inicial e mostrou que a combinação de Engenharia de Dados, regras determinísticas e IA com auditoria é superior ao uso isolado de uma única técnica.

---

# 7. Autoavaliação

## 7.1 Atingimento dos objetivos

Ao final deste MVP, considero que consegui atingir o objetivo principal que havia traçado no início do trabalho. Minha proposta era construir um pipeline funcional de dados em nuvem capaz de organizar, tratar e analisar jurisprudência relacionada a danos morais decorrentes de negativação indevida, e esse objetivo foi alcançado.

Consegui estruturar o fluxo completo no Databricks, desde a carga dos dados até a persistência das tabelas em Delta Lake, passando por etapas de transformação, controle de qualidade, criação das camadas analíticas, construção do catálogo de dados, análise estatística e publicação de um dashboard.

Além disso, ampliei o escopo inicial ao incluir uma etapa de validação semântica por inteligência artificial. Essa etapa me permitiu comparar classificações heurísticas com uma análise contextual mais aprofundada, principalmente quanto ao resultado recursal e ao valor final da indenização.

Entendo, portanto, que o projeto não apenas atendeu ao objetivo acadêmico de construção de um pipeline de dados de ponta a ponta, mas também produziu uma base concreta para uma aplicação jurídica mais ampla no futuro.

## 7.2 Dificuldades encontradas

Durante o desenvolvimento, enfrentei dificuldades que foram importantes para o meu aprendizado.

Uma das principais foi a heterogeneidade dos portais dos tribunais. Cada tribunal disponibiliza suas decisões de maneira diferente, com estruturas próprias de URL, mecanismos de consulta e níveis distintos de acesso ao inteiro teor.

Também encontrei dificuldades relacionadas à própria natureza do texto jurídico. As decisões são documentos não estruturados e frequentemente apresentam várias informações relevantes no mesmo texto. Um único acórdão pode mencionar, por exemplo, o valor pedido pela parte, o valor fixado em primeiro grau, o valor posteriormente reduzido ou majorado, além de valores relacionados a dano material, honorários e custas.

Outra dificuldade importante foi diferenciar corretamente o resultado recursal. Expressões presentes no texto nem sempre significam que o dano moral foi reconhecido naquele julgamento. Em muitos casos, o tribunal apenas mantém uma decisão anterior, o que exige uma interpretação contextual mais cuidadosa.

A recuperação do inteiro teor também foi um desafio. Nem todos os tribunais disponibilizam documentos de forma direta e padronizada, e por isso parte dos casos precisou ser analisada apenas com base na ementa.

Na etapa de inteligência artificial, enfrentei ainda situações de respostas extensas, truncamento e JSON incompleto. Isso exigiu criação de mecanismos de checkpoint, reprocessamento e validação posterior das respostas.

Essas dificuldades acabaram sendo valiosas porque me obrigaram a pensar não apenas na execução técnica, mas também na confiabilidade, rastreabilidade e qualidade do dado produzido.

## 7.3 Decisões que considero positivas

Algumas decisões tomadas durante o projeto foram especialmente importantes para aumentar a confiabilidade do resultado.

A criação de checkpoints no processamento por IA permitiu evitar a perda do trabalho já realizado em caso de falha ou interrupção.

Também optei por utilizar um identificador determinístico, o `id_llm`, o que facilitou a rastreabilidade de cada caso ao longo do processo.

Outra decisão importante foi utilizar uma amostra estratificada em vez de uma seleção aleatória simples. Dessa forma, consegui preservar maior diversidade entre tribunais, resultados recursais e faixas de valor.

Considerei também importante manter os resultados da regra heurística e da IA lado a lado, sem substituir um pelo outro. Isso permitiu medir divergências e identificar situações em que uma classificação puramente textual poderia ser insuficiente.

A auditoria literal das evidências retornadas pela IA foi outra decisão relevante. Sempre que uma evidência não podia ser localizada no texto efetivamente analisado, ela era removida. Isso reduziu o risco de manter justificativas sem suporte textual.

Também incluí um indicador de necessidade de revisão humana, porque considero inadequado tratar uma análise automatizada como infalível, especialmente em um contexto jurídico.

Por fim, considero positiva a separação clara entre o universo principal de 1.617 casos qualificados e a amostra de 677 casos analisada por IA. Essa distinção evita transmitir a impressão incorreta de que todos os casos passaram pela mesma etapa de enriquecimento semântico.

Na parte estatística, optei por utilizar mediana e quartis além da média, porque percebi que a média isolada poderia ser influenciada por valores extremos.

## 7.4 Limitações do trabalho

Reconheço que o projeto possui limitações.

A principal delas é que a análise por IA não foi aplicada aos 1.617 casos do universo qualificado, mas apenas a uma amostra estratificada de 677 casos.

Essa foi uma decisão deliberada. O objetivo deste MVP era demonstrar o funcionamento completo da arquitetura e validar o fluxo de processamento, e não necessariamente realizar a análise semântica de todo o universo disponível.

Também reconheço que a ausência de inteiro teor em parte dos processos limita a profundidade de algumas análises. Em determinados casos, a ementa oferece informação suficiente; em outros, uma análise jurídica mais completa dependeria do acesso ao conteúdo integral da decisão.

Outra limitação é que a confiança declarada pelo modelo de IA não deve ser interpretada como uma probabilidade estatística calibrada de acerto.

Por fim, este trabalho não pretende substituir a análise jurídica profissional. A IA foi utilizada como ferramenta de enriquecimento e apoio à classificação, e não como fonte autônoma de conclusão jurídica.

## 7.5 Trabalhos futuros

Vejo este MVP como uma primeira etapa de um projeto maior.

Como evolução futura, pretendo aplicar a análise semântica ao universo completo dos casos qualificados e ampliar a cobertura para outros tribunais brasileiros.

Também considero importante aperfeiçoar os mecanismos de recuperação do inteiro teor diretamente das fontes oficiais, aumentando a rastreabilidade e a qualidade das análises.

Outro passo relevante seria construir um processo sistemático de validação humana das respostas produzidas pela IA, permitindo medir com maior precisão a qualidade das classificações.

Pretendo ainda implementar uma rotina incremental de atualização dos dados, para que novas decisões possam ser incorporadas ao pipeline sem necessidade de reprocessar todo o conjunto histórico.

Também seria interessante desenvolver mecanismos de monitoramento contínuo da qualidade dos dados e de possíveis mudanças de padrão ao longo do tempo.

Por fim, uma evolução natural do projeto seria aprofundar a análise semântica de precedentes, fundamentos jurídicos e critérios de fixação do quantum indenizatório, transformando o JusExpertia em uma ferramenta cada vez mais útil para pesquisa jurisprudencial e apoio à análise jurídica.


# 8. Evidências visuais

Inserir screenshots com legenda e numeração:

1. Managed Volume com `resultados_deepseek_677.csv`;
2. validação `677 linhas / 677 ids`;
3. Catalog Explorer com tabelas;
4. `GATE_TABELA_FINAL = PASS`;
5. `GATE_ANALYTICS = PASS`;
6. comparação regra × IA;
7. seção **Análise Jurisprudencial — Universo de 1.617 casos qualificados**;
8. seção **Validação por IA — Amostra estratificada de 677 casos**;
9. dashboard publicado.

---

# 9. Repositório e organização do código

**Repositório:** `[INSERIR URL DO GITHUB]`

```text
jus-expertia-mvp/
├── README.md
├── notebooks/
├── scripts/
└── docs/
    └── screenshots/
```

Não publicar chaves, `.env`, tokens, pesos privados, adapters LoRA, datasets de treinamento privados ou prompts internos sensíveis.

`.gitignore` mínimo:

```text
.env
*.key
*.pem
__pycache__/
.DS_Store
```

---


# 10. Conclusão

O JusExpertia demonstra um pipeline funcional em nuvem que transforma jurisprudência pública não estruturada em informação analítica organizada.

```text
problema → coleta → nuvem → modelagem → ETL → qualidade → análise → dashboard → validação
```

A principal conclusão técnica é que um pipeline jurídico confiável não deve depender exclusivamente de palavras-chave, regex ou IA generativa. A combinação de **proveniência, transformação determinística, validação de qualidade, análise estatística, enriquecimento semântico e revisão humana** produz um resultado mais rastreável e defensável.
