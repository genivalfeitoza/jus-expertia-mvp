# JusExpertia — Análise Jurisprudencial de Danos Morais

**MVP de Engenharia de Dados em Nuvem | Databricks Lakehouse, IA e Power BI**  
**Tema:** negativação indevida e indenização por dano moral  
**Tribunais:** TJAM · TJPA · TJPE · TJSE  
**Período analítico:** 2020–2025  
**Atualização desta documentação:** 23 de setembro de 2026

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)](https://www.databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark%20%7C%20SQL-Delta%20Lake-blue)](https://spark.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard%20p%C3%BAblico-F2C811)](https://powerbi.microsoft.com/)

## Acesso rápido

**[▶ Abrir o dashboard interativo do JusExpertia no Power BI](https://app.powerbi.com/view?r=eyJrIjoiYzIxZDRkMDEtNTQ2Zi00YTZmLWJjNzYtNGQ1NjE5N2FkYWQ2IiwidCI6IjFiYTk1NTkzLTIzZDctNGJhYS1hMWQ2LTNjNWMwMDZlMWQ4NSJ9)**

**[Repositório no GitHub](https://github.com/genivalfeitoza/jus-expertia-mvp)** · **[Notebooks](notebooks/)** · **[Scripts](scripts/)** · **[Documentação](docs/)** · **[Evidências visuais](docs/screenshots/)**

> **Status de entrega:** o pipeline e as tabelas analíticas foram produzidos no Databricks; o enriquecimento semântico de 677 casos foi concluído, auditado e importado; o relatório Power BI foi publicado com três páginas e teve um código de publicação na Web gerado. Convém testar o link em uma janela anônima para confirmar o acesso sem autenticação. A documentação do repositório distingue o que foi implementado do que permanece como melhoria futura.

## Resumo executivo

O **JusExpertia** é um MVP que transforma textos de decisões judiciais disponibilizadas publicamente em dados estruturados para pesquisa jurisprudencial, análises estatísticas e validação semântica por inteligência artificial. O projeto responde a um problema prático: um mesmo acórdão pode mencionar pedidos, valores anteriormente arbitrados, danos materiais, honorários e valor final da indenização, além de conter linguagem que dificulta a classificação automática do resultado recursal.

O fluxo parte de um corpus local com **1.224.133 registros** de quatro tribunais, pré-seleciona **21.371 candidatos temáticos** e organiza os dados em arquitetura Lakehouse no Databricks. No recorte analítico principal, **1.617 julgados qualificados** de 2020 a 2025 sustentam indicadores, filtros e gráficos. Uma **amostra estratificada separada de 677 casos** passou por processamento semântico com DeepSeek, controle de evidências textuais e comparação com classificações heurísticas.

O projeto apresenta resultados em duas interfaces complementares: um dashboard analítico no Databricks e um relatório interativo público no **Microsoft Power BI**, com as páginas **Visão Geral**, **Julgados** e **Detalhe do Processo**. As limitações da disponibilidade de documentos, das datas e dos links são indicadas explicitamente: uma URL cadastrada não equivale necessariamente a acesso direto ao inteiro teor.

| Indicador | Resultado | População / interpretação |
|:---|---:|:---|
| Corpus de origem | **1.224.133** | Registros locais dos quatro tribunais |
| Pré-seleção temática | **21.371** | Candidatos, não casos necessariamente confirmados |
| Universo qualificado | **1.617** | Recorte principal, 2020–2025, `flag_valor_v5 = OK` |
| Média / mediana | **R$ 5.686,48 / R$ 5.000,00** | Universo qualificado |
| P25 / P75 | **R$ 4.000,00 / R$ 7.000,00** | Universo qualificado |
| Registros com URL cadastrada | **65,62%** | Inclui links genéricos de consulta, quando existentes |
| Amostra submetida à IA | **677/677** | Amostra estratificada; não representa processamento integral dos 1.617 |
| Inteiro teor recuperado | **337** | Entre os 677; outros 340 foram analisados pela ementa |
| Revisão humana indicada | **9** | Entre os 677 |
| Divergências regra × IA | **224 resultados / 71 valores** | Discordâncias, não taxa de erro verificada |

> **Uso responsável:** resultados produzidos por regras ou LLM são apoio à pesquisa e dependem de conferência na fonte oficial e, para uso jurídico, de revisão profissional. O link Power BI criado por *Publicar na Web* é público; não deve ser utilizado para incluir informações sigilosas ou dados que não tenham sido avaliados para divulgação.

---

## Sumário

1. [Contexto de negócios e perguntas](#1-contexto-de-negócios-e-perguntas)
2. [Origem, coleta e carga dos dados](#2-origem-coleta-e-carga-dos-dados)
3. [Modelagem e catálogo de dados](#3-modelagem-e-catálogo-de-dados)
4. [Pipeline e enriquecimento por IA](#4-pipeline-e-enriquecimento-por-ia)
5. [Qualidade, rastreabilidade e limitações](#5-qualidade-rastreabilidade-e-limitações)
6. [Análises e resultados](#6-análises-e-resultados)
7. [Dashboards: Databricks e Power BI](#7-dashboards-databricks-e-power-bi)
8. [Estrutura e reprodutibilidade do repositório](#8-estrutura-e-reprodutibilidade-do-repositório)
9. [Evidências e documentação complementar](#9-evidências-e-documentação-complementar)
10. [Autoavaliação](#10-autoavaliação)
11. [Próximos passos](#11-próximos-passos)

---

# 1. Contexto de negócios e perguntas

### Problema

A pesquisa de indenizações por negativação indevida exige identificar e comparar, em documentos extensos e não estruturados, **resultado do recurso**, **quantum indenizatório**, **fundamentos jurídicos**, **data do julgamento** e **endereço oficial**. Uma leitura baseada exclusivamente em palavras-chave pode confundir uma alegação da parte com a conclusão do tribunal ou um valor pedido com aquele efetivamente fixado.

**Pergunta central:** como construir um pipeline em nuvem que organize, qualifique e analise esse conjunto de decisões de forma rastreável, permita exploração interativa e ajude a identificar divergências entre extração heurística e interpretação contextual por IA?

As perguntas de negócio do MVP são:

- **P1 — Quantum:** qual é a distribuição dos valores de dano moral, incluindo média, mediana, P25 e P75?
- **P2 — Tribunais:** como variam os indicadores entre TJAM, TJPA, TJPE e TJSE?
- **P3 — Tempo:** como os valores e volumes se comportam no recorte analítico de 2020 a 2025?
- **P4 — Resultado recursal:** qual é a distribuição de `MAJORADO`, `MANTIDO`, `RECONHECIDO` e `REDUZIDO`?
- **P5 — Fundamentos:** quais fundamentos jurídicos foram detectados com maior frequência?
- **P6 — Proveniência:** qual é a disponibilidade de URLs oficiais registradas?
- **P7 — Validação semântica:** onde as classificações e os valores extraídos por regras divergem daqueles identificados pela IA?

O projeto tem finalidade acadêmica e demonstrativa. Os dados de origem provêm de portais públicos, mas não se presume uma licença aberta única para todos os tribunais e documentos. O corpus integral não é republicado neste repositório.

# 2. Origem, coleta e carga dos dados

### 2.1 Corpus de origem

| Tribunal | Registros locais | Pré-seleção temática |
|:---|---:|---:|
| TJSE | 393.078 | 11.971 |
| TJPA | 145.465 | 1.277 |
| TJAM | 101.746 | 2.629 |
| TJPE | 583.844 | 5.494 |
| **Total** | **1.224.133** | **21.371** |

A pré-seleção reúne **candidatos** para o tema de negativação indevida e dano moral. Ela não equivale a confirmação jurídica de todos os registros: expressões como “não houve negativação”, por exemplo, precisam ser interpretadas no contexto.

### 2.2 Fluxo de ingestão

```text
Portais públicos dos tribunais
    ↓ coleta e consolidação local
Corpus SQLite, separado por tribunal
    ↓ pré-seleção temática
21.371 candidatos
    ↓ carga no Databricks Free Edition
workspace.default.bronze_jurisprudencia_negativacao
    ↓ transformações, normalização e classificação
Camadas analíticas Delta / Unity Catalog
    ↓ recorte de qualidade e período
1.617 julgados qualificados → dashboard
    ↓ amostragem estratificada
677 casos → processamento DeepSeek → auditoria → Delta → dashboards
```

O recorte `2020–2025` é aplicado ao **ano analítico da base**, relacionado à referência temporal normalizada. Isso não significa que todos os registros contenham a data exata de julgamento. Tampouco se deve confundir o ano inserido no número de um processo com o ano em que o recurso foi julgado.

### 2.3 Upload da saída de IA

O processamento por IA foi concluído em ambiente local e sua saída foi enviada ao Managed Volume no Databricks:

```text
/Volumes/workspace/default/jus_expertia_mvp/resultados_deepseek_677.csv
```

Validação do arquivo na carga:

| Verificação | Resultado |
|:---|---:|
| Linhas | 677 |
| Colunas | 81 |
| IDs `id_llm` únicos | 677 |
| TJAM | 138 |
| TJPA | 139 |
| TJPE | 200 |
| TJSE | 200 |

A persistência foi realizada nas tabelas `gold_deepseek_677_final`, `gold_deepseek_677_analytics` e `gold_deepseek_677_kpis`, em `workspace.default`. Os testes de carga e das tabelas analíticas retornaram `PASS`.

# 3. Modelagem e catálogo de dados

### 3.1 Arquitetura Lakehouse / Medalhão

| Camada | Papel no pipeline | Artefatos confirmados |
|:---|:---|:---|
| **Bronze** | Persistência dos candidatos, metadados e proveniência | `bronze_jurisprudencia_negativacao` |
| **Silver — etapa lógica** | Limpeza, normalização, deduplicação e classificação temática/monetária | Transformações do pipeline; consultar notebooks e catálogo para os nomes físicos existentes |
| **Gold** | Dados qualificados, amostra de IA, resultados tipados e KPIs | `gold_judicial_analytics_base`, `gold_amostra_llm_2020_2025`, `gold_deepseek_677_final`, `gold_deepseek_677_analytics`, `gold_deepseek_677_kpis` |

As etapas Bronze, Silver e Gold descrevem a organização lógica do processamento. **Este README não atribui um nome físico a uma tabela Silver sem confirmação pelo catálogo.** A tabela `gold_judicial_analytics_base` é uma base analítica a partir da qual se aplica o filtro dos 1.617 casos utilizados no dashboard principal; não se deve confundir a contagem total de uma tabela com a contagem de seu subconjunto qualificado.

### 3.2 Principais campos

| Campo | Origem / uso | Interpretação |
|:---|:---|:---|
| `id_origem`, `processo`, `numero_acordao` | Ingestão / Gold | Identificadores para rastreabilidade; um processo pode ter mais de uma decisão |
| `tribunal` | Todas as camadas | TJAM, TJPA, TJPE ou TJSE |
| `classe`, `relator`, `orgao` | Ingestão | Metadados processuais, quando disponíveis |
| `data_julgamento`, `data_publicacao` | Ingestão / apresentação | Datas distintas, ambas passíveis de ausência |
| `data_referencia`, `ano` | Gold | Referência temporal normalizada e ano analítico |
| `ementa` | Ingestão / análise | Texto-base disponível em todos os casos analisados por IA |
| `url_oficial` | Ingestão / Power BI | Endereço oficial registrado; nem sempre link direto ao inteiro teor |
| `content_hash` | Ingestão | Apoio à identificação e deduplicação |
| `resultado_dano_moral` | Gold principal | Resultado recursal por regra heurística |
| `valor_final_dano_moral_v5`, `flag_valor_v5` | Gold principal | Valor extraído e respectivo status de qualidade |
| `id_llm`, `resultado_llm`, `valor_final_dano_moral_llm` | Gold IA | Identificador determinístico e respostas estruturadas da IA |
| `inteiro_teor_obtido_bool`, `revisao_humana_bool` | Gold IA | Proveniência do documento e sinalização de revisão |
| `divergencia_resultado_bool`, `divergencia_valor_bool` | Gold IA | Comparação regra × IA |

O Unity Catalog organiza as tabelas persistidas, seus esquemas e a consulta de metadados. Uma documentação de catálogo **exaustiva** deve usar os esquemas reais exportados do Databricks, sem criar campos, tipos, descrições ou relações não verificados.

### 3.3 Linhagem resumida

```text
SQLite local / portais oficiais
  → pré-seleção de 21.371 candidatos
  → bronze_jurisprudencia_negativacao
  → limpeza / extração / validação
  → gold_judicial_analytics_base
       ├─ recorte 2020–2025 + V5 OK: 1.617 → análise principal
       └─ amostra estratificada: 677
            → DeepSeek + auditoria literal
            → gold_deepseek_677_final
            → gold_deepseek_677_analytics
            → gold_deepseek_677_kpis
            → análise de divergências e visualização
```

# 4. Pipeline e enriquecimento por IA

### 4.1 Tratamento e seleção

A transformação combina normalização textual e temporal, padronização de tribunais, tratamento de ausências, análise temática contextual e extração monetária. As regras procuram distinguir o **valor final** de dano moral de valor pleiteado, honorários, dano material, valor da causa e quantias de decisões anteriores.

Após os filtros de período e de qualidade monetária (`flag_valor_v5 = OK`), o universo principal contém:

| Tribunal | Casos qualificados |
|:---|---:|
| TJAM | 138 |
| TJPA | 139 |
| TJPE | 556 |
| TJSE | 784 |
| **Total** | **1.617** |

### 4.2 Amostragem e modelo

A amostra de **677 casos** contém todos os registros qualificados de TJAM (138) e TJPA (139) e uma seleção de 200 registros de TJPE e 200 de TJSE. A seleção considerou tribunal, categoria de resultado e faixas de valor, com ordenação determinística e sem duplicação artificial. **Não é uma amostra proporcional ao tamanho de todos os tribunais**; as análises de IA devem ser interpretadas dentro desse desenho amostral.

O enriquecimento foi realizado pelo modelo `deepseek-ai/DeepSeek-V4-Flash-0731`, via API da Together AI, com entrada estruturada e exigência de saída JSON. Todos os casos utilizaram a ementa; o inteiro teor foi incorporado somente quando recuperado de uma fonte oficial pública e processável. O modelo não navega automaticamente pelos links, e não houve tentativa de contornar autenticação, CAPTCHA ou bloqueios dos portais.

| Tribunal | Casos na amostra | Inteiro teor recuperado | Casos para revisão humana |
|:---|---:|---:|---:|
| TJAM | 138 | 0 | 7 |
| TJPA | 139 | 139 | 1 |
| TJPE | 200 | 0 | 1 |
| TJSE | 200 | 198 | 0 |
| **Total** | **677** | **337** | **9** |

Nos demais **340 casos**, a análise foi feita com a ementa. Para TJPA, foi utilizada, quando aplicável, uma interface pública de consulta por identificador de documento; para TJSE, relatórios oficiais acessíveis diretamente. TJAM e TJPE ficaram com ementa no processamento atual.

### 4.3 Resiliência, reprocessamento e auditoria

Foram implementados checkpoint por caso, recuperação de respostas JSON incompletas e reprocessamento dirigido de quatro casos que não haviam sido concluídos na primeira passagem. Após a recuperação, o processamento atingiu `677/677` (`GATE_677 = PASS`).

A auditoria **EVIDENCIA_LITERAL_V1** comparou as evidências textuais devolvidas pela IA com os textos efetivamente enviados ao modelo. Quando não havia correspondência literal, a evidência foi removida; rótulos de origem foram corrigidos quando o trecho estava presente em outra parte da entrada. No processamento consolidado:

- **315** evidências sem suporte literal foram removidas;
- **13** referências de fonte foram corrigidas;
- **9** casos permaneceram sinalizados para revisão humana.

Em uma verificação manual inicial de **10 casos**, as classificações de resultado e os valores da IA coincidiram com a auditoria realizada nessa amostra. Isso **não é uma taxa de acerto generalizável** para todos os 677 casos.

### 4.4 Saídas persistidas

```text
workspace.default.gold_deepseek_677_final
workspace.default.gold_deepseek_677_analytics
workspace.default.gold_deepseek_677_kpis
```

Os resultados heurísticos foram preservados ao lado das respostas da IA. Nenhuma divergência é automaticamente interpretada como erro da regra ou acerto do modelo sem validação jurídica adicional.

# 5. Qualidade, rastreabilidade e limitações

A qualidade foi examinada quanto a **completude, consistência, unicidade, plausibilidade de valores, referências oficiais e suporte literal das evidências**.

| Controle | Aplicação / resultado |
|:---|:---|
| **Completude** | Avaliação de ementa, processo, datas, valores, URLs e inteiro teor; ausências preservadas, não preenchidas por suposição |
| **Consistência** | Normalização de datas, tribunal, resultado recursal, valores e indicadores booleanos |
| **Unicidade** | 677 linhas para 677 identificadores `id_llm` no resultado importado |
| **Plausibilidade monetária** | Distinção entre pedido, condenação, honorários e valores acessórios; análise de média, mediana, quartis e faixas |
| **Evidência da IA** | Auditoria literal e indicação de casos para revisão humana |
| **Proveniência** | Registro de origem e URL oficial quando disponível; distinção entre ementa e inteiro teor na amostra de IA |

#### Limites de links oficiais

| Tribunal | Situação nesta versão |
|:---|:---|
| TJSE | Há links de relatórios/decisões específicas cadastrados; acesso depende da validade do endereço |
| TJPA | Há links para páginas de documentos específicos e recuperação de inteiro teor na amostra de IA |
| TJAM | Os endereços cadastrados podem abrir a **consulta geral** do tribunal, exigindo pesquisa pelo usuário; não se deve apresentá-los como links diretos ao acórdão |
| TJPE | **Sem URLs oficiais cadastradas** nesta versão do conjunto analisado |

A métrica de **65,62%** indica a proporção de registros qualificados com alguma URL oficial cadastrada. Ela **não** significa que 65,62% dos julgados tenham link direto ao inteiro teor. Datas de julgamento ausentes aparecem como **“Não informada”** no detalhamento, mesmo quando existe `ano` analítico ou data de publicação.

A qualidade da IA também tem limites: o modelo pode interpretar incorretamente fatos e valores, sua confiança declarada não é uma probabilidade calibrada e a obtenção de inteiro teor foi desigual entre tribunais. A revisão jurídica continua necessária.

# 6. Análises e resultados

### 6.1 Universo qualificado: 1.617 casos

| Métrica | Valor |
|:---|---:|
| Casos | 1.617 |
| Média | R$ 5.686,48 |
| Mediana | R$ 5.000,00 |
| P25 | R$ 4.000,00 |
| P75 | R$ 7.000,00 |
| Cobertura de URL cadastrada | 65,62% |

A mediana de R$ 5.000,00 e o intervalo interquartil de R$ 4.000,00 a R$ 7.000,00 resumem o centro da distribuição, sem dispensar a análise de valores extremos e das diferenças entre tribunais.

**Comparação descritiva no recorte principal:**

| Tribunal | Casos | Média aproximada | Mediana aproximada |
|:---|---:|---:|---:|
| TJAM | 138 | R$ 6,79 mil | R$ 5 mil |
| TJPA | 139 | R$ 8,26 mil | R$ 5 mil |
| TJPE | 556 | R$ 6,38 mil | R$ 5 mil |
| TJSE | 784 | R$ 4,54 mil | R$ 4 mil |

**Classificação heurística dos resultados recursais:**

| Tribunal | MAJORADO | MANTIDO | RECONHECIDO | REDUZIDO |
|:---|---:|---:|---:|---:|
| TJAM | 47 | 27 | 39 | 25 |
| TJPA | 27 | 31 | 37 | 44 |
| TJPE | 87 | 150 | 246 | 73 |
| TJSE | 271 | 84 | 187 | 242 |

Os gráficos permitem explorar a mediana anual, o volume por tribunal, as faixas de indenização e os fundamentos detectados, como *dano in re ipsa*, responsabilidade objetiva, falha na prestação do serviço e razoabilidade/proporcionalidade. Esses resultados são descrições do conjunto selecionado, não uma previsão de decisões futuras.

### 6.2 Amostra de IA: 677 casos

| Indicador | Quantidade | Proporção da amostra |
|:---|---:|---:|
| Inteiro teor recuperado | 337 | 49,78% |
| Somente ementa | 340 | 50,22% |
| Revisão humana sinalizada | 9 | 1,33% |
| Divergência de resultado | 224 | 33,09% |
| Divergência de valor | 71 | 10,49% |

**Divergências de resultado por tribunal na amostra:**

| Tribunal | Casos na amostra | Divergências | Taxa dentro do tribunal amostrado |
|:---|---:|---:|---:|
| TJAM | 138 | 40 | 28,99% |
| TJPA | 139 | 49 | 35,25% |
| TJPE | 200 | 98 | 49,00% |
| TJSE | 200 | 37 | 18,50% |

**Divergências de valor por tribunal:** TJAM 11; TJPA 20; TJPE 22; TJSE 18.

**Médias da amostra de 677: regra × IA** (não confundir com a média do universo de 1.617):

| Tribunal | Média da regra | Média da IA |
|:---|---:|---:|
| TJAM | R$ 6.789,59 | R$ 6.488,72 |
| TJPA | R$ 8.256,01 | R$ 6.471,01 |
| TJPE | R$ 6.047,07 | R$ 5.993,64 |
| TJSE | R$ 4.542,50 | R$ 4.389,45 |

O contraste identifica casos prioritários para auditoria. A presença de divergências mostra que palavras-chave e expressões monetárias, isoladamente, podem não resolver questões como valor pedido versus valor fixado ou `RECONHECIDO` versus `MANTIDO`. **Divergência não equivale a erro confirmado**, pois a IA também exige validação.

# 7. Dashboards: Databricks e Power BI

### 7.1 Dashboard Databricks

O Databricks dispõe de uma visão analítica baseada no universo de **1.617 casos qualificados**, além de seção própria para a **amostra de 677 casos submetida à IA**. A seção de IA apresenta cinco indicadores — amostra total, inteiro teor, revisão humana, divergências de resultado e divergências de valor — e gráficos por tribunal. O ambiente Databricks é separado da publicação pública do Power BI e pode exigir acesso autorizado ao workspace.

### 7.2 Relatório público Microsoft Power BI

**[▶ ACESSAR AS TRÊS PÁGINAS DO DASHBOARD](https://app.powerbi.com/view?r=eyJrIjoiYzIxZDRkMDEtNTQ2Zi00YTZmLWJjNzYtNGQ1NjE5N2FkYWQ2IiwidCI6IjFiYTk1NTkzLTIzZDctNGJhYS1hMWQ2LTNjNWMwMDZlMWQ4NSJ9)**

| Página | Conteúdo |
|:---|:---|
| **Visão Geral** | KPIs do conjunto de 1.617 casos, gráficos de distribuição e comparação entre tribunais, filtros de tribunal, ano, resultado e faixa de valor; seção de validação por IA com indicadores e divergências |
| **Julgados** | Tabela navegável com número do processo, tribunal, data quando preenchida, resultado heurístico, valor extraído, fundamentos, ementa e URL cadastrada; filtros combináveis |
| **Detalhe do Processo** | Seletor individual de processo; cartões de tribunal, data de julgamento, valor formatado em reais e resultado; ementa e link oficial quando existente |

O relatório foi publicado no Power BI on-line por meio de **Publicar na Web**, com código público gerado em 23/09/2026. O endereço do navegador do autor (`/groups/me/reports/...`) não deve ser utilizado no GitHub, pois aponta para a área autenticada; o link acima é o endereço público de visualização (`/view?r=...`). O link deve ser conferido em janela anônima antes da entrega.

**Limitações da navegação:** o link do TJAM pode exigir nova pesquisa no portal oficial; o TJPE está sem URL nesta versão; nem todo registro contém a data exata do julgamento. A métrica de cobertura de URL não mede diretamente acesso ao inteiro teor. A publicação pública é adequada apenas aos dados avaliados para divulgação irrestrita, inclusive dados potencialmente expostos por interação com o relatório.

# 8. Estrutura e reprodutibilidade do repositório

A estrutura abaixo foi conferida no GitHub. Ela mostra **arquivos presentes** no repositório e não presume a existência de notebooks ou documentos ainda não enviados.

```text
jus-expertia-mvp/
├── .gitignore
├── README.md
├── notebooks/
│   ├── README.md
│   └── 01_JusExpertia_Diagnostico_Dados.py
├── scripts/
│   ├── README.md
│   ├── separar_tribunais.py
│   ├── processar_deepseek_677.py
│   ├── diagnosticar_4_faltantes.py
│   └── reprocessar_4_faltantes.py
└── docs/
    ├── README.md
    ├── JusExpertia_MVP_Documento_Final_com_Catalogo_Completo.docx
    └── screenshots/
        ├── README.md
        ├── imagem 1.png
        ├── imagem 2.png
        └── imagem 3.png
```

**Repositório:** https://github.com/genivalfeitoza/jus-expertia-mvp

O material publicado contém código de diagnóstico e processamento local de IA, documentação acadêmica e evidências visuais. As tabelas geradas no workspace e o dashboard Databricks não são automaticamente replicados pelo GitHub. Para uma reexecução integral em outro ambiente, são necessários os dados de origem legitimamente obtidos, a configuração do Databricks e os notebooks/etapas de transformação correspondentes; **não se afirma que a clonagem isolada deste repositório reproduza toda a ingestão**.

Não publicar tokens da Together AI, segredos do Databricks, `.env`, chaves privadas ou dados confidenciais. As configurações de autenticação devem ser fornecidas localmente ou por mecanismo seguro de segredos. O enriquecimento por uma API externa também pressupõe avaliação prévia dos textos que serão enviados ao provedor.

# 9. Evidências e documentação complementar

Há um documento acadêmico complementar em [`docs/JusExpertia_MVP_Documento_Final_com_Catalogo_Completo.docx`](docs/JusExpertia_MVP_Documento_Final_com_Catalogo_Completo.docx) e imagens já existentes em [`docs/screenshots/`](docs/screenshots/). Antes da entrega, recomenda-se conferir suas legendas e se correspondem à versão atual do pipeline e dos dashboards.

Evidências particularmente úteis para avaliação:

- Managed Volume com o arquivo `resultados_deepseek_677.csv`;
- contagem `677 linhas = 677 IDs únicos` e distribuição entre os quatro tribunais;
- Catalog Explorer e esquema das tabelas Gold;
- execução dos testes `GATE_TABELA_FINAL = PASS` e `GATE_ANALYTICS = PASS`;
- comparação regra × IA e os cinco KPIs da amostra;
- Visão Geral, Julgados e Detalhe do Processo no Power BI publicado;
- teste de acesso ao link público sem login.

**Nota de documentação:** esta lista indica evidências desejáveis; ela não afirma que todas essas capturas já estejam presentes na pasta pública. Fotografias ou capturas adicionais devem ser inseridas apenas quando tiverem sido efetivamente geradas.

# 10. Autoavaliação

## 10.1 Atingimento dos objetivos

Ao final deste MVP, considero que consegui atingir o objetivo principal que havia traçado no início do trabalho. Minha proposta era construir um pipeline funcional de dados em nuvem capaz de organizar, tratar e analisar jurisprudência relacionada a danos morais decorrentes de negativação indevida, e esse objetivo foi alcançado.

Consegui estruturar o fluxo completo no Databricks, desde a carga dos dados até a persistência das tabelas em Delta Lake, passando por etapas de transformação, controle de qualidade, criação das camadas analíticas, organização das tabelas no Unity Catalog, análise estatística e publicação de dashboards.

Além disso, ampliei o escopo inicial ao incluir uma etapa de validação semântica por inteligência artificial. Essa etapa me permitiu comparar classificações heurísticas com uma análise contextual mais aprofundada, principalmente quanto ao resultado recursal e ao valor final da indenização.

Entendo, portanto, que o projeto não apenas atendeu ao objetivo acadêmico de construção de um pipeline de dados de ponta a ponta, mas também produziu uma base concreta para uma aplicação jurídica mais ampla no futuro.

## 10.2 Dificuldades encontradas

Durante o desenvolvimento, enfrentei dificuldades que foram importantes para o meu aprendizado.

Uma das principais foi a heterogeneidade dos portais dos tribunais. Cada tribunal disponibiliza suas decisões de maneira diferente, com estruturas próprias de URL, mecanismos de consulta e níveis distintos de acesso ao inteiro teor.

Também encontrei dificuldades relacionadas à própria natureza do texto jurídico. As decisões são documentos não estruturados e frequentemente apresentam várias informações relevantes no mesmo texto. Um único acórdão pode mencionar, por exemplo, o valor pedido pela parte, o valor fixado em primeiro grau, o valor posteriormente reduzido ou majorado, além de valores relacionados a dano material, honorários e custas.

Outra dificuldade importante foi diferenciar corretamente o resultado recursal. Expressões presentes no texto nem sempre significam que o dano moral foi reconhecido naquele julgamento. Em muitos casos, o tribunal apenas mantém uma decisão anterior, o que exige uma interpretação contextual mais cuidadosa.

A recuperação do inteiro teor também foi um desafio. Nem todos os tribunais disponibilizam documentos de forma direta e padronizada, e por isso parte dos casos precisou ser analisada apenas com base na ementa.

Na etapa de inteligência artificial, enfrentei ainda situações de respostas extensas, truncamento e JSON incompleto. Isso exigiu criação de mecanismos de checkpoint, reprocessamento e validação posterior das respostas.

Essas dificuldades acabaram sendo valiosas porque me obrigaram a pensar não apenas na execução técnica, mas também na confiabilidade, rastreabilidade e qualidade do dado produzido.

## 10.3 Decisões que considero positivas

Algumas decisões tomadas durante o projeto foram especialmente importantes para aumentar a confiabilidade do resultado.

A criação de checkpoints no processamento por IA permitiu evitar a perda do trabalho já realizado em caso de falha ou interrupção.

Também optei por utilizar um identificador determinístico, o `id_llm`, o que facilitou a rastreabilidade de cada caso ao longo do processo.

Outra decisão importante foi utilizar uma amostra estratificada em vez de uma seleção aleatória simples. Dessa forma, consegui preservar maior diversidade entre tribunais, resultados recursais e faixas de valor.

Considerei também importante manter os resultados da regra heurística e da IA lado a lado, sem substituir um pelo outro. Isso permitiu medir divergências e identificar situações em que uma classificação puramente textual poderia ser insuficiente.

A auditoria literal das evidências retornadas pela IA foi outra decisão relevante. Sempre que uma evidência não podia ser localizada no texto efetivamente analisado, ela era removida. Isso reduziu o risco de manter justificativas sem suporte textual.

Também incluí um indicador de necessidade de revisão humana, porque considero inadequado tratar uma análise automatizada como infalível, especialmente em um contexto jurídico.

Por fim, considero positiva a separação clara entre o universo principal de 1.617 casos qualificados e a amostra de 677 casos analisada por IA. Essa distinção evita transmitir a impressão incorreta de que todos os casos passaram pela mesma etapa de enriquecimento semântico.

Na parte estatística, optei por utilizar mediana e quartis além da média, porque percebi que a média isolada poderia ser influenciada por valores extremos.

## 10.4 Limitações do trabalho

Reconheço que o projeto possui limitações.

A principal delas é que a análise por IA não foi aplicada aos 1.617 casos do universo qualificado, mas apenas a uma amostra estratificada de 677 casos.

Essa foi uma decisão deliberada. O objetivo deste MVP era demonstrar o funcionamento completo da arquitetura e validar o fluxo de processamento, e não necessariamente realizar a análise semântica de todo o universo disponível.

Também reconheço que a ausência de inteiro teor em parte dos processos limita a profundidade de algumas análises. Em determinados casos, a ementa oferece informação suficiente; em outros, uma análise jurídica mais completa dependeria do acesso ao conteúdo integral da decisão.

Outra limitação é que a confiança declarada pelo modelo de IA não deve ser interpretada como uma probabilidade estatística calibrada de acerto.

Por fim, este trabalho não pretende substituir a análise jurídica profissional. A IA foi utilizada como ferramenta de enriquecimento e apoio à classificação, e não como fonte autônoma de conclusão jurídica.

## 10.5 Trabalhos futuros

Vejo este MVP como uma primeira etapa de um projeto maior.

Como evolução futura, pretendo aplicar a análise semântica ao universo completo dos casos qualificados e ampliar a cobertura para outros tribunais brasileiros.

Também considero importante aperfeiçoar os mecanismos de recuperação do inteiro teor diretamente das fontes oficiais, aumentando a rastreabilidade e a qualidade das análises.

Outro passo relevante seria construir um processo sistemático de validação humana das respostas produzidas pela IA, permitindo medir com maior precisão a qualidade das classificações em um conjunto de referência maior.

Pretendo ainda implementar uma rotina incremental de atualização dos dados, para que novas decisões possam ser incorporadas ao pipeline sem necessidade de reprocessar todo o conjunto histórico.

Também seria interessante desenvolver mecanismos de monitoramento contínuo da qualidade dos dados e de possíveis mudanças de padrão ao longo do tempo.

Por fim, uma evolução natural do projeto seria aprofundar a análise semântica de precedentes, fundamentos jurídicos e critérios de fixação do quantum indenizatório, transformando o JusExpertia em uma ferramenta cada vez mais útil para pesquisa jurisprudencial e apoio à análise jurídica.

# 11. Próximos passos

A evolução proposta preserva a separação entre **trabalho entregue** e **melhorias planejadas**:

1. Completar os links diretos dos julgados do TJAM e cadastrar, quando disponíveis e validados, os endereços oficiais específicos do TJPE. Associar cada URL à **decisão**, não apenas ao número do processo, pois pode haver múltiplos julgamentos.
2. Aplicar a análise contextual a mais casos do universo de 1.617 e ampliar a validação humana com amostra de referência maior.
3. Melhorar a padronização e a documentação da data de julgamento versus data de publicação e ano analítico.
4. Disponibilizar exportações de esquemas reais e metadados completos do catálogo, além de notebooks adicionais de transformação e qualidade quando finalizados.
5. Automatizar atualização incremental, monitoramento de qualidade, auditoria das evidências e testes de integridade dos links.
6. Manter uma versão de demonstração somente com dados adequados à publicação pública e revisar periodicamente as permissões de compartilhamento.

---

**Conclusão:** o JusExpertia demonstra um fluxo acadêmico de engenharia de dados que combina pré-seleção temática, processamento Lakehouse, avaliação de qualidade, estatística descritiva, enriquecimento semântico auditável e exploração interativa. Seu resultado mais importante é a separação explícita entre **origem**, **regra heurística**, **interpretação da IA** e **validação humana**, evitando que um dashboard transmita certeza jurídica superior à evidência disponível.
