# Databricks notebook source
print("JUS-EXPERTIA MVP - TESTE DO DATABRICKS")
print("Python funcionando.")

df_teste = spark.range(1, 6)
display(df_teste)

print("SPARK_OK=1")

# COMMAND ----------

df = spark.table("workspace.default.bronze_jurisprudencia_negativacao")

print("TOTAL_BRONZE =", df.count())

df.groupBy("tribunal").count().orderBy("tribunal").show()

print("COLUNAS =", len(df.columns))
print(df.columns)

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table("workspace.default.bronze_jurisprudencia_negativacao")

silver = (
    bronze
    .withColumn("ementa_norm", F.lower(F.col("ementa")))
    .withColumn(
        "tem_negativacao_positiva",
        F.when(
            F.col("ementa_norm").rlike(
                r"(negativa[cç][aã]o indevida|inscri[cç][aã]o indevida|"
                r"serasa|spc|cadastro de inadimplentes|cadastros de inadimplentes|"
                r"cadastro restritivo|cadastros restritivos|"
                r"restri[cç][aã]o credit[ií]cia|restri[cç][aã]o de cr[eé]dito|"
                r"nome negativado|inscri[cç][aã]o em [oó]rg[aã]os de prote[cç][aã]o ao cr[eé]dito)"
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "indicio_sem_negativacao",
        F.when(
            F.col("ementa_norm").rlike(
                r"(sem negativa[cç][aã]o|sem inscri[cç][aã]o|"
                r"aus[eê]ncia de negativa[cç][aã]o|"
                r"n[aã]o houve negativa[cç][aã]o|"
                r"n[aã]o ocorreu negativa[cç][aã]o|"
                r"desacompanhada de inscri[cç][aã]o)"
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "menciona_dano_moral",
        F.when(
            F.col("ementa_norm").rlike(
                r"(dano moral|danos morais|indeniza[cç][aã]o por dano moral|"
                r"indeniza[cç][aã]o por danos morais)"
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "candidato_silver",
        F.when(
            (F.col("tem_negativacao_positiva") == 1)
            & (F.col("indicio_sem_negativacao") == 0)
            & (F.col("menciona_dano_moral") == 1),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

silver.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.silver_jurisprudencia_negativacao"
)

print("SILVER_CRIADA=1")

print("TOTAL_SILVER =", silver.count())

silver.groupBy("candidato_silver").count().orderBy("candidato_silver").show()

silver.groupBy("tribunal", "candidato_silver").count().orderBy(
    "tribunal", "candidato_silver"
).show()

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("workspace.default.silver_jurisprudencia_negativacao")

silver2 = (
    silver

    .withColumn(
        "resultado_dano_moral",
        F.when(
            F.col("ementa_norm").rlike(
                r"(afastad[oa]s? os danos morais|"
                r"excluir a condena[cç][aã]o.{0,150}dano moral|"
                r"improcedente.{0,150}dano moral|"
                r"n[aã]o enseja dano moral|"
                r"n[aã]o gera dano moral|"
                r"mero aborrecimento|"
                r"mero dissabor)"
            ),
            F.lit("AFASTADO")
        )

        .when(
            F.col("ementa_norm").rlike(
                r"(majorad[oa]|majora[cç][aã]o).{0,150}"
                r"(dano moral|danos morais|indeniza[cç][aã]o)|"
                r"(dano moral|danos morais|indeniza[cç][aã]o).{0,150}"
                r"(majorad[oa]|majora[cç][aã]o)"
            ),
            F.lit("MAJORADO")
        )

        .when(
            F.col("ementa_norm").rlike(
                r"(reduzid[oa]|redu[cç][aã]o|minora[cç][aã]o).{0,150}"
                r"(dano moral|danos morais|indeniza[cç][aã]o)|"
                r"(dano moral|danos morais|indeniza[cç][aã]o).{0,150}"
                r"(reduzid[oa]|redu[cç][aã]o|minora[cç][aã]o)"
            ),
            F.lit("REDUZIDO")
        )

        .when(
            F.col("ementa_norm").rlike(
                r"(mantid[oa]).{0,150}"
                r"(dano moral|danos morais|indeniza[cç][aã]o)|"
                r"(dano moral|danos morais|indeniza[cç][aã]o).{0,150}"
                r"(mantid[oa])"
            ),
            F.lit("MANTIDO")
        )

        .when(
            F.col("ementa_norm").rlike(
                r"(condena[cç][aã]o.{0,150}(dano moral|danos morais)|"
                r"(dano moral|danos morais).{0,150}"
                r"(devid[oa]|configurad[oa]|reconhecid[oa]|indeniz[aá]vel))"
            ),
            F.lit("RECONHECIDO")
        )

        .otherwise(F.lit("NAO_CLASSIFICADO"))
    )

    .withColumn(
        "valor_dano_moral_texto",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:"
            r"danos?\s+morais?|"
            r"indeniza[cç][aã]o(?:\s+por\s+danos?\s+morais?)?|"
            r"quantum\s+indenizat[oó]rio"
            r").{0,150}?"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    .withColumn(
        "valor_dano_moral_limpo",
        F.regexp_replace(
            F.lower(F.col("valor_dano_moral_texto")),
            r"r\$\s*",
            ""
        )
    )

    .withColumn(
        "valor_dano_moral_normalizado",
        F.when(
            F.instr(F.col("valor_dano_moral_limpo"), ",") > 0,
            F.regexp_replace(
                F.regexp_replace(
                    F.col("valor_dano_moral_limpo"),
                    r"\.",
                    ""
                ),
                ",",
                "."
            )
        )
        .otherwise(
            F.regexp_replace(
                F.col("valor_dano_moral_limpo"),
                ",",
                ""
            )
        )
    )

    .withColumn(
        "valor_dano_moral_num",
        F.expr(
            "try_cast(valor_dano_moral_normalizado as double)"
        )
    )
)

silver2.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.silver_jurisprudencia_negativacao_v2"
)

print("SILVER_V2_CRIADA=1")
print("TOTAL_SILVER_V2 =", silver2.count())

print("\nRESULTADO_DANO_MORAL:")
silver2.filter(
    F.col("candidato_silver") == 1
).groupBy(
    "resultado_dano_moral"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

print("\nESTATISTICAS_PRELIMINARES_DE_VALOR:")
silver2.filter(
    (F.col("candidato_silver") == 1)
    & F.col("valor_dano_moral_num").isNotNull()
).groupBy(
    "tribunal"
).agg(
    F.count("*").alias("casos_com_valor"),
    F.round(
        F.avg("valor_dano_moral_num"),
        2
    ).alias("media_preliminar"),
    F.round(
        F.expr(
            "percentile_approx(valor_dano_moral_num, 0.5)"
        ),
        2
    ).alias("mediana_preliminar"),
    F.min(
        "valor_dano_moral_num"
    ).alias("menor_preliminar"),
    F.max(
        "valor_dano_moral_num"
    ).alias("maior_preliminar")
).orderBy(
    "tribunal"
).show(truncate=False)

print("\nAMOSTRA_VALORES_EXTRAIDOS:")
silver2.filter(
    F.col("valor_dano_moral_num").isNotNull()
).select(
    "tribunal",
    "processo",
    "valor_dano_moral_texto",
    "valor_dano_moral_num",
    "resultado_dano_moral"
).show(20, truncate=False)

print("GATE_SILVER_V2=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F

v2 = spark.table("workspace.default.silver_jurisprudencia_negativacao_v2")

# ============================================================
# 1. EXTRAÇÕES CONTEXTUAIS MAIS FORTES
# ============================================================

v3 = (
    v2

    # Ex.: "condenou ... ao pagamento de R$ 5.000,00 por danos morais"
    .withColumn(
        "valor_ctx_condenacao",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)conden(?:ou|ado|ada|ação).{0,180}?"
            r"(?:pagamento\s+de|pagar|no\s+valor\s+de|em)\s*"
            r"(R\$\s*[\d\.,]+)"
            r".{0,80}?(?:dano\s+moral|danos\s+morais)",
            1
        )
    )

    # Ex.: "danos morais fixados/arbitrados em R$ 8.000,00"
    .withColumn(
        "valor_ctx_fixacao",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)"
            r".{0,180}?"
            r"(?:fixad[oa]s?|arbitrad[oa]s?|estabelecid[oa]s?)"
            r".{0,80}?"
            r"(?:em|no\s+valor\s+de)?\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "indenização majorada para R$ 10.000,00"
    .withColumn(
        "valor_ctx_majoracao",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:majorad[oa]s?|majora[cç][aã]o)"
            r".{0,180}?"
            r"(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)?"
            r".{0,80}?"
            r"(?:para|em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "danos morais majorados para R$ 10.000,00"
    .withColumn(
        "valor_ctx_majoracao_2",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)"
            r".{0,180}?"
            r"(?:majorad[oa]s?|majora[cç][aã]o)"
            r".{0,80}?"
            r"(?:para|em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "indenização reduzida para R$ 5.000,00"
    .withColumn(
        "valor_ctx_reducao",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:reduzid[oa]s?|redu[cç][aã]o|minora[cç][aã]o)"
            r".{0,180}?"
            r"(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)?"
            r".{0,80}?"
            r"(?:para|em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "danos morais reduzidos para R$ 5.000,00"
    .withColumn(
        "valor_ctx_reducao_2",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)"
            r".{0,180}?"
            r"(?:reduzid[oa]s?|redu[cç][aã]o|minora[cç][aã]o)"
            r".{0,80}?"
            r"(?:para|em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "indenização mantida em R$ 5.000,00"
    .withColumn(
        "valor_ctx_manutencao",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:mantid[oa]s?|manuten[cç][aã]o)"
            r".{0,180}?"
            r"(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)?"
            r".{0,80}?"
            r"(?:em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )

    # Ex.: "danos morais mantidos em R$ 5.000,00"
    .withColumn(
        "valor_ctx_manutencao_2",
        F.regexp_extract(
            F.col("ementa"),
            r"(?i)(?:dano\s+moral|danos\s+morais|indeniza[cç][aã]o)"
            r".{0,180}?"
            r"(?:mantid[oa]s?|manuten[cç][aã]o)"
            r".{0,80}?"
            r"(?:em|no\s+valor\s+de)\s*"
            r"(R\$\s*[\d\.,]+)",
            1
        )
    )
)

# ============================================================
# 2. ESCOLHA DO MELHOR VALOR CONFORME O RESULTADO JURÍDICO
# ============================================================

v3 = (
    v3
    .withColumn(
        "valor_final_texto",
        F.when(
            F.col("resultado_dano_moral") == "MAJORADO",
            F.coalesce(
                F.nullif(F.col("valor_ctx_majoracao"), F.lit("")),
                F.nullif(F.col("valor_ctx_majoracao_2"), F.lit("")),
                F.nullif(F.col("valor_ctx_fixacao"), F.lit("")),
                F.nullif(F.col("valor_ctx_condenacao"), F.lit(""))
            )
        )
        .when(
            F.col("resultado_dano_moral") == "REDUZIDO",
            F.coalesce(
                F.nullif(F.col("valor_ctx_reducao"), F.lit("")),
                F.nullif(F.col("valor_ctx_reducao_2"), F.lit("")),
                F.nullif(F.col("valor_ctx_fixacao"), F.lit("")),
                F.nullif(F.col("valor_ctx_condenacao"), F.lit(""))
            )
        )
        .when(
            F.col("resultado_dano_moral") == "MANTIDO",
            F.coalesce(
                F.nullif(F.col("valor_ctx_manutencao"), F.lit("")),
                F.nullif(F.col("valor_ctx_manutencao_2"), F.lit("")),
                F.nullif(F.col("valor_ctx_fixacao"), F.lit("")),
                F.nullif(F.col("valor_ctx_condenacao"), F.lit(""))
            )
        )
        .when(
            F.col("resultado_dano_moral") == "RECONHECIDO",
            F.coalesce(
                F.nullif(F.col("valor_ctx_fixacao"), F.lit("")),
                F.nullif(F.col("valor_ctx_condenacao"), F.lit(""))
            )
        )
    )
)

# ============================================================
# 3. NORMALIZAÇÃO DO VALOR
# ============================================================

v3 = (
    v3
    .withColumn(
        "valor_final_limpo",
        F.regexp_replace(
            F.lower(F.col("valor_final_texto")),
            r"r\$\s*",
            ""
        )
    )
    .withColumn(
        "valor_final_normalizado",
        F.when(
            F.instr(F.col("valor_final_limpo"), ",") > 0,
            F.regexp_replace(
                F.regexp_replace(
                    F.col("valor_final_limpo"),
                    r"\.",
                    ""
                ),
                ",",
                "."
            )
        )
        .otherwise(F.col("valor_final_limpo"))
    )
    .withColumn(
        "valor_final_dano_moral",
        F.expr("try_cast(valor_final_normalizado as double)")
    )
)

# ============================================================
# 4. FLAGS DE QUALIDADE
# Não eliminam registros; apenas indicam casos para auditoria.
# ============================================================

v3 = (
    v3
    .withColumn(
        "flag_valor_suspeito",
        F.when(
            F.col("valor_final_dano_moral").isNull(),
            F.lit("SEM_VALOR_EXTRAIDO")
        )
        .when(
            F.col("valor_final_dano_moral") < 500,
            F.lit("VALOR_MUITO_BAIXO_REVISAR")
        )
        .when(
            F.col("valor_final_dano_moral") > 100000,
            F.lit("VALOR_MUITO_ALTO_REVISAR")
        )
        .otherwise(F.lit("OK"))
    )
)

# ============================================================
# 5. PERSISTÊNCIA
# ============================================================

v3.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.silver_jurisprudencia_negativacao_v3"
)

print("SILVER_V3_CRIADA=1")
print("TOTAL_V3 =", v3.count())

# ============================================================
# 6. DIAGNÓSTICOS
# ============================================================

print("\nQUALIDADE_DOS_VALORES:")
v3.filter(
    F.col("candidato_silver") == 1
).groupBy(
    "flag_valor_suspeito"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

print("\nVALORES_CONTEXTUAIS_VALIDOS_POR_TRIBUNAL:")
v3.filter(
    (F.col("candidato_silver") == 1)
    & (F.col("flag_valor_suspeito") == "OK")
    & (F.col("resultado_dano_moral").isin(
        "RECONHECIDO",
        "MANTIDO",
        "REDUZIDO",
        "MAJORADO"
    ))
).groupBy(
    "tribunal"
).agg(
    F.count("*").alias("casos_com_valor_contextual"),
    F.round(
        F.avg("valor_final_dano_moral"),
        2
    ).alias("media"),
    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral, 0.5)"
        ),
        2
    ).alias("mediana"),
    F.min("valor_final_dano_moral").alias("menor"),
    F.max("valor_final_dano_moral").alias("maior")
).orderBy(
    "tribunal"
).show(truncate=False)

print("\nAMOSTRA_VALORES_CONTEXTUAIS:")
v3.filter(
    F.col("valor_final_dano_moral").isNotNull()
).select(
    "tribunal",
    "processo",
    "resultado_dano_moral",
    "valor_final_texto",
    "valor_final_dano_moral",
    "flag_valor_suspeito",
    "url_oficial"
).show(30, truncate=False)

print("\nCASOS_SUSPEITOS_PARA_AUDITORIA:")
v3.filter(
    F.col("flag_valor_suspeito").isin(
        "VALOR_MUITO_BAIXO_REVISAR",
        "VALOR_MUITO_ALTO_REVISAR"
    )
).select(
    "tribunal",
    "processo",
    "resultado_dano_moral",
    "valor_final_texto",
    "valor_final_dano_moral",
    "url_oficial"
).orderBy(
    "valor_final_dano_moral"
).show(30, truncate=False)

print("GATE_SILVER_V3=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import re

v3 = spark.table("workspace.default.silver_jurisprudencia_negativacao_v3")

# ============================================================
# NORMALIZADOR ROBUSTO DE VALORES EM REAIS
# ============================================================

def normalizar_valor_real(valor):
    if valor is None:
        return None

    s = str(valor).strip().lower()

    if not s:
        return None

    # Remove R$, espaços e caracteres estranhos
    s = re.sub(r"r\$\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\d,\.]", "", s)

    if not s:
        return None

    try:
        # ----------------------------------------------------
        # CASO 1: existe vírgula
        # Padrão brasileiro:
        # 5.000,00 -> 5000.00
        # 5000,00  -> 5000.00
        # ----------------------------------------------------
        if "," in s:
            partes = s.rsplit(",", 1)
            inteiro = partes[0].replace(".", "")
            decimal = partes[1]

            if decimal == "":
                decimal = "00"

            if len(decimal) == 1:
                decimal = decimal + "0"

            numero = inteiro + "." + decimal
            return float(numero)

        # ----------------------------------------------------
        # CASO 2: somente ponto
        # Precisamos distinguir:
        #
        # 3.000   -> 3000
        # 10.000  -> 10000
        # 58.031  -> 58031
        #
        # de:
        #
        # 5000.00 -> 5000.00
        # ----------------------------------------------------
        if "." in s:
            partes = s.split(".")

            # Mais de um ponto -> separadores de milhar
            # 1.000.000 -> 1000000
            if len(partes) > 2:
                return float("".join(partes))

            esquerda, direita = partes

            # Exatamente 3 dígitos após o ponto:
            # padrão brasileiro de milhar.
            # 3.000 -> 3000
            # 10.000 -> 10000
            if len(direita) == 3:
                return float(esquerda + direita)

            # 1 ou 2 dígitos após ponto:
            # tratamos como decimal.
            # 5000.00 -> 5000.00
            if len(direita) <= 2:
                return float(s)

            # fallback conservador
            return float(s)

        # ----------------------------------------------------
        # CASO 3: apenas números
        # 5000 -> 5000
        # 200  -> 200
        # ----------------------------------------------------
        return float(s)

    except Exception:
        return None


normalizar_valor_udf = F.udf(
    normalizar_valor_real,
    DoubleType()
)

# ============================================================
# CRIA SILVER V4
# ============================================================

v4 = (
    v3
    .withColumn(
        "valor_final_dano_moral_v4",
        normalizar_valor_udf(
            F.col("valor_final_texto")
        )
    )

    .withColumn(
        "flag_valor_v4",
        F.when(
            F.col("valor_final_dano_moral_v4").isNull(),
            F.lit("SEM_VALOR_EXTRAIDO")
        )
        .when(
            F.col("valor_final_dano_moral_v4") < 500,
            F.lit("VALOR_MUITO_BAIXO_REVISAR")
        )
        .when(
            F.col("valor_final_dano_moral_v4") > 100000,
            F.lit("VALOR_MUITO_ALTO_REVISAR")
        )
        .otherwise(
            F.lit("OK")
        )
    )
)

# ============================================================
# SALVA SILVER V4
# ============================================================

v4.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.silver_jurisprudencia_negativacao_v4"
)

print("SILVER_V4_CRIADA=1")
print("TOTAL_V4 =", v4.count())

# ============================================================
# QUALIDADE
# ============================================================

print("\nQUALIDADE_VALORES_V4:")

v4.filter(
    F.col("candidato_silver") == 1
).groupBy(
    "flag_valor_v4"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

# ============================================================
# ESTATÍSTICAS V4
# ============================================================

print("\nESTATISTICAS_V4:")

v4.filter(
    (F.col("candidato_silver") == 1)
    & (F.col("flag_valor_v4") == "OK")
    & (F.col("resultado_dano_moral").isin(
        "RECONHECIDO",
        "MANTIDO",
        "REDUZIDO",
        "MAJORADO"
    ))
).groupBy(
    "tribunal"
).agg(

    F.count("*").alias(
        "casos_com_valor"
    ),

    F.round(
        F.avg(
            "valor_final_dano_moral_v4"
        ),
        2
    ).alias(
        "media"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v4, 0.25)"
        ),
        2
    ).alias(
        "p25"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v4, 0.50)"
        ),
        2
    ).alias(
        "mediana"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v4, 0.75)"
        ),
        2
    ).alias(
        "p75"
    ),

    F.min(
        "valor_final_dano_moral_v4"
    ).alias(
        "menor"
    ),

    F.max(
        "valor_final_dano_moral_v4"
    ).alias(
        "maior"
    )

).orderBy(
    "tribunal"
).show(truncate=False)

# ============================================================
# VERIFICAÇÃO DOS CASOS QUE ESTAVAM ERRADOS
# ============================================================

print("\nCORRECAO_DOS_VALORES_COM_PONTO:")

v4.filter(
    F.col("valor_final_texto").rlike(
        r"(?i)R\$\s*\d{1,3}\.\d{3}$"
    )
).select(
    "tribunal",
    "processo",
    "valor_final_texto",
    "valor_final_dano_moral",
    "valor_final_dano_moral_v4",
    "flag_valor_v4"
).show(
    50,
    truncate=False
)

# ============================================================
# CASOS AINDA SUSPEITOS
# ============================================================

print("\nSUSPEITOS_V4:")

v4.filter(
    F.col("flag_valor_v4").isin(
        "VALOR_MUITO_BAIXO_REVISAR",
        "VALOR_MUITO_ALTO_REVISAR"
    )
).select(
    "tribunal",
    "processo",
    "resultado_dano_moral",
    "valor_final_texto",
    "valor_final_dano_moral_v4",
    "url_oficial"
).orderBy(
    "valor_final_dano_moral_v4"
).show(
    100,
    truncate=False
)

print("GATE_SILVER_V4=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import re

v4 = spark.table("workspace.default.silver_jurisprudencia_negativacao_v4")

# ============================================================
# SILVER V5
# NORMALIZAÇÃO MONETÁRIA FINAL
# ============================================================

def normalizar_valor_real_v5(valor):
    if valor is None:
        return None

    s = str(valor).strip().lower()

    if not s:
        return None

    s = re.sub(r"r\$\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\d,\.]", "", s)

    if not s:
        return None

    try:

        # ====================================================
        # CASO 1 — EXISTE VÍRGULA
        #
        # Formatos brasileiros:
        # 5.000,00       -> 5000.00
        # 5000,00        -> 5000.00
        # 291.646,89     -> 291646.89
        # 3.901.425,31   -> 3901425.31
        # ====================================================

        if "," in s:

            esquerda, direita = s.rsplit(",", 1)

            esquerda = esquerda.replace(".", "")

            if direita == "":
                direita = "00"

            elif len(direita) == 1:
                direita = direita + "0"

            elif len(direita) > 2:
                direita = direita[:2]

            return float(esquerda + "." + direita)

        # ====================================================
        # CASO 2 — NÃO EXISTE VÍRGULA, MAS EXISTE PONTO
        # ====================================================

        if "." in s:

            partes = s.split(".")

            # ------------------------------------------------
            # Um único ponto
            # ------------------------------------------------

            if len(partes) == 2:

                esquerda, direita = partes

                # 5.000 -> 5000
                # 10.000 -> 10000
                if len(direita) == 3:
                    return float(esquerda + direita)

                # 5000.00 -> 5000.00
                # 38.14 -> 38.14
                if len(direita) in (1, 2):
                    return float(esquerda + "." + direita)

                return float(s)

            # ------------------------------------------------
            # Dois ou mais pontos
            # ------------------------------------------------

            ultima = partes[-1]

            # Formato híbrido:
            #
            # 5.000.00 -> 5000.00
            # 8.000.00 -> 8000.00
            # 10.000.00 -> 10000.00
            #
            # Se o último grupo tem 2 dígitos, interpretamos
            # o último ponto como decimal e os anteriores
            # como separadores de milhar.
            if len(ultima) == 2:

                inteiro = "".join(partes[:-1])

                return float(inteiro + "." + ultima)

            # Formato apenas com milhares:
            #
            # 1.000.000 -> 1000000
            # 3.901.425 -> 3901425
            if all(len(p) == 3 for p in partes[1:]):

                return float("".join(partes))

            # Qualquer formato ambíguo fica sem valor.
            return None

        # ====================================================
        # CASO 3 — APENAS DÍGITOS
        #
        # 5000 -> 5000
        # 200  -> 200
        # ====================================================

        if s.isdigit():
            return float(s)

        return None

    except Exception:
        return None


normalizar_v5_udf = F.udf(
    normalizar_valor_real_v5,
    DoubleType()
)

# ============================================================
# APLICA NORMALIZAÇÃO
# ============================================================

v5 = (
    v4

    .withColumn(
        "valor_final_dano_moral_v5",
        normalizar_v5_udf(
            F.col("valor_final_texto")
        )
    )

    .withColumn(
        "flag_valor_v5",

        F.when(
            F.col("valor_final_dano_moral_v5").isNull(),
            F.lit("SEM_VALOR_EXTRAIDO")
        )

        .when(
            F.col("valor_final_dano_moral_v5") < 500,
            F.lit("VALOR_MUITO_BAIXO_REVISAR")
        )

        .when(
            F.col("valor_final_dano_moral_v5") > 100000,
            F.lit("VALOR_MUITO_ALTO_REVISAR")
        )

        .otherwise(
            F.lit("OK")
        )
    )
)

# ============================================================
# SALVA SILVER V5
# ============================================================

v5.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.silver_jurisprudencia_negativacao_v5"
)

print("SILVER_V5_CRIADA=1")
print("TOTAL_V5 =", v5.count())

# ============================================================
# QUALIDADE
# ============================================================

print("\nQUALIDADE_VALORES_V5:")

v5.filter(
    F.col("candidato_silver") == 1
).groupBy(
    "flag_valor_v5"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

# ============================================================
# ESTATÍSTICAS
# ============================================================

print("\nESTATISTICAS_V5:")

v5.filter(
    (F.col("candidato_silver") == 1)
    & (F.col("flag_valor_v5") == "OK")
    & (
        F.col("resultado_dano_moral").isin(
            "RECONHECIDO",
            "MANTIDO",
            "REDUZIDO",
            "MAJORADO"
        )
    )
).groupBy(
    "tribunal"
).agg(

    F.count("*").alias(
        "casos_com_valor"
    ),

    F.round(
        F.avg("valor_final_dano_moral_v5"),
        2
    ).alias(
        "media"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v5, 0.25)"
        ),
        2
    ).alias(
        "p25"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v5, 0.50)"
        ),
        2
    ).alias(
        "mediana"
    ),

    F.round(
        F.expr(
            "percentile_approx(valor_final_dano_moral_v5, 0.75)"
        ),
        2
    ).alias(
        "p75"
    ),

    F.min(
        "valor_final_dano_moral_v5"
    ).alias(
        "menor"
    ),

    F.max(
        "valor_final_dano_moral_v5"
    ).alias(
        "maior"
    )

).orderBy(
    "tribunal"
).show(truncate=False)

# ============================================================
# CONFERE ESPECIFICAMENTE FORMATOS HÍBRIDOS
# ============================================================

print("\nTESTE_FORMATOS_HIBRIDOS:")

v5.filter(
    F.col("valor_final_texto").rlike(
        r"(?i)R\$\s*\d{1,3}(?:\.\d{3})+\.\d{2}"
    )
).select(
    "tribunal",
    "processo",
    "valor_final_texto",
    "valor_final_dano_moral_v4",
    "valor_final_dano_moral_v5",
    "flag_valor_v5"
).show(
    100,
    truncate=False
)

# ============================================================
# CASOS SUSPEITOS
# ============================================================

print("\nSUSPEITOS_V5:")

v5.filter(
    F.col("flag_valor_v5").isin(
        "VALOR_MUITO_BAIXO_REVISAR",
        "VALOR_MUITO_ALTO_REVISAR"
    )
).select(
    "tribunal",
    "processo",
    "resultado_dano_moral",
    "valor_final_texto",
    "valor_final_dano_moral_v5",
    "url_oficial"
).orderBy(
    "valor_final_dano_moral_v5"
).show(
    100,
    truncate=False
)

# ============================================================
# COMPARAÇÃO V4 x V5
# ============================================================

print("\nALTERACOES_V4_PARA_V5:")

v5.filter(
    (
        F.col("valor_final_dano_moral_v4")
        !=
        F.col("valor_final_dano_moral_v5")
    )
    |
    (
        F.col("valor_final_dano_moral_v4").isNull()
        !=
        F.col("valor_final_dano_moral_v5").isNull()
    )
).select(
    "tribunal",
    "processo",
    "valor_final_texto",
    "valor_final_dano_moral_v4",
    "valor_final_dano_moral_v5",
    "flag_valor_v5"
).show(
    100,
    truncate=False
)

print("GATE_SILVER_V5=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(
    "workspace.default.silver_jurisprudencia_negativacao_v5"
)

# ============================================================
# 1. GOLD BASE
# SOMENTE CASOS DE ALTA CONFIANÇA
# ============================================================

gold_base = (
    silver
    .filter(
        (F.col("candidato_silver") == 1)
        & (F.col("flag_valor_v5") == "OK")
        & F.col("valor_final_dano_moral_v5").isNotNull()
        & F.col("resultado_dano_moral").isin(
            "RECONHECIDO",
            "MANTIDO",
            "REDUZIDO",
            "MAJORADO"
        )
    )

    .withColumn(
        "data_referencia",
        F.coalesce(
            F.to_date("data_julgamento"),
            F.to_date("data_publicacao")
        )
    )

    .withColumn(
        "ano",
        F.year("data_referencia")
    )

    .withColumn(
        "valor_dano_moral",
        F.col("valor_final_dano_moral_v5")
    )

    .withColumn(
        "faixa_valor",
        F.when(
            F.col("valor_dano_moral") < 2000,
            "01_ATÉ_1999"
        )
        .when(
            F.col("valor_dano_moral") < 5000,
            "02_2000_A_4999"
        )
        .when(
            F.col("valor_dano_moral") < 10000,
            "03_5000_A_9999"
        )
        .when(
            F.col("valor_dano_moral") < 20000,
            "04_10000_A_19999"
        )
        .otherwise(
            "05_20000_OU_MAIS"
        )
    )

    .withColumn(
        "tem_url_oficial",
        F.when(
            F.col("url_oficial").isNotNull()
            & (F.trim(F.col("url_oficial")) != ""),
            1
        ).otherwise(0)
    )
)

gold_base.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_judicial_analytics_base"
)

print("GOLD_BASE_CRIADA=1")
print("TOTAL_GOLD_BASE =", gold_base.count())

# ============================================================
# 2. KPIs POR TRIBUNAL
# ============================================================

gold_kpis = (
    gold_base
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.25)"
            ),
            2
        ).alias("p25"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.75)"
            ),
            2
        ).alias("p75"),

        F.min(
            "valor_dano_moral"
        ).alias("menor"),

        F.max(
            "valor_dano_moral"
        ).alias("maior"),

        F.sum(
            "tem_url_oficial"
        ).alias("casos_com_url"),

        F.round(
            100.0 * F.avg("tem_url_oficial"),
            2
        ).alias("pct_com_url")
    )
)

gold_kpis.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_kpis_tribunal"
)

# ============================================================
# 3. TENDÊNCIA ANUAL
# ============================================================

gold_tendencia = (
    gold_base
    .filter(
        F.col("ano").isNotNull()
    )
    .groupBy(
        "tribunal",
        "ano"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.25)"
            ),
            2
        ).alias("p25"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.75)"
            ),
            2
        ).alias("p75")
    )
)

gold_tendencia.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_tendencia_anual"
)

# ============================================================
# 4. RESULTADO RECURSAL
# ============================================================

gold_resultado = (
    gold_base
    .groupBy(
        "tribunal",
        "resultado_dano_moral"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana")
    )
)

total_tribunal = (
    gold_base
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("total_tribunal")
    )
)

gold_resultado = (
    gold_resultado
    .join(
        total_tribunal,
        on="tribunal",
        how="left"
    )
    .withColumn(
        "percentual",
        F.round(
            100.0
            * F.col("casos")
            / F.col("total_tribunal"),
            2
        )
    )
)

gold_resultado.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_resultado_recursal"
)

# ============================================================
# 5. FAIXAS DE VALOR
# ============================================================

gold_faixas = (
    gold_base
    .groupBy(
        "tribunal",
        "faixa_valor"
    )
    .agg(
        F.count("*").alias("casos")
    )
)

total_faixas = (
    gold_base
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("total_tribunal")
    )
)

gold_faixas = (
    gold_faixas
    .join(
        total_faixas,
        on="tribunal",
        how="left"
    )
    .withColumn(
        "percentual",
        F.round(
            100.0
            * F.col("casos")
            / F.col("total_tribunal"),
            2
        )
    )
)

gold_faixas.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_faixas_valor"
)

# ============================================================
# 6. FUNDAMENTOS
# ============================================================

gold_fundamentos_base = (
    gold_base
    .filter(
        F.col("fundamentos_detectados").isNotNull()
        & (
            F.trim(
                F.col("fundamentos_detectados")
            ) != ""
        )
    )
    .withColumn(
        "fundamento",
        F.explode(
            F.split(
                F.col("fundamentos_detectados"),
                r"\s*\|\s*"
            )
        )
    )
    .withColumn(
        "fundamento",
        F.trim(
            F.col("fundamento")
        )
    )
    .filter(
        F.col("fundamento") != ""
    )
)

gold_fundamentos = (
    gold_fundamentos_base
    .groupBy(
        "tribunal",
        "fundamento"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana")
    )
)

gold_fundamentos.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_fundamentos"
)

# ============================================================
# 7. PRECEDENTES REPRESENTATIVOS
#
# Seleciona casos mais próximos da mediana
# de cada tribunal.
# ============================================================

medianas = (
    gold_base
    .groupBy("tribunal")
    .agg(
        F.expr(
            "percentile_approx(valor_dano_moral, 0.50)"
        ).alias("mediana_tribunal")
    )
)

precedentes_base = (
    gold_base
    .join(
        medianas,
        on="tribunal",
        how="left"
    )

    .withColumn(
        "distancia_mediana",
        F.abs(
            F.col("valor_dano_moral")
            - F.col("mediana_tribunal")
        )
    )
)

janela = (
    Window
    .partitionBy("tribunal")
    .orderBy(
        F.col("distancia_mediana").asc(),
        F.col("tem_url_oficial").desc(),
        F.col("data_referencia").desc_nulls_last()
    )
)

gold_precedentes = (
    precedentes_base
    .withColumn(
        "ordem",
        F.row_number().over(janela)
    )
    .filter(
        F.col("ordem") <= 10
    )
    .select(
        "tribunal",
        "processo",
        "numero_acordao",
        "data_referencia",
        "ano",
        "orgao",
        "relator",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "fundamentos_detectados",
        "ementa",
        "url_oficial",
        "tipo_url",
        "fonte"
    )
)

gold_precedentes.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_precedentes_representativos"
)

# ============================================================
# 8. BASE DE AUDITORIA
# ============================================================

auditoria = (
    silver
    .filter(
        F.col("flag_valor_v5").isin(
            "VALOR_MUITO_BAIXO_REVISAR",
            "VALOR_MUITO_ALTO_REVISAR"
        )
    )
    .select(
        "tribunal",
        "processo",
        "numero_acordao",
        "data_julgamento",
        "data_publicacao",
        "orgao",
        "relator",
        "resultado_dano_moral",
        "valor_final_texto",
        "valor_final_dano_moral_v5",
        "flag_valor_v5",
        "ementa",
        "url_oficial",
        "tipo_url",
        "fonte"
    )
)

auditoria.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_auditoria_valores"
)

# ============================================================
# 9. RESULTADOS
# ============================================================

print("\nKPIS_POR_TRIBUNAL:")

gold_kpis.orderBy(
    "tribunal"
).show(
    truncate=False
)

print("\nRESULTADO_RECURSAL:")

gold_resultado.orderBy(
    "tribunal",
    F.desc("casos")
).show(
    100,
    truncate=False
)

print("\nFAIXAS_DE_VALOR:")

gold_faixas.orderBy(
    "tribunal",
    "faixa_valor"
).show(
    100,
    truncate=False
)

print("\nTENDENCIA_ANUAL:")

gold_tendencia.orderBy(
    "tribunal",
    "ano"
).show(
    200,
    truncate=False
)

print("\nFUNDAMENTOS:")

gold_fundamentos.orderBy(
    "tribunal",
    F.desc("casos")
).show(
    100,
    truncate=False
)

print("\nPRECEDENTES_REPRESENTATIVOS:")

gold_precedentes.select(
    "tribunal",
    "processo",
    "ano",
    "resultado_dano_moral",
    "valor_dano_moral",
    "url_oficial"
).orderBy(
    "tribunal",
    "ordem"
).show(
    100,
    truncate=False
)

print("\nAUDITORIA:")

auditoria.groupBy(
    "tribunal",
    "flag_valor_v5"
).count().orderBy(
    "tribunal",
    "flag_valor_v5"
).show(
    truncate=False
)

print("GATE_GOLD=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table("workspace.default.gold_judicial_analytics_base")

print("PERIODO_COMPLETO:")

gold.groupBy("tribunal").agg(
    F.min("ano").alias("primeiro_ano"),
    F.max("ano").alias("ultimo_ano"),
    F.count("*").alias("casos")
).orderBy("tribunal").show()

print("\nPERIODO_COMPARAVEL_2020_2026:")

gold.filter(
    (F.col("ano") >= 2020)
    & (F.col("ano") <= 2026)
).groupBy("tribunal").agg(
    F.min("ano").alias("primeiro_ano"),
    F.max("ano").alias("ultimo_ano"),
    F.count("*").alias("casos"),
    F.round(F.avg("valor_dano_moral"), 2).alias("media"),
    F.round(
        F.expr("percentile_approx(valor_dano_moral, 0.5)"),
        2
    ).alias("mediana")
).orderBy("tribunal").show()

print("\nCASOS_FORA_DA_JANELA_2020_2026:")

gold.filter(
    (F.col("ano") < 2020)
    | (F.col("ano") > 2026)
    | F.col("ano").isNull()
).groupBy("tribunal").count().orderBy("tribunal").show()

# COMMAND ----------

from pyspark.sql import functions as F

gold = spark.table(
    "workspace.default.gold_judicial_analytics_base"
)

# ============================================================
# GOLD COMPARÁVEL OFICIAL DO DASHBOARD
# JANELA COMUM: 2020 A 2025
# ============================================================

gold_comp = (
    gold
    .filter(
        (F.col("ano") >= 2020)
        & (F.col("ano") <= 2025)
    )
)

gold_comp.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_2020_2025"
)

print("GOLD_COMPARAVEL_CRIADA=1")
print("TOTAL_GOLD_COMPARAVEL =", gold_comp.count())

# ============================================================
# 1. KPI PRINCIPAL POR TRIBUNAL
# ============================================================

kpis = (
    gold_comp
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.25)"
            ),
            2
        ).alias("p25"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.75)"
            ),
            2
        ).alias("p75"),

        F.min(
            "valor_dano_moral"
        ).alias("menor"),

        F.max(
            "valor_dano_moral"
        ).alias("maior"),

        F.sum(
            "tem_url_oficial"
        ).alias("casos_com_url"),

        F.round(
            100.0 * F.avg("tem_url_oficial"),
            2
        ).alias("pct_com_url")
    )
)

kpis.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_kpis_2020_2025"
)

# ============================================================
# 2. EVOLUÇÃO ANUAL
# ============================================================

tendencia = (
    gold_comp
    .groupBy(
        "tribunal",
        "ano"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.25)"
            ),
            2
        ).alias("p25"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.75)"
            ),
            2
        ).alias("p75")
    )
)

tendencia.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_tendencia_2020_2025"
)

# ============================================================
# 3. RESULTADO RECURSAL
# ============================================================

resultado = (
    gold_comp
    .groupBy(
        "tribunal",
        "resultado_dano_moral"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana")
    )
)

totais = (
    gold_comp
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("total_tribunal")
    )
)

resultado = (
    resultado
    .join(
        totais,
        on="tribunal",
        how="left"
    )
    .withColumn(
        "percentual",
        F.round(
            100.0
            * F.col("casos")
            / F.col("total_tribunal"),
            2
        )
    )
)

resultado.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_resultado_2020_2025"
)

# ============================================================
# 4. FAIXAS DE VALOR
# ============================================================

faixas = (
    gold_comp
    .groupBy(
        "tribunal",
        "faixa_valor"
    )
    .agg(
        F.count("*").alias("casos")
    )
)

faixas_totais = (
    gold_comp
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("total_tribunal")
    )
)

faixas = (
    faixas
    .join(
        faixas_totais,
        on="tribunal",
        how="left"
    )
    .withColumn(
        "percentual",
        F.round(
            100.0
            * F.col("casos")
            / F.col("total_tribunal"),
            2
        )
    )
)

faixas.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_faixas_2020_2025"
)

# ============================================================
# 5. FUNDAMENTOS NA JANELA COMPARÁVEL
# ============================================================

fundamentos_base = (
    gold_comp
    .filter(
        F.col("fundamentos_detectados").isNotNull()
        & (F.trim(F.col("fundamentos_detectados")) != "")
    )
    .withColumn(
        "fundamento",
        F.explode(
            F.split(
                F.col("fundamentos_detectados"),
                r"\s*\|\s*"
            )
        )
    )
    .withColumn(
        "fundamento",
        F.trim(F.col("fundamento"))
    )
    .filter(
        F.col("fundamento") != ""
    )
)

fundamentos = (
    fundamentos_base
    .groupBy(
        "tribunal",
        "fundamento"
    )
    .agg(
        F.count("*").alias("casos"),

        F.round(
            F.avg("valor_dano_moral"),
            2
        ).alias("media"),

        F.round(
            F.expr(
                "percentile_approx(valor_dano_moral, 0.50)"
            ),
            2
        ).alias("mediana")
    )
)

fundamentos.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_fundamentos_2020_2025"
)

# ============================================================
# 6. DISTRIBUIÇÃO DOS CASOS POR TRIBUNAL E ANO
# ============================================================

volume = (
    gold_comp
    .groupBy(
        "tribunal",
        "ano"
    )
    .agg(
        F.count("*").alias("casos")
    )
)

volume.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_volume_2020_2025"
)

# ============================================================
# RESULTADOS DO GATE
# ============================================================

print("\nKPIS_COMPARAVEIS:")

kpis.orderBy(
    "tribunal"
).show(truncate=False)

print("\nTENDENCIA_COMPARAVEL:")

tendencia.orderBy(
    "tribunal",
    "ano"
).show(100, truncate=False)

print("\nRESULTADO_RECURSAL_COMPARAVEL:")

resultado.orderBy(
    "tribunal",
    F.desc("casos")
).show(100, truncate=False)

print("\nFAIXAS_COMPARAVEIS:")

faixas.orderBy(
    "tribunal",
    "faixa_valor"
).show(100, truncate=False)

print("\nVOLUME_ANUAL:")

volume.orderBy(
    "tribunal",
    "ano"
).show(100, truncate=False)

print("GATE_GOLD_COMPARAVEL=EXECUTADO")

# COMMAND ----------

from pyspark.sql import functions as F

kpis = spark.table(
    "workspace.default.gold_dashboard_kpis_2020_2025"
)

display(
    kpis.select(
        "tribunal",
        "casos",
        "media",
        "mediana",
        "p25",
        "p75"
    ).orderBy("tribunal")
)

# COMMAND ----------

tendencia = spark.table(
    "workspace.default.gold_dashboard_tendencia_2020_2025"
)

display(
    tendencia.select(
        "tribunal",
        "ano",
        "casos",
        "media",
        "mediana",
        "p25",
        "p75"
    ).orderBy(
        "ano",
        "tribunal"
    )
)

# COMMAND ----------

faixas = spark.table(
    "workspace.default.gold_dashboard_faixas_2020_2025"
)

display(
    faixas.select(
        "tribunal",
        "faixa_valor",
        "casos",
        "percentual"
    ).orderBy(
        "faixa_valor",
        "tribunal"
    )
)

# COMMAND ----------

resultado = spark.table(
    "workspace.default.gold_dashboard_resultado_2020_2025"
)

display(
    resultado.select(
        "tribunal",
        "resultado_dano_moral",
        "casos",
        "percentual",
        "media",
        "mediana"
    ).orderBy(
        "resultado_dano_moral",
        "tribunal"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

fundamentos = spark.table(
    "workspace.default.gold_dashboard_fundamentos_2020_2025"
)

janela = (
    Window
    .partitionBy("tribunal")
    .orderBy(
        F.col("casos").desc()
    )
)

fundamentos_top5 = (
    fundamentos
    .withColumn(
        "ranking",
        F.row_number().over(janela)
    )
    .filter(
        F.col("ranking") <= 5
    )
    .select(
        "tribunal",
        "fundamento",
        "casos",
        "media",
        "mediana",
        "ranking"
    )
)

fundamentos_top5.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_fundamentos_top5_2020_2025"
)

display(
    fundamentos_top5.orderBy(
        "fundamento",
        "tribunal"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

fundamentos = spark.table(
    "workspace.default.gold_dashboard_fundamentos_2020_2025"
)

janela = (
    Window
    .partitionBy("tribunal")
    .orderBy(
        F.col("casos").desc()
    )
)

fundamentos_mediana = (
    fundamentos
    .withColumn(
        "ranking",
        F.row_number().over(janela)
    )
    .filter(
        F.col("ranking") <= 5
    )
    .select(
        "tribunal",
        "fundamento",
        "casos",
        "mediana",
        "media"
    )
)

display(
    fundamentos_mediana.orderBy(
        "fundamento",
        "tribunal"
    )
)

# COMMAND ----------

volume = spark.table(
    "workspace.default.gold_dashboard_volume_2020_2025"
)

display(
    volume.select(
        "tribunal",
        "ano",
        "casos"
    ).orderBy(
        "ano",
        "tribunal"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

base = spark.table(
    "workspace.default.gold_dashboard_2020_2025"
)

kpis_gerais = (
    base
    .agg(
        F.count("*").alias("total_casos"),
        F.round(F.avg("valor_dano_moral"), 2).alias("media_geral"),
        F.round(
            F.expr("percentile_approx(valor_dano_moral, 0.25)"),
            2
        ).alias("p25"),
        F.round(
            F.expr("percentile_approx(valor_dano_moral, 0.50)"),
            2
        ).alias("mediana_geral"),
        F.round(
            F.expr("percentile_approx(valor_dano_moral, 0.75)"),
            2
        ).alias("p75"),
        F.min("valor_dano_moral").alias("menor_valor"),
        F.max("valor_dano_moral").alias("maior_valor"),
        F.round(
            100.0 * F.avg("tem_url_oficial"),
            2
        ).alias("pct_com_url")
    )
)

kpis_gerais.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.gold_dashboard_kpis_gerais_2020_2025"
)

display(kpis_gerais)

# COMMAND ----------

display(
    spark.sql("SHOW TABLES IN workspace.default")
         .orderBy("tableName")
)

# COMMAND ----------

df_base = spark.table("workspace.default.gold_judicial_analytics_base")

print("TOTAL_REGISTROS =", df_base.count())
print("TOTAL_COLUNAS =", len(df_base.columns))
print(df_base.columns)

display(df_base.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

base = spark.table("workspace.default.gold_judicial_analytics_base")

print("=== TOTAL ===")
print("REGISTROS =", base.count())

print("\n=== POR TRIBUNAL ===")
display(
    base.groupBy("tribunal")
        .count()
        .orderBy("tribunal")
)

print("\n=== POR ANO ===")
display(
    base.groupBy("ano")
        .count()
        .orderBy("ano")
)

print("\n=== POR RESULTADO DO DANO MORAL ===")
display(
    base.groupBy("resultado_dano_moral")
        .count()
        .orderBy(F.desc("count"))
)

print("\n=== POR FAIXA DE VALOR ===")
display(
    base.groupBy("faixa_valor")
        .count()
        .orderBy("faixa_valor")
)

print("\n=== URL OFICIAL ===")
display(
    base.groupBy("tem_url_oficial")
        .count()
        .orderBy("tem_url_oficial")
)

print("\n=== CRUZAMENTO TRIBUNAL x RESULTADO ===")
display(
    base.groupBy("tribunal", "resultado_dano_moral")
        .count()
        .orderBy("tribunal", "resultado_dano_moral")
)

print("\n=== CRUZAMENTO TRIBUNAL x FAIXA ===")
display(
    base.groupBy("tribunal", "faixa_valor")
        .count()
        .orderBy("tribunal", "faixa_valor")
)

# COMMAND ----------

from pyspark.sql import functions as F

base = spark.table("workspace.default.gold_judicial_analytics_base")

periodo = (
    base
    .filter(F.col("ano").between(2020, 2025))
    .filter(F.col("ementa").isNotNull())
    .filter(F.length(F.trim(F.col("ementa"))) > 0)
)

print("=== REGISTROS 2020-2025 COM EMENTA ===")
display(
    periodo.groupBy("tribunal")
           .count()
           .orderBy("tribunal")
)

print("=== FLAG_VALOR_V5 ===")
display(
    periodo.groupBy("flag_valor_v5")
           .count()
           .orderBy(F.desc("count"))
)

print("=== FLAG_VALOR_V5 POR TRIBUNAL ===")
display(
    periodo.groupBy("tribunal", "flag_valor_v5")
           .count()
           .orderBy("tribunal", F.desc("count"))
)

print("=== URL OFICIAL POR TRIBUNAL ===")
display(
    periodo.groupBy("tribunal", "tem_url_oficial")
           .count()
           .orderBy("tribunal", "tem_url_oficial")
)

print("=== RESULTADO POR TRIBUNAL - 2020 A 2025 ===")
display(
    periodo.groupBy("tribunal", "resultado_dano_moral")
           .count()
           .orderBy("tribunal", "resultado_dano_moral")
)

print("=== FAIXA DE VALOR POR TRIBUNAL - 2020 A 2025 ===")
display(
    periodo.groupBy("tribunal", "faixa_valor")
           .count()
           .orderBy("tribunal", "faixa_valor")
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

base = (
    spark.table("workspace.default.gold_judicial_analytics_base")
    .filter(F.col("ano").between(2020, 2025))
    .filter(F.col("ementa").isNotNull())
    .filter(F.length(F.trim(F.col("ementa"))) > 0)
    .filter(F.col("flag_valor_v5") == "OK")
)

# Meta por tribunal:
# TJAM e TJPA: usar todos os elegíveis
# TJPE e TJSE: limitar a 200 cada
metas = spark.createDataFrame(
    [
        ("TJAM", 138),
        ("TJPA", 139),
        ("TJPE", 200),
        ("TJSE", 200),
    ],
    ["tribunal", "meta_tribunal"]
)

base = base.join(metas, "tribunal", "inner")

# Contagem total por tribunal
totais = (
    base.groupBy("tribunal")
        .agg(F.count("*").alias("total_tribunal"))
)

base = base.join(totais, "tribunal", "left")

# Contagem de cada estrato:
# tribunal + resultado + faixa
estratos = (
    base.groupBy(
        "tribunal",
        "resultado_dano_moral",
        "faixa_valor"
    )
    .agg(F.count("*").alias("total_estrato"))
)

base = base.join(
    estratos,
    ["tribunal", "resultado_dano_moral", "faixa_valor"],
    "left"
)

# Cota proporcional de cada estrato
base = base.withColumn(
    "cota_estrato",
    F.greatest(
        F.lit(1),
        F.round(
            F.col("total_estrato")
            / F.col("total_tribunal")
            * F.col("meta_tribunal")
        ).cast("int")
    )
)

# Ordenação determinística dentro de cada estrato
chave_hash = F.xxhash64(
    F.coalesce(F.col("processo"), F.lit("")),
    F.coalesce(F.col("numero_acordao"), F.lit("")),
    F.col("id_origem").cast("string")
)

w_estrato = Window.partitionBy(
    "tribunal",
    "resultado_dano_moral",
    "faixa_valor"
).orderBy(chave_hash)

amostra_pre = (
    base
    .withColumn("rn_estrato", F.row_number().over(w_estrato))
    .filter(F.col("rn_estrato") <= F.col("cota_estrato"))
)

# Ajuste final para não ultrapassar a meta de cada tribunal
w_final = Window.partitionBy("tribunal").orderBy(
    F.col("resultado_dano_moral"),
    F.col("faixa_valor"),
    chave_hash
)

amostra = (
    amostra_pre
    .withColumn("rn_final", F.row_number().over(w_final))
    .filter(F.col("rn_final") <= F.col("meta_tribunal"))
    .drop(
        "meta_tribunal",
        "total_tribunal",
        "total_estrato",
        "cota_estrato",
        "rn_estrato",
        "rn_final"
    )
)

# Salva como nova tabela Gold específica para a LLM
(
    amostra.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.default.gold_amostra_llm_2020_2025")
)

print("=== AMOSTRA FINAL ===")
display(
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
         .groupBy("tribunal")
         .count()
         .orderBy("tribunal")
)

print("=== RESULTADO POR TRIBUNAL ===")
display(
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
         .groupBy("tribunal", "resultado_dano_moral")
         .count()
         .orderBy("tribunal", "resultado_dano_moral")
)

print("=== FAIXA POR TRIBUNAL ===")
display(
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
         .groupBy("tribunal", "faixa_valor")
         .count()
         .orderBy("tribunal", "faixa_valor")
)

print("TOTAL_AMOSTRA =", spark.table(
    "workspace.default.gold_amostra_llm_2020_2025"
).count())

# COMMAND ----------

from pyspark.sql import functions as F

amostra = spark.table("workspace.default.gold_amostra_llm_2020_2025")

llm_input = (
    amostra
    .select(
        "tribunal",
        "id_origem",
        "processo",
        "numero_acordao",
        "classe",
        "relator",
        "orgao",
        "data_referencia",
        "ano",
        "ementa",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "fundamentos_detectados",
        "url_oficial",
        "tipo_url",
        "tem_url_oficial",
        "content_hash"
    )
    .withColumn(
        "ementa_llm",
        F.trim(F.regexp_replace(F.col("ementa"), r"\s+", " "))
    )
)

(
    llm_input.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.default.gold_amostra_llm_input_2020_2025")
)

print("=== INPUT DA LLM ===")
print("TOTAL =", llm_input.count())
print("COLUNAS =", len(llm_input.columns))

display(
    llm_input.select(
        "tribunal",
        "processo",
        "ano",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "tem_url_oficial",
        "ementa_llm"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

entrada = spark.table("workspace.default.gold_amostra_llm_input_2020_2025")

instrucao = """
Você é um modelo especializado em Direito brasileiro.

Analise EXCLUSIVAMENTE a ementa fornecida.
Não invente fatos, fundamentos, precedentes, súmulas, valores, datas ou conclusões
que não estejam expressamente sustentados pelo texto.

O tema da pesquisa é:
DANO MORAL DECORRENTE DE NEGATIVAÇÃO OU INSCRIÇÃO INDEVIDA EM CADASTROS
DE INADIMPLENTES, COMO SPC, SERASA E EQUIVALENTES.

Retorne APENAS um objeto JSON válido, sem markdown e sem explicações adicionais,
com exatamente esta estrutura:

{
  "tema_confirmado_llm": true,
  "negativacao_ocorreu_llm": "SIM|NAO|INCERTO",
  "dano_moral_reconhecido_llm": "SIM|NAO|INCERTO",
  "resultado_llm": "RECONHECIDO|MANTIDO|MAJORADO|REDUZIDO|AFASTADO|NAO_IDENTIFICADO",
  "valor_final_dano_moral_llm": null,
  "tese_central_llm": "",
  "fundamento_dominante_llm": "",
  "fundamentos_secundarios_llm": [],
  "dano_in_re_ipsa_llm": "SIM|NAO|NAO_MENCIONADO",
  "sumula_385_stj_llm": "SIM|NAO|NAO_MENCIONADA",
  "inscricao_preexistente_llm": "SIM|NAO|NAO_IDENTIFICADO",
  "responsabilidade_objetiva_llm": "SIM|NAO|NAO_MENCIONADA",
  "falha_prestacao_servico_llm": "SIM|NAO|NAO_MENCIONADA",
  "fraude_terceiro_llm": "SIM|NAO|NAO_MENCIONADA",
  "razoabilidade_proporcionalidade_llm": "SIM|NAO|NAO_MENCIONADA",
  "carater_pedagogico_llm": "SIM|NAO|NAO_MENCIONADO",
  "enriquecimento_sem_causa_llm": "SIM|NAO|NAO_MENCIONADO",
  "justificativa_quantum_llm": "",
  "sintese_juridica_llm": "",
  "confianca_llm": 0.0
}

REGRAS IMPORTANTES:

1. "tema_confirmado_llm" somente será true quando a ementa efetivamente tratar
   de negativação, inscrição ou manutenção em cadastro restritivo de crédito.
   Menção meramente hipotética, risco de negativação ou simples citação de precedente
   não é suficiente.

2. "valor_final_dano_moral_llm" deverá conter apenas o valor FINAL da indenização
   por dano moral definido no julgamento analisado.
   Não confundir com:
   - valor da dívida;
   - valor da causa;
   - multa;
   - honorários;
   - danos materiais;
   - valor fixado em instância anterior posteriormente modificado ou afastado.

3. Se o julgamento afastar ou excluir a indenização, não utilize como valor final
   o montante anteriormente fixado.

4. Não inferir Súmula 385/STJ, dano in re ipsa ou qualquer outro fundamento
   se não houver suporte textual na ementa.

5. "confianca_llm" deve variar de 0.0 a 1.0 e refletir apenas a clareza da própria
   ementa para sustentar a classificação.

6. Quando a informação não puder ser determinada pela ementa, utilize os valores
   NAO_IDENTIFICADO, INCERTO, NAO_MENCIONADO ou null, conforme o campo.
"""

dados_contexto = F.concat_ws(
    "\n",
    F.concat(F.lit("TRIBUNAL: "), F.coalesce(F.col("tribunal"), F.lit(""))),
    F.concat(F.lit("PROCESSO: "), F.coalesce(F.col("processo"), F.lit(""))),
    F.concat(F.lit("ACORDAO: "), F.coalesce(F.col("numero_acordao"), F.lit(""))),
    F.concat(F.lit("ANO: "), F.coalesce(F.col("ano").cast("string"), F.lit(""))),
    F.concat(F.lit("RESULTADO EXTRAIDO POR REGRA: "), F.coalesce(F.col("resultado_dano_moral"), F.lit(""))),
    F.concat(F.lit("VALOR EXTRAIDO POR REGRA: "), F.coalesce(F.col("valor_dano_moral").cast("string"), F.lit(""))),
    F.concat(F.lit("FAIXA DE VALOR: "), F.coalesce(F.col("faixa_valor"), F.lit(""))),
    F.concat(F.lit("FUNDAMENTOS PRE-DETECTADOS: "), F.coalesce(F.col("fundamentos_detectados"), F.lit(""))),
    F.concat(F.lit("EMENTA:\n"), F.col("ementa_llm"))
)

prompts = (
    entrada
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "prompt_llm",
        F.concat(
            F.lit(instrucao),
            F.lit("\n\nDADOS DO JULGADO:\n"),
            dados_contexto
        )
    )
)

(
    prompts.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("workspace.default.gold_amostra_llm_prompt_2020_2025")
)

print("=== PROMPTS DA LLM ===")
print("TOTAL =", prompts.count())
print("IDS_UNICOS =", prompts.select("id_llm").distinct().count())

display(
    prompts.select(
        "id_llm",
        "tribunal",
        "processo",
        "ano",
        "resultado_dano_moral",
        "valor_dano_moral",
        "prompt_llm"
    ).limit(3)
)

# COMMAND ----------

import requests
import json
import time

RUNPOD_ENDPOINT = "https://api.runpod.ai/v2/u8blgg3b4om5uj/runsync"

# Cole temporariamente sua API KEY entre as aspas.
# NÃO envie a chave para o ChatGPT.
RUNPOD_API_KEY = "COLE_SUA_API_KEY_RUNPOD_AQUI"

prompts = spark.table(
    "workspace.default.gold_amostra_llm_prompt_2020_2025"
)

registro = (
    prompts
    .select(
        "id_llm",
        "tribunal",
        "processo",
        "prompt_llm"
    )
    .limit(1)
    .collect()[0]
)

payload = {
    "input": {
        "prompt": registro["prompt_llm"]
    }
}

headers = {
    "Authorization": f"Bearer {RUNPOD_API_KEY}",
    "Content-Type": "application/json"
}

print("=== TESTE DE 1 JULGADO ===")
print("ID_LLM =", registro["id_llm"])
print("TRIBUNAL =", registro["tribunal"])
print("PROCESSO =", registro["processo"])

inicio = time.time()

response = requests.post(
    RUNPOD_ENDPOINT,
    headers=headers,
    json=payload,
    timeout=600
)

tempo = time.time() - inicio

print("\nHTTP_STATUS =", response.status_code)
print("TEMPO_SEGUNDOS =", round(tempo, 2))

try:
    resposta = response.json()

    print("\nSTATUS_RUNPOD =", resposta.get("status"))
    print("\n=== OUTPUT ===")

    output = resposta.get("output")

    if isinstance(output, (dict, list)):
        print(
            json.dumps(
                output,
                ensure_ascii=False,
                indent=2
            )
        )
    else:
        print(output)

except Exception:
    print("\nRESPOSTA_NAO_JSON:")
    print(response.text)

# COMMAND ----------

import socket
import requests

print("=== TESTE DNS ===")

try:
    ip = socket.gethostbyname("api.runpod.ai")
    print("DNS_RUNPOD_OK =", ip)
except Exception as e:
    print("DNS_RUNPOD_ERRO =", repr(e))

print("\n=== TESTE INTERNET HTTPS ===")

try:
    r = requests.get("https://www.google.com", timeout=20)
    print("HTTPS_GOOGLE_STATUS =", r.status_code)
except Exception as e:
    print("HTTPS_GOOGLE_ERRO =", repr(e))

print("\n=== TESTE RUNPOD SEM AUTENTICACAO ===")

try:
    r = requests.get(
        "https://api.runpod.ai",
        timeout=20
    )
    print("RUNPOD_HTTP_STATUS =", r.status_code)
    print("RUNPOD_RESPONSE_PREFIX =", r.text[:300])
except Exception as e:
    print("RUNPOD_HTTP_ERRO =", repr(e))

# COMMAND ----------

export_llm = (
    spark.table("workspace.default.gold_amostra_llm_prompt_2020_2025")
    .select(
        "id_llm",
        "tribunal",
        "processo",
        "numero_acordao",
        "ano",
        "prompt_llm"
    )
    .orderBy("tribunal", "ano", "processo")
)

print("TOTAL_EXPORTACAO =", export_llm.count())

display(export_llm)

# COMMAND ----------

from pyspark.sql import functions as F

ID_TESTE = "a39d5398ceaf4535b93bbd5f9b10ce5a393043bafc6d9d3ed8ba9fafa9b5e4a9"
PROCESSO_TESTE = "0603682-98.2015.8.04.0001"

print("=== 1. TABELA DE PROMPTS ===")

prompt = (
    spark.table("workspace.default.gold_amostra_llm_prompt_2020_2025")
    .filter(
        (F.col("id_llm") == ID_TESTE) |
        (F.col("processo") == PROCESSO_TESTE)
    )
)

display(
    prompt.select(
        "id_llm",
        "tribunal",
        "processo",
        "numero_acordao",
        "ano",
        "data_referencia",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "ementa",
        "prompt_llm"
    )
)

print("TOTAL_PROMPT =", prompt.count())


print("\n=== 2. TABELA INPUT LLM ===")

entrada = (
    spark.table("workspace.default.gold_amostra_llm_input_2020_2025")
    .filter(F.col("processo") == PROCESSO_TESTE)
)

display(
    entrada.select(
        "tribunal",
        "processo",
        "ano",
        "data_referencia",
        "resultado_dano_moral",
        "valor_dano_moral",
        "fundamentos_detectados",
        "ementa"
    )
)

print("TOTAL_INPUT =", entrada.count())


print("\n=== 3. AMOSTRA ORIGINAL ===")

amostra = (
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
    .filter(F.col("processo") == PROCESSO_TESTE)
)

display(
    amostra.select(
        "tribunal",
        "processo",
        "ano",
        "data_referencia",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "flag_valor_v5",
        "ementa"
    )
)

print("TOTAL_AMOSTRA =", amostra.count())


print("\n=== 4. BASE GOLD ORIGINAL ===")

base = (
    spark.table("workspace.default.gold_judicial_analytics_base")
    .filter(F.col("processo") == PROCESSO_TESTE)
)

display(
    base.select(
        "tribunal",
        "id_origem",
        "processo",
        "ano",
        "data_referencia",
        "data_julgamento",
        "data_publicacao",
        "resultado_dano_moral",
        "valor_dano_moral",
        "tem_negativacao_positiva",
        "indicio_sem_negativacao",
        "menciona_dano_moral",
        "candidato_silver",
        "fundamentos_detectados",
        "ementa"
    )
)

print("TOTAL_BASE =", base.count())


print("\n=== 5. AUDITORIA DO PERÍODO DA AMOSTRA ===")

auditoria = (
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
    .agg(
        F.count("*").alias("total"),
        F.min("ano").alias("ano_minimo"),
        F.max("ano").alias("ano_maximo"),
        F.sum(
            F.when(
                ~F.col("ano").between(2020, 2025),
                1
            ).otherwise(0)
        ).alias("fora_2020_2025")
    )
)

display(auditoria)

print("\n=== 6. REGISTROS FORA DO PERÍODO, SE EXISTIREM ===")

display(
    spark.table("workspace.default.gold_amostra_llm_2020_2025")
    .filter(~F.col("ano").between(2020, 2025))
    .select(
        "tribunal",
        "processo",
        "ano",
        "data_referencia",
        "resultado_dano_moral",
        "ementa"
    )
    .orderBy("ano")
)

# COMMAND ----------

from pyspark.sql import functions as F

PROCESSO_TESTE = "0603682-98.2015.8.04.0001"

r = (
    spark.table("workspace.default.gold_judicial_analytics_base")
    .filter(F.col("processo") == PROCESSO_TESTE)
    .select(
        "tribunal",
        "processo",
        "data_julgamento",
        "data_publicacao",
        "ano",
        "tem_negativacao_positiva",
        "indicio_sem_negativacao",
        "menciona_dano_moral",
        "candidato_silver",
        "termos_fortes",
        "termos_contexto",
        "termos_dano",
        "fundamentos_detectados",
        "resultado_dano_moral",
        "valor_dano_moral",
        "ementa"
    )
    .collect()[0]
)

print("TRIBUNAL =", r["tribunal"])
print("PROCESSO =", r["processo"])
print("DATA_JULGAMENTO =", r["data_julgamento"])
print("ANO =", r["ano"])
print()
print("tem_negativacao_positiva =", r["tem_negativacao_positiva"])
print("indicio_sem_negativacao =", r["indicio_sem_negativacao"])
print("menciona_dano_moral =", r["menciona_dano_moral"])
print("candidato_silver =", r["candidato_silver"])
print()
print("termos_fortes =", r["termos_fortes"])
print("termos_contexto =", r["termos_contexto"])
print("termos_dano =", r["termos_dano"])
print("fundamentos_detectados =", r["fundamentos_detectados"])
print()
print("resultado_dano_moral =", r["resultado_dano_moral"])
print("valor_dano_moral =", r["valor_dano_moral"])
print()
print("=" * 100)
print("EMENTA INTEGRAL")
print("=" * 100)
print(r["ementa"])

# COMMAND ----------

from pyspark.sql import functions as F

entrada = spark.table(
    "workspace.default.gold_amostra_llm_input_2020_2025"
)

instrucao_v2 = """
Você é um modelo especializado em Direito brasileiro.

Sua tarefa é analisar EXCLUSIVAMENTE o conteúdo jurídico da EMENTA fornecida.

Não utilize conhecimento externo para completar fatos ausentes.
Não invente fatos, fundamentos, precedentes, valores, datas ou conclusões.
Quando a ementa não permitir conclusão segura, use a opção de incerteza apropriada.

IMPORTANTE:
A análise do resultado deve considerar especificamente o capítulo referente
à indenização por dano moral decorrente da negativação indevida.
Não classifique o dano moral pelo simples resultado global do recurso.

DEFINIÇÕES DE RESULTADO:

MANTIDO:
a indenização por dano moral já havia sido fixada anteriormente e o tribunal
mantém o valor ou a condenação.

MAJORADO:
o tribunal aumenta o valor anteriormente fixado a título de dano moral.

REDUZIDO:
o tribunal diminui o valor anteriormente fixado a título de dano moral.

AFASTADO:
havia condenação ou pretensão de dano moral, mas o tribunal exclui ou rejeita
a indenização.

RECONHECIDO:
o julgamento passa a reconhecer o dano moral ou fixa a indenização quando ela
não havia sido reconhecida anteriormente.

NAO_IDENTIFICADO:
a ementa não permite determinar com segurança o efeito do julgamento sobre
o dano moral.

REGRAS ESPECÍFICAS:

1. Expressões como "recurso provido", "parcialmente provido" ou "improvido"
   NÃO determinam sozinhas o resultado do dano moral, pois o recurso pode
   envolver outros capítulos.

2. Se a ementa disser que o valor "fixado pelo juízo a quo" é adequado,
   razoável, proporcional ou deve ser preservado, classifique como MANTIDO.

3. Se houver expressões equivalentes a dano moral presumido em razão da
   negativação indevida, como "dano presumido", "dispensa prova do prejuízo",
   "decorre do próprio fato" ou equivalente, classifique dano_in_re_ipsa_llm
   como SIM, ainda que a expressão latina "in re ipsa" não apareça literalmente.

4. "valor_final_dano_moral_llm" deve representar somente o valor final da
   indenização por dano moral após o julgamento analisado.

5. Não confunda dano moral com valor da dívida, valor da causa, multa,
   honorários, dano material, restituição ou outros valores.

6. Se o tribunal afastar a indenização, utilize null em
   valor_final_dano_moral_llm.

7. "tema_confirmado_llm" somente será true se a ementa efetivamente tratar de
   negativação, inscrição ou manutenção indevida em cadastro restritivo de crédito.

8. Não considere mera citação de precedente sobre negativação como confirmação
   de que a negativação ocorreu no caso concreto.

9. A resposta DEVE começar com { e terminar com }.
   Não escreva nenhuma palavra, frase, comentário ou markdown antes ou depois
   do objeto JSON.

Retorne EXATAMENTE um JSON válido com esta estrutura:

{
  "tema_confirmado_llm": true,
  "negativacao_ocorreu_llm": "SIM|NAO|INCERTO",
  "dano_moral_reconhecido_llm": "SIM|NAO|INCERTO",
  "resultado_llm": "RECONHECIDO|MANTIDO|MAJORADO|REDUZIDO|AFASTADO|NAO_IDENTIFICADO",
  "valor_final_dano_moral_llm": null,
  "tese_central_llm": "",
  "fundamento_dominante_llm": "",
  "fundamentos_secundarios_llm": [],
  "dano_in_re_ipsa_llm": "SIM|NAO|NAO_MENCIONADO",
  "sumula_385_stj_llm": "SIM|NAO|NAO_MENCIONADA",
  "inscricao_preexistente_llm": "SIM|NAO|NAO_IDENTIFICADO",
  "responsabilidade_objetiva_llm": "SIM|NAO|NAO_MENCIONADA",
  "falha_prestacao_servico_llm": "SIM|NAO|NAO_MENCIONADA",
  "fraude_terceiro_llm": "SIM|NAO|NAO_MENCIONADA",
  "razoabilidade_proporcionalidade_llm": "SIM|NAO|NAO_MENCIONADA",
  "carater_pedagogico_llm": "SIM|NAO|NAO_MENCIONADO",
  "enriquecimento_sem_causa_llm": "SIM|NAO|NAO_MENCIONADO",
  "justificativa_quantum_llm": "",
  "sintese_juridica_llm": "",
  "confianca_llm": 0.0
}
"""

contexto_neutro = F.concat_ws(
    "\n",
    F.concat(
        F.lit("TRIBUNAL: "),
        F.coalesce(F.col("tribunal"), F.lit(""))
    ),
    F.concat(
        F.lit("PROCESSO: "),
        F.coalesce(F.col("processo"), F.lit(""))
    ),
    F.concat(
        F.lit("ANO DO JULGAMENTO: "),
        F.coalesce(F.col("ano").cast("string"), F.lit(""))
    ),
    F.concat(
        F.lit("EMENTA:\n"),
        F.col("ementa_llm")
    )
)

prompts_v2 = (
    entrada
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "prompt_llm",
        F.concat(
            F.lit(instrucao_v2),
            F.lit("\n\nDADOS DO JULGADO:\n"),
            contexto_neutro
        )
    )
)

(
    prompts_v2.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_amostra_llm_prompt_v2_2020_2025"
    )
)

print("TOTAL_V2 =", prompts_v2.count())
print(
    "IDS_UNICOS_V2 =",
    prompts_v2.select("id_llm").distinct().count()
)

teste = (
    prompts_v2
    .filter(
        F.col("processo") ==
        "0603682-98.2015.8.04.0001"
    )
)

display(
    teste.select(
        "id_llm",
        "tribunal",
        "processo",
        "ano",
        "prompt_llm"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

PROCESSO_TESTE = "0603682-98.2015.8.04.0001"

teste_v2 = (
    spark.table("workspace.default.gold_amostra_llm_prompt_v2_2020_2025")
    .filter(F.col("processo") == PROCESSO_TESTE)
    .select(
        "id_llm",
        "tribunal",
        "processo",
        "numero_acordao",
        "ano",
        "prompt_llm"
    )
)

print("TOTAL_TESTE_V2 =", teste_v2.count())

display(teste_v2)

# COMMAND ----------

from pyspark.sql import functions as F

entrada = spark.table(
    "workspace.default.gold_amostra_llm_input_2020_2025"
)

instrucao_v3 = """
Você é um modelo especializado em Direito brasileiro.

Analise EXCLUSIVAMENTE a EMENTA fornecida.

REGRA FUNDAMENTAL:
NÃO INFERIR fundamentos jurídicos que não estejam expressamente presentes
ou semanticamente inequívocos no texto.

A ausência de menção NÃO significa negação.

Portanto:

- Use SIM somente quando houver suporte textual na ementa.
- Use NAO somente quando a ementa EXPRESSAMENTE negar aquele elemento.
- Use NAO_MENCIONADO ou NAO_MENCIONADA quando o texto simplesmente não tratar dele.
- Não use conhecimento jurídico externo para completar a decisão.
- Não conclua responsabilidade objetiva apenas porque se trata de relação de consumo.
- Não conclua falha na prestação do serviço apenas porque houve dano moral.
- Não conclua fraude de terceiro sem menção textual.
- Não conclua existência ou inexistência de inscrição preexistente sem menção textual.
- Não conclua aplicação da Súmula 385/STJ se ela não estiver mencionada ou claramente aplicada.

TEMA DA PESQUISA:
dano moral decorrente de negativação, inscrição ou manutenção indevida
em cadastro de inadimplentes ou órgão de proteção ao crédito.

RESULTADO DO DANO MORAL:

MANTIDO:
a indenização já havia sido fixada e o tribunal preserva a condenação
ou o respectivo valor.

MAJORADO:
o tribunal aumenta o valor anteriormente fixado.

REDUZIDO:
o tribunal reduz o valor anteriormente fixado.

AFASTADO:
o tribunal exclui, rejeita ou afasta a indenização.

RECONHECIDO:
o tribunal passa a reconhecer o dano moral ou fixa indenização que antes
não havia sido reconhecida.

NAO_IDENTIFICADO:
a ementa não permite saber com segurança.

IMPORTANTE:
"recurso provido", "parcialmente provido" ou "improvido" não determina,
sozinho, o resultado do capítulo de dano moral.

VALOR:

"valor_final_dano_moral_llm" deve conter somente o valor final do dano moral
após o julgamento analisado.

Não confundir com dívida, restituição, danos materiais, multa, honorários,
valor da causa ou outro montante.

DANO IN RE IPSA:

Se a ementa afirmar que o dano moral é presumido, decorre do próprio fato,
dispensa prova do prejuízo ou expressão equivalente, marque SIM.

EVIDÊNCIA:

Para as classificações principais, copie um pequeno trecho da própria ementa
que sustente a conclusão.

Não invente evidência.

FORMATO:

Retorne apenas UM objeto JSON válido.
Nenhum texto antes ou depois do JSON.

Estrutura obrigatória:

{
  "tema_confirmado_llm": true,
  "evidencia_tema_llm": "",
  "negativacao_ocorreu_llm": "SIM|NAO|INCERTO",
  "dano_moral_reconhecido_llm": "SIM|NAO|INCERTO",
  "resultado_llm": "RECONHECIDO|MANTIDO|MAJORADO|REDUZIDO|AFASTADO|NAO_IDENTIFICADO",
  "evidencia_resultado_llm": "",
  "valor_final_dano_moral_llm": null,
  "evidencia_valor_llm": "",
  "tese_central_llm": "",
  "fundamento_dominante_llm": "",
  "dano_in_re_ipsa_llm": "SIM|NAO|NAO_MENCIONADO",
  "evidencia_in_re_ipsa_llm": "",
  "sumula_385_stj_llm": "SIM|NAO|NAO_MENCIONADA",
  "inscricao_preexistente_llm": "SIM|NAO|NAO_MENCIONADA",
  "responsabilidade_objetiva_llm": "SIM|NAO|NAO_MENCIONADA",
  "falha_prestacao_servico_llm": "SIM|NAO|NAO_MENCIONADA",
  "fraude_terceiro_llm": "SIM|NAO|NAO_MENCIONADA",
  "razoabilidade_proporcionalidade_llm": "SIM|NAO|NAO_MENCIONADA",
  "carater_pedagogico_llm": "SIM|NAO|NAO_MENCIONADO",
  "enriquecimento_sem_causa_llm": "SIM|NAO|NAO_MENCIONADO",
  "justificativa_quantum_llm": "",
  "sintese_juridica_llm": "",
  "confianca_llm": 0.0
}
"""

contexto = F.concat_ws(
    "\n",
    F.concat(
        F.lit("TRIBUNAL: "),
        F.coalesce(F.col("tribunal"), F.lit(""))
    ),
    F.concat(
        F.lit("PROCESSO: "),
        F.coalesce(F.col("processo"), F.lit(""))
    ),
    F.concat(
        F.lit("ANO DO JULGAMENTO: "),
        F.coalesce(F.col("ano").cast("string"), F.lit(""))
    ),
    F.concat(
        F.lit("EMENTA:\n"),
        F.col("ementa_llm")
    )
)

prompts_v3 = (
    entrada
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "prompt_llm",
        F.concat(
            F.lit(instrucao_v3),
            F.lit("\n\nDADOS DO JULGADO:\n"),
            contexto
        )
    )
)

(
    prompts_v3.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_amostra_llm_prompt_v3_2020_2025"
    )
)

print("TOTAL_V3 =", prompts_v3.count())
print(
    "IDS_UNICOS_V3 =",
    prompts_v3.select("id_llm").distinct().count()
)

display(
    prompts_v3
    .filter(
        F.col("processo") ==
        "0603682-98.2015.8.04.0001"
    )
    .select(
        "id_llm",
        "tribunal",
        "processo",
        "ano",
        "prompt_llm"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

PROCESSO_TESTE = "0603682-98.2015.8.04.0001"

teste_v3 = (
    spark.table("workspace.default.gold_amostra_llm_prompt_v3_2020_2025")
    .filter(F.col("processo") == PROCESSO_TESTE)
    .select(
        "id_llm",
        "tribunal",
        "processo",
        "numero_acordao",
        "ano",
        "prompt_llm"
    )
)

print("TOTAL_TESTE_V3 =", teste_v3.count())

display(teste_v3)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

base = spark.table(
    "workspace.default.gold_amostra_llm_input_2020_2025"
)

# Prioriza diversidade:
# tribunal + resultado + faixa de valor
w = (
    Window
    .partitionBy(
        "tribunal",
        "resultado_dano_moral",
        "faixa_valor"
    )
    .orderBy(
        F.xxhash64(
            F.coalesce(F.col("processo"), F.lit("")),
            F.coalesce(F.col("content_hash"), F.lit(""))
        )
    )
)

candidatos = (
    base
    .withColumn(
        "rn_estrato",
        F.row_number().over(w)
    )
    .filter(F.col("rn_estrato") == 1)
)

# Ordenação determinística buscando alternância entre tribunais,
# resultados e faixas
w_final = Window.orderBy(
    F.xxhash64(
        F.col("tribunal"),
        F.col("resultado_dano_moral"),
        F.col("faixa_valor"),
        F.coalesce(F.col("processo"), F.lit(""))
    )
)

validacao10 = (
    candidatos
    .withColumn(
        "rn_final",
        F.row_number().over(w_final)
    )
    .filter(F.col("rn_final") <= 10)
    .drop(
        "rn_estrato",
        "rn_final"
    )
)

(
    validacao10.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_validacao_deepseek_10"
    )
)

print("TOTAL_VALIDACAO =", validacao10.count())

print("\n=== DISTRIBUIÇÃO DOS 10 CASOS ===")

display(
    validacao10.select(
        "tribunal",
        "processo",
        "ano",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor",
        "ementa_llm"
    )
    .orderBy(
        "tribunal",
        "resultado_dano_moral"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

v10 = spark.table("workspace.default.gold_validacao_deepseek_10")

print("=== POR TRIBUNAL ===")
display(
    v10.groupBy("tribunal")
       .count()
       .orderBy("tribunal")
)

print("=== POR RESULTADO ===")
display(
    v10.groupBy("resultado_dano_moral")
       .count()
       .orderBy("resultado_dano_moral")
)

print("=== CASOS SELECIONADOS ===")
display(
    v10.select(
        "tribunal",
        "processo",
        "ano",
        "resultado_dano_moral",
        "valor_dano_moral",
        "faixa_valor"
    ).orderBy(
        "tribunal",
        "resultado_dano_moral"
    )
)

# COMMAND ----------

from pyspark.sql import functions as F

v10 = spark.table(
    "workspace.default.gold_validacao_deepseek_10"
)

export10 = (
    v10
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .select(
        "id_llm",
        "tribunal",
        "id_origem",
        "processo",
        "numero_acordao",
        "ano",
        "ementa_llm",
        F.col("resultado_dano_moral").alias("resultado_regra"),
        F.col("valor_dano_moral").alias("valor_regra"),
        "faixa_valor",
        "content_hash"
    )
    .orderBy(
        "tribunal",
        "processo"
    )
)

(
    export10.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_validacao_deepseek_10_export"
    )
)

print("TOTAL_EXPORT =", export10.count())
print(
    "IDS_UNICOS =",
    export10.select("id_llm").distinct().count()
)

print(
    "EMENTAS_NULAS =",
    export10.filter(
        F.col("ementa_llm").isNull() |
        (F.length(F.trim(F.col("ementa_llm"))) == 0)
    ).count()
)

display(export10)

# COMMAND ----------

from pyspark.sql import functions as F

base = spark.table(
    "workspace.default.gold_amostra_llm_input_2020_2025"
)

export677 = (
    base
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .select(
        "id_llm",
        "tribunal",
        "id_origem",
        "processo",
        "numero_acordao",
        "ano",
        "ementa_llm",
        F.col("resultado_dano_moral").alias("resultado_regra"),
        F.col("valor_dano_moral").alias("valor_regra"),
        "faixa_valor",
        "content_hash"
    )
    .orderBy(
        "tribunal",
        "processo"
    )
)

(
    export677.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_deepseek_677_export"
    )
)

print("TOTAL_EXPORT =", export677.count())

print(
    "IDS_UNICOS =",
    export677.select("id_llm").distinct().count()
)

print(
    "EMENTAS_NULAS =",
    export677.filter(
        F.col("ementa_llm").isNull() |
        (F.length(F.trim(F.col("ementa_llm"))) == 0)
    ).count()
)

display(
    export677.groupBy("tribunal")
             .count()
             .orderBy("tribunal")
)

# COMMAND ----------

from pyspark.sql import functions as F

base = spark.table(
    "workspace.default.gold_amostra_llm_2020_2025"
)

print("=== COLUNAS DISPONÍVEIS NA BASE ===")
print(base.columns)

export677_v2 = (
    base
    .filter(F.col("ano").between(2020, 2025))
    .filter(F.col("ementa").isNotNull())
    .filter(F.length(F.trim(F.col("ementa"))) > 0)
    .withColumn(
        "id_llm",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("tribunal"),
                F.col("id_origem").cast("string"),
                F.coalesce(F.col("content_hash"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "ementa_llm",
        F.trim(
            F.regexp_replace(
                F.col("ementa"),
                r"\s+",
                " "
            )
        )
    )
    .select(
        "id_llm",
        "tribunal",
        "id_origem",
        "processo",
        "numero_acordao",
        "classe",
        "relator",
        "orgao",
        "data_referencia",
        "ano",
        "ementa_llm",
        "url_oficial",
        "tipo_url",
        "tem_url_oficial",
        "fonte",
        "provider",
        F.col("resultado_dano_moral").alias("resultado_regra"),
        F.col("valor_dano_moral").alias("valor_regra"),
        "faixa_valor",
        "content_hash"
    )
    .orderBy(
        "tribunal",
        "processo"
    )
)

(
    export677_v2.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(
        "workspace.default.gold_deepseek_677_export_v2"
    )
)

print()
print("============================================================")
print("JUS-EXPERTIA - EXPORTAÇÃO DEEPSEEK V2 COM URLs")
print("============================================================")

print("TOTAL_EXPORT =", export677_v2.count())

print(
    "IDS_UNICOS =",
    export677_v2.select("id_llm").distinct().count()
)

print(
    "EMENTAS_NULAS =",
    export677_v2.filter(
        F.col("ementa_llm").isNull()
        |
        (F.length(F.trim(F.col("ementa_llm"))) == 0)
    ).count()
)

print(
    "COM_URL_OFICIAL =",
    export677_v2.filter(
        F.col("tem_url_oficial") == 1
    ).count()
)

print(
    "SEM_URL_OFICIAL =",
    export677_v2.filter(
        F.col("tem_url_oficial") == 0
    ).count()
)

print()
print("=== POR TRIBUNAL ===")

display(
    export677_v2
    .groupBy("tribunal")
    .agg(
        F.count("*").alias("total"),
        F.sum(
            F.when(
                F.col("tem_url_oficial") == 1,
                1
            ).otherwise(0)
        ).alias("com_url"),
        F.sum(
            F.when(
                F.col("tem_url_oficial") == 0,
                1
            ).otherwise(0)
        ).alias("sem_url")
    )
    .orderBy("tribunal")
)

print()
print("=== TIPOS DE URL ===")

display(
    export677_v2
    .groupBy(
        "tribunal",
        "tipo_url"
    )
    .count()
    .orderBy(
        "tribunal",
        F.desc("count")
    )
)

print()
print("=== EXEMPLOS DE URLs ===")

display(
    export677_v2
    .filter(
        F.col("tem_url_oficial") == 1
    )
    .select(
        "tribunal",
        "processo",
        "tipo_url",
        "url_oficial"
    )
    .orderBy(
        "tribunal",
        "processo"
    )
    .limit(20)
)

# COMMAND ----------

display(
    spark.table(
        "workspace.default.gold_deepseek_677_export_v2"
    ).orderBy(
        "tribunal",
        "processo"
    )
)

# COMMAND ----------

ARQUIVO_CSV = "/Volumes/workspace/default/jus_expertia_mvp/resultados_deepseek_677.csv"

df_teste = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-8")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", "false")
    .csv(ARQUIVO_CSV)
)

print("TOTAL_LINHAS =", df_teste.count())
print("TOTAL_COLUNAS =", len(df_teste.columns))
print("IDS_UNICOS =", df_teste.select("id_llm").distinct().count())

df_teste.groupBy("tribunal").count().orderBy("tribunal").show()

display(df_teste.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

ARQUIVO_CSV = "/Volumes/workspace/default/jus_expertia_mvp/resultados_deepseek_677.csv"

TABELA_FINAL = "workspace.default.gold_deepseek_677_final"

df_deepseek = (
    spark.read
    .option("header", "true")
    .option("encoding", "UTF-8")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", "false")
    .csv(ARQUIVO_CSV)
)

print("TOTAL_LIDO =", df_deepseek.count())
print("COLUNAS =", len(df_deepseek.columns))
print("IDS_UNICOS =", df_deepseek.select("id_llm").distinct().count())

(
    df_deepseek.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_FINAL)
)

total_tabela = spark.table(TABELA_FINAL).count()

print("TABELA_CRIADA =", TABELA_FINAL)
print("TOTAL_TABELA =", total_tabela)

if total_tabela == 677:
    print("GATE_TABELA_FINAL = PASS")
else:
    print("GATE_TABELA_FINAL = FAIL")

# COMMAND ----------

from pyspark.sql import functions as F

TABELA_FINAL = "workspace.default.gold_deepseek_677_final"
TABELA_ANALYTICS = "workspace.default.gold_deepseek_677_analytics"

df = spark.table(TABELA_FINAL)

df_analytics = (
    df

    .withColumn(
        "ano",
        F.col("ano").cast("int")
    )

    .withColumn(
        "valor_regra",
        F.col("valor_regra").cast("double")
    )

    .withColumn(
        "valor_final_dano_moral_llm",
        F.col("valor_final_dano_moral_llm").cast("double")
    )

    .withColumn(
        "valor_anterior_dano_moral_llm",
        F.col("valor_anterior_dano_moral_llm").cast("double")
    )

    .withColumn(
        "confianca_llm",
        F.col("confianca_llm").cast("double")
    )

    .withColumn(
        "qtd_evidencias_removidas",
        F.col("qtd_evidencias_removidas").cast("int")
    )

    .withColumn(
        "qtd_fontes_corrigidas",
        F.col("qtd_fontes_corrigidas").cast("int")
    )

    .withColumn(
        "prompt_tokens",
        F.col("prompt_tokens").cast("long")
    )

    .withColumn(
        "completion_tokens",
        F.col("completion_tokens").cast("long")
    )

    .withColumn(
        "total_tokens",
        F.col("total_tokens").cast("long")
    )

    .withColumn(
        "inteiro_teor_obtido_bool",
        F.lower(F.col("inteiro_teor_obtido")).isin("true", "1", "sim")
    )

    .withColumn(
        "revisao_humana_bool",
        F.lower(F.col("necessita_revisao_humana_llm")).isin("true", "1", "sim")
    )

    .withColumn(
        "divergencia_resultado_bool",
        F.lower(F.col("divergencia_resultado_regra_llm")).isin("true", "1", "sim")
    )

    .withColumn(
        "divergencia_valor_bool",
        F.lower(F.col("divergencia_valor_regra_llm")).isin("true", "1", "sim")
    )

    .withColumn(
        "faixa_valor_llm",
        F.when(
            F.col("valor_final_dano_moral_llm").isNull(),
            "00_NAO_IDENTIFICADO"
        )
        .when(
            F.col("valor_final_dano_moral_llm") < 2000,
            "01_ATE_1999"
        )
        .when(
            F.col("valor_final_dano_moral_llm") < 5000,
            "02_2000_A_4999"
        )
        .when(
            F.col("valor_final_dano_moral_llm") < 10000,
            "03_5000_A_9999"
        )
        .when(
            F.col("valor_final_dano_moral_llm") < 20000,
            "04_10000_A_19999"
        )
        .otherwise(
            "05_20000_OU_MAIS"
        )
    )

    .withColumn(
        "tipo_fonte_analise",
        F.when(
            F.col("fonte_analise_llm") == "EMENTA_E_INTEIRO_TEOR",
            "EMENTA + INTEIRO TEOR"
        )
        .otherwise(
            "SOMENTE EMENTA"
        )
    )
)

(
    df_analytics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_ANALYTICS)
)

total = spark.table(TABELA_ANALYTICS).count()

ids_unicos = (
    spark.table(TABELA_ANALYTICS)
    .select("id_llm")
    .distinct()
    .count()
)

print("TABELA_ANALYTICS =", TABELA_ANALYTICS)
print("TOTAL_ANALYTICS =", total)
print("IDS_UNICOS_ANALYTICS =", ids_unicos)

spark.table(TABELA_ANALYTICS).groupBy("tribunal").count().orderBy("tribunal").show()

if total == 677 and ids_unicos == 677:
    print("GATE_ANALYTICS = PASS")
else:
    print("GATE_ANALYTICS = FAIL")

# COMMAND ----------

from pyspark.sql import functions as F

TABELA = "workspace.default.gold_deepseek_677_analytics"

df = spark.table(TABELA)

total = df.count()

div_resultado = (
    df.filter(
        F.col("divergencia_resultado_bool") == True
    ).count()
)

div_valor = (
    df.filter(
        F.col("divergencia_valor_bool") == True
    ).count()
)

revisao = (
    df.filter(
        F.col("revisao_humana_bool") == True
    ).count()
)

com_inteiro = (
    df.filter(
        F.col("inteiro_teor_obtido_bool") == True
    ).count()
)

somente_ementa = total - com_inteiro

print("=" * 90)
print("JUS-EXPERTIA - COMPARAÇÃO DEEPSEEK x REGRA HEURÍSTICA")
print("=" * 90)

print("TOTAL =", total)
print("COM_INTEIRO_TEOR =", com_inteiro)
print("SOMENTE_EMENTA =", somente_ementa)
print("REVISAO_HUMANA =", revisao)

print(
    "DIVERGENCIAS_RESULTADO =",
    div_resultado,
    "|",
    round(div_resultado / total * 100, 2),
    "%"
)

print(
    "DIVERGENCIAS_VALOR =",
    div_valor,
    "|",
    round(div_valor / total * 100, 2),
    "%"
)

print()
print("=== DIVERGÊNCIAS POR TRIBUNAL ===")

display(
    df.groupBy("tribunal")
    .agg(
        F.count("*").alias("total"),

        F.sum(
            F.when(
                F.col("divergencia_resultado_bool") == True,
                1
            ).otherwise(0)
        ).alias("divergencias_resultado"),

        F.sum(
            F.when(
                F.col("divergencia_valor_bool") == True,
                1
            ).otherwise(0)
        ).alias("divergencias_valor"),

        F.sum(
            F.when(
                F.col("revisao_humana_bool") == True,
                1
            ).otherwise(0)
        ).alias("revisao_humana"),

        F.round(
            F.avg("valor_regra"),
            2
        ).alias("media_valor_regra"),

        F.round(
            F.avg("valor_final_dano_moral_llm"),
            2
        ).alias("media_valor_deepseek")
    )
    .orderBy("tribunal")
)

print()
print("=== RESULTADO RECURSAL: REGRA x DEEPSEEK ===")

display(
    df.groupBy(
        "tribunal",
        "resultado_regra",
        "resultado_llm"
    )
    .count()
    .orderBy(
        "tribunal",
        F.desc("count")
    )
)

print()
print("=== 20 MAIORES DIVERGÊNCIAS DE VALOR ===")

display(
    df
    .filter(
        F.col("divergencia_valor_bool") == True
    )
    .withColumn(
        "diferenca_valor",
        F.round(
            F.abs(
                F.col("valor_final_dano_moral_llm")
                - F.col("valor_regra")
            ),
            2
        )
    )
    .select(
        "tribunal",
        "processo",
        "resultado_regra",
        "resultado_llm",
        "valor_regra",
        "valor_final_dano_moral_llm",
        "diferenca_valor",
        "confianca_llm",
        "tipo_fonte_analise"
    )
    .orderBy(
        F.desc("diferenca_valor")
    )
    .limit(20)
)

print()
print("=== CASOS MARCADOS PARA REVISÃO HUMANA ===")

display(
    df
    .filter(
        F.col("revisao_humana_bool") == True
    )
    .select(
        "tribunal",
        "processo",
        "resultado_regra",
        "resultado_llm",
        "valor_regra",
        "valor_final_dano_moral_llm",
        "confianca_llm",
        "tipo_fonte_analise",
        "motivo_revisao_humana_llm"
    )
    .orderBy(
        "tribunal",
        "processo"
    )
)

print()
print("GATE_COMPARACAO_DEEPSEEK = PASS")

# COMMAND ----------

from pyspark.sql import functions as F

TABELA = "workspace.default.gold_deepseek_677_analytics"
TABELA_KPIS = "workspace.default.gold_deepseek_677_kpis"

df = spark.table(TABELA)

df_kpis = (
    df.agg(
        F.count("*").alias("total_amostra"),

        F.sum(
            F.when(
                F.col("inteiro_teor_obtido_bool") == True,
                1
            ).otherwise(0)
        ).alias("com_inteiro_teor"),

        F.sum(
            F.when(
                F.col("revisao_humana_bool") == True,
                1
            ).otherwise(0)
        ).alias("revisao_humana"),

        F.sum(
            F.when(
                F.col("divergencia_resultado_bool") == True,
                1
            ).otherwise(0)
        ).alias("divergencias_resultado"),

        F.sum(
            F.when(
                F.col("divergencia_valor_bool") == True,
                1
            ).otherwise(0)
        ).alias("divergencias_valor"),

        F.round(
            F.avg("valor_final_dano_moral_llm"),
            2
        ).alias("valor_medio_deepseek"),

        F.expr(
            "percentile_approx(valor_final_dano_moral_llm, 0.50)"
        ).alias("valor_mediano_deepseek"),

        F.round(
            F.avg("confianca_llm"),
            4
        ).alias("confianca_media")
    )
    .withColumn(
        "pct_inteiro_teor",
        F.round(
            F.col("com_inteiro_teor")
            / F.col("total_amostra")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_revisao_humana",
        F.round(
            F.col("revisao_humana")
            / F.col("total_amostra")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_divergencia_resultado",
        F.round(
            F.col("divergencias_resultado")
            / F.col("total_amostra")
            * 100,
            2
        )
    )
    .withColumn(
        "pct_divergencia_valor",
        F.round(
            F.col("divergencias_valor")
            / F.col("total_amostra")
            * 100,
            2
        )
    )
)

(
    df_kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_KPIS)
)

display(
    spark.table(TABELA_KPIS)
)

print("TOTAL_KPIS =", spark.table(TABELA_KPIS).count())

if spark.table(TABELA_KPIS).count() == 1:
    print("GATE_KPIS_DEEPSEEK = PASS")
else:
    print("GATE_KPIS_DEEPSEEK = FAIL")

# COMMAND ----------

tabelas = [
    "workspace.default.bronze_jurisprudencia_negativacao",
    "workspace.default.gold_judicial_analytics_base",
    "workspace.default.gold_amostra_llm_2020_2025",
    "workspace.default.gold_deepseek_677_final",
    "workspace.default.gold_deepseek_677_analytics",
    "workspace.default.gold_deepseek_677_kpis"
]

for tabela in tabelas:
    print()
    print("=" * 120)
    print("TABELA:", tabela)
    print("=" * 120)

    df = spark.table(tabela)

    print("TOTAL_LINHAS =", df.count())
    print("TOTAL_COLUNAS =", len(df.columns))

    print()
    print("SCHEMA:")
    df.printSchema()

    print()
    print("DESCRIBE EXTENDED:")
    spark.sql(f"DESCRIBE EXTENDED {tabela}").show(
        500,
        truncate=False
    )

# COMMAND ----------

from pyspark.sql import functions as F

tabelas = [
    "bronze_jurisprudencia_negativacao",
    "gold_judicial_analytics_base",
    "gold_amostra_llm_2020_2025",
    "gold_deepseek_677_final",
    "gold_deepseek_677_analytics",
    "gold_deepseek_677_kpis"
]

lista_tabelas_sql = ",".join(
    [f"'{t}'" for t in tabelas]
)

catalogo = spark.sql(f"""
SELECT
    table_catalog,
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    full_data_type,
    is_nullable,
    comment
FROM system.information_schema.columns
WHERE table_catalog = 'workspace'
  AND table_schema = 'default'
  AND table_name IN ({lista_tabelas_sql})
ORDER BY table_name, ordinal_position
""")

print("TOTAL_COLUNAS_CATALOGO =", catalogo.count())

display(catalogo)

saida = "/Volumes/workspace/default/jus_expertia_mvp/catalogo_colunas_jusexpertia"

(
    catalogo
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(saida)
)

print("CATALOGO_EXPORTADO =", saida)

for tabela in tabelas:
    df = spark.table(f"workspace.default.{tabela}")

    print(
        tabela,
        "| linhas =",
        df.count(),
        "| colunas =",
        len(df.columns)
    )

print("GATE_CATALOGO = PASS")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    DecimalType,
    BooleanType,
    DateType,
    TimestampType
)

tabelas = [
    "bronze_jurisprudencia_negativacao",
    "gold_judicial_analytics_base",
    "gold_amostra_llm_2020_2025",
    "gold_deepseek_677_final",
    "gold_deepseek_677_analytics",
    "gold_deepseek_677_kpis"
]

linhas_saida = []

for nome_tabela in tabelas:
    tabela_completa = f"workspace.default.{nome_tabela}"
    df = spark.table(tabela_completa)

    total_linhas = df.count()

    print()
    print("=" * 100)
    print("PROCESSANDO:", nome_tabela)
    print("LINHAS:", total_linhas)
    print("=" * 100)

    for campo in df.schema.fields:
        nome_coluna = campo.name
        tipo = campo.dataType
        tipo_str = tipo.simpleString()

        base = df.select(F.col(nome_coluna))

        nulos = base.filter(
            F.col(nome_coluna).isNull()
        ).count()

        nao_nulos = total_linhas - nulos

        try:
            distintos_aprox = (
                base
                .agg(
                    F.approx_count_distinct(
                        F.col(nome_coluna)
                    ).alias("n")
                )
                .collect()[0]["n"]
            )
        except Exception:
            distintos_aprox = None

        minimo = None
        maximo = None

        if isinstance(
            tipo,
            (
                IntegerType,
                LongType,
                ShortType,
                ByteType,
                FloatType,
                DoubleType,
                DecimalType,
                DateType,
                TimestampType
            )
        ):
            try:
                mm = (
                    base
                    .agg(
                        F.min(nome_coluna).alias("minimo"),
                        F.max(nome_coluna).alias("maximo")
                    )
                    .collect()[0]
                )

                minimo = (
                    None
                    if mm["minimo"] is None
                    else str(mm["minimo"])
                )

                maximo = (
                    None
                    if mm["maximo"] is None
                    else str(mm["maximo"])
                )

            except Exception:
                pass

        exemplos = []

        if isinstance(
            tipo,
            BooleanType
        ):
            try:
                exemplos = [
                    str(r[nome_coluna])
                    for r in (
                        base
                        .filter(
                            F.col(nome_coluna).isNotNull()
                        )
                        .distinct()
                        .limit(10)
                        .collect()
                    )
                ]
            except Exception:
                exemplos = []

        elif isinstance(
            tipo,
            StringType
        ):
            try:
                amostra = (
                    base
                    .filter(
                        F.col(nome_coluna).isNotNull()
                    )
                    .select(
                        F.substring(
                            F.col(nome_coluna),
                            1,
                            120
                        ).alias(nome_coluna)
                    )
                    .distinct()
                    .limit(5)
                    .collect()
                )

                exemplos = [
                    str(r[nome_coluna])
                    for r in amostra
                ]

            except Exception:
                exemplos = []

        linhas_saida.append(
            (
                nome_tabela,
                nome_coluna,
                tipo_str,
                total_linhas,
                nulos,
                nao_nulos,
                distintos_aprox,
                minimo,
                maximo,
                " | ".join(exemplos)
            )
        )

schema_saida = """
table_name string,
column_name string,
data_type string,
total_rows long,
null_count long,
non_null_count long,
approx_distinct long,
min_value string,
max_value string,
sample_values string
"""

catalogo_dominios = spark.createDataFrame(
    linhas_saida,
    schema_saida
)

display(
    catalogo_dominios
    .orderBy(
        "table_name",
        "column_name"
    )
)

saida = (
    "/Volumes/workspace/default/jus_expertia_mvp/"
    "catalogo_dominios_jusexpertia"
)

(
    catalogo_dominios
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(saida)
)

print()
print("TOTAL_REGISTROS =", catalogo_dominios.count())
print("CATALOGO_DOMINIOS_EXPORTADO =", saida)
print("GATE_DOMINIOS = PASS")

# COMMAND ----------

tabelas = [
    "workspace.default.bronze_jurisprudencia_negativacao",
    "workspace.default.gold_judicial_analytics_base",
    "workspace.default.gold_amostra_llm_2020_2025",
    "workspace.default.gold_deepseek_677_final",
    "workspace.default.gold_deepseek_677_analytics",
    "workspace.default.gold_deepseek_677_kpis"
]

for tabela in tabelas:
    print()
    print("=" * 100)
    print(tabela)
    print("=" * 100)

    spark.sql(f"""
        DESCRIBE HISTORY {tabela}
    """).select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "userName"
    ).show(
        20,
        truncate=False
    )

# COMMAND ----------

from pyspark.sql import functions as F

tabelas = [
    "workspace.default.bronze_jurisprudencia_negativacao",
    "workspace.default.gold_judicial_analytics_base",
    "workspace.default.gold_amostra_llm_2020_2025",
    "workspace.default.gold_deepseek_677_final",
    "workspace.default.gold_deepseek_677_analytics",
    "workspace.default.gold_deepseek_677_kpis"
]

historicos = []

for tabela in tabelas:
    hist = (
        spark.sql(f"DESCRIBE HISTORY {tabela}")
        .select(
            "version",
            "timestamp",
            "operation",
            "operationParameters",
            "userName"
        )
        .withColumn(
            "table_name",
            F.lit(tabela)
        )
    )

    historicos.append(hist)

historico_final = historicos[0]

for hist in historicos[1:]:
    historico_final = historico_final.unionByName(
        hist,
        allowMissingColumns=True
    )

historico_final = (
    historico_final
    .select(
        "table_name",
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "userName"
    )
    .orderBy(
        "table_name",
        F.desc("version")
    )
)

display(historico_final)

saida = (
    "/Volumes/workspace/default/jus_expertia_mvp/"
    "catalogo_historico_tabelas"
)

(
    historico_final
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(saida)
)

print()
print(
    "TOTAL_REGISTROS_HISTORICO =",
    historico_final.count()
)

print(
    "HISTORICO_EXPORTADO =",
    saida
)

print(
    "GATE_HISTORICO = PASS"
)