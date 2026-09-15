import csv
import getpass
import html
import json
import os
import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup


# ============================================================
# JUS-EXPERTIA
# PROCESSAMENTO DEEPSEEK - AMOSTRA 677
#
# VERSÃO FINAL COM AUDITORIA DETERMINÍSTICA DE EVIDÊNCIAS
#
# TJPA -> ementa + inteiro teor via API pública
# TJSE -> ementa + inteiro teor via URL oficial
# TJAM -> ementa
# TJPE -> ementa
#
# Recursos:
# - Structured Output
# - fundamentos jurídicos estruturados
# - fundamentos do quantum
# - evidências literais
# - verificação determinística de evidências
# - correção automática da fonte da evidência
# - remoção de fundamentos sem suporte textual
# - checkpoint/resume
# - auditoria regra x LLM
# ============================================================


# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(
    r"C:\Users\elson\Downloads\Projeto MVP"
)

INPUT_CSV = (
    BASE_DIR
    / "jus_expertia_deepseek_677_v2.csv"
)

OUTPUT_DIR = (
    BASE_DIR
    / "deepseek_677"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


CHECKPOINT_JSONL = (
    OUTPUT_DIR
    / "checkpoint_deepseek_677.jsonl"
)

RESULTADOS_CSV = (
    OUTPUT_DIR
    / "resultados_deepseek_677.csv"
)

DIVERGENCIAS_CSV = (
    OUTPUT_DIR
    / "divergencias_deepseek_677.csv"
)

ERROS_JSONL = (
    OUTPUT_DIR
    / "erros_deepseek_677.jsonl"
)

RAW_JSONL = (
    OUTPUT_DIR
    / "raw_deepseek_677.jsonl"
)

PROGRESSO_JSON = (
    OUTPUT_DIR
    / "progresso_deepseek_677.json"
)

RESUMO_JSON = (
    OUTPUT_DIR
    / "resumo_deepseek_677.json"
)


TOGETHER_URL = (
    "https://api.together.xyz/v1/chat/completions"
)

MODEL = (
    "deepseek-ai/DeepSeek-V4-Flash-0731"
)


# Alterar este identificador força reprocessamento
# dos checkpoints produzidos por versões anteriores.
VALIDACAO_EVIDENCIA_VERSAO = (
    "EVIDENCIA_LITERAL_V1"
)


HTTP_TIMEOUT_TRIBUNAIS = 45
HTTP_TIMEOUT_LLM = 180

MAX_TENTATIVAS_COLETA = 3
MAX_TENTATIVAS_LLM = 4

PAUSA_ENTRE_LLM = 0.30

SALVAR_CONSOLIDADO_A_CADA = 10


MAX_EMENTA_CHARS = 14000

MAX_INTEIRO_TEOR_RELEVANTE_CHARS = 18000

JANELA_ANTES = 1300
JANELA_DEPOIS = 2200

MAX_JANELAS = 12


# ============================================================
# TERMOS PARA SELEÇÃO DO INTEIRO TEOR
# ============================================================

TERMOS_RELEVANTES = [
    "negativação",
    "negativacao",
    "cadastro de inadimplentes",
    "cadastros de inadimplentes",
    "cadastro restritivo",
    "cadastros restritivos",
    "restrição de crédito",
    "restricao de credito",
    "restritivo de crédito",
    "restritivo de credito",
    "spc",
    "serasa",
    "dano moral",
    "danos morais",
    "quantum",
    "indenização",
    "indenizacao",
    "indenizatório",
    "indenizatorio",
    "majorar",
    "majorado",
    "majoração",
    "majoracao",
    "reduzir",
    "reduzido",
    "redução",
    "reducao",
    "manter",
    "mantido",
    "manutenção",
    "manutencao",
    "arbitrado",
    "fixado",
    "in re ipsa",
    "súmula 385",
    "sumula 385",
    "súmula 479",
    "sumula 479",
    "responsabilidade objetiva",
    "fortuito interno",
    "fraude de terceiro",
    "fraude praticada por terceiro",
    "fraude",
    "inexistência do débito",
    "inexistencia do debito",
    "inscrição preexistente",
    "inscricao preexistente",
    "anotação preexistente",
    "anotacao preexistente",
    "razoabilidade",
    "proporcionalidade",
    "caráter pedagógico",
    "carater pedagogico",
    "caráter compensatório",
    "carater compensatorio",
    "enriquecimento sem causa",
    "extensão do dano",
    "extensao do dano",
    "capacidade econômica",
    "capacidade economica",
    "falha na prestação",
    "falha na prestacao",
    "falha do serviço",
    "falha do servico",
    "recurso provido",
    "recurso desprovido",
    "parcialmente provido",
    "parcial provimento",
]


# ============================================================
# CATEGORIAS
# ============================================================

CATEGORIAS_FUNDAMENTOS = [
    "DANO_IN_RE_IPSA",
    "SUMULA_385_STJ",
    "SUMULA_479_STJ",
    "INSCRICAO_PREEXISTENTE",
    "RESPONSABILIDADE_OBJETIVA",
    "FALHA_PRESTACAO_SERVICO",
    "FRAUDE_TERCEIRO",
    "INEXISTENCIA_DEBITO",
    "IRREGULARIDADE_NEGATIVACAO",
    "RAZOABILIDADE_PROPORCIONALIDADE",
    "CARATER_PEDAGOGICO",
    "CARATER_COMPENSATORIO",
    "ENRIQUECIMENTO_SEM_CAUSA",
    "EXTENSAO_DANO",
    "CAPACIDADE_ECONOMICA_PARTES",
    "JURISPRUDENCIA_TRIBUNAL",
    "CDC",
    "FORTUITO_INTERNO",
    "ONUS_DA_PROVA",
    "OUTRO",
]


# ============================================================
# MARCADORES QUE NÃO PODEM SER ACEITOS COMO EVIDÊNCIA
# ============================================================

MARCADORES_SEM_EVIDENCIA = {
    "",
    "NAO_MENCIONADA",
    "NAO_MENCIONADO",
    "NAO_DETERMINADO",
    "NÃO MENCIONADA",
    "NÃO MENCIONADO",
    "NÃO DETERMINADO",
    "AUSENTE",
    "N/A",
    "NA",
    "NULL",
    "NONE",
}


# ============================================================
# STRUCTURED OUTPUT
# ============================================================

SCHEMA = {
    "type": "object",
    "additionalProperties": False,

    "properties": {

        "tema_confirmado_llm": {
            "type": "boolean"
        },

        "evidencia_tema_llm": {
            "type": "string"
        },

        "negativacao_ocorreu_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_DETERMINADO"
            ]
        },

        "tipo_negativacao_llm": {
            "type": "string"
        },

        "orgao_restritivo_llm": {
            "type": "string"
        },

        "dano_moral_reconhecido_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_DETERMINADO"
            ]
        },

        "resultado_llm": {
            "type": "string",
            "enum": [
                "MAJORADO",
                "REDUZIDO",
                "MANTIDO",
                "RECONHECIDO",
                "NAO_DETERMINADO"
            ]
        },

        "evidencia_resultado_llm": {
            "type": "string"
        },

        "valor_final_dano_moral_llm": {
            "anyOf": [
                {
                    "type": "number"
                },
                {
                    "type": "null"
                }
            ]
        },

        "evidencia_valor_llm": {
            "type": "string"
        },

        "valor_anterior_dano_moral_llm": {
            "anyOf": [
                {
                    "type": "number"
                },
                {
                    "type": "null"
                }
            ]
        },

        "dano_in_re_ipsa_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADO"
            ]
        },

        "evidencia_in_re_ipsa_llm": {
            "type": "string"
        },

        "sumula_385_stj_llm": {
            "type": "string",
            "enum": [
                "APLICADA",
                "AFASTADA",
                "MENCIONADA",
                "NAO_MENCIONADA"
            ]
        },

        "inscricao_preexistente_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADA"
            ]
        },

        "existencia_debito_llm": {
            "type": "string",
            "enum": [
                "EXISTENTE",
                "INEXISTENTE",
                "CONTROVERTIDO",
                "NAO_DETERMINADO"
            ]
        },

        "legitimidade_debito_llm": {
            "type": "string",
            "enum": [
                "LEGITIMO",
                "ILEGITIMO",
                "CONTROVERTIDO",
                "NAO_DETERMINADO"
            ]
        },

        "fraude_terceiro_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADA"
            ]
        },

        "responsabilidade_objetiva_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADA"
            ]
        },

        "falha_prestacao_servico_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADA"
            ]
        },

        "razoabilidade_proporcionalidade_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADA"
            ]
        },

        "carater_pedagogico_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADO"
            ]
        },

        "carater_compensatorio_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADO"
            ]
        },

        "enriquecimento_sem_causa_llm": {
            "type": "string",
            "enum": [
                "SIM",
                "NAO",
                "NAO_MENCIONADO"
            ]
        },

        "criterios_quantum_llm": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },

        "motivo_resultado_llm": {
            "type": "string"
        },

        "justificativa_quantum_llm": {
            "type": "string"
        },

        "tese_central_llm": {
            "type": "string"
        },

        "fundamento_dominante_llm": {
            "type": "string"
        },

        "fundamentos_secundarios_llm": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },

        "fundamentos_decisao_llm": {
            "type": "array",

            "items": {
                "type": "object",
                "additionalProperties": False,

                "properties": {

                    "categoria": {
                        "type": "string",
                        "enum": CATEGORIAS_FUNDAMENTOS
                    },

                    "fundamento": {
                        "type": "string"
                    },

                    "fonte": {
                        "type": "string",
                        "enum": [
                            "EMENTA",
                            "INTEIRO_TEOR"
                        ]
                    },

                    "evidencia_literal": {
                        "type": "string"
                    }
                },

                "required": [
                    "categoria",
                    "fundamento",
                    "fonte",
                    "evidencia_literal"
                ]
            }
        },

        "fundamentos_quantum_llm": {
            "type": "array",

            "items": {
                "type": "object",
                "additionalProperties": False,

                "properties": {

                    "criterio": {
                        "type": "string"
                    },

                    "efeito_no_valor": {
                        "type": "string",
                        "enum": [
                            "MAJORACAO",
                            "REDUCAO",
                            "MANUTENCAO",
                            "NAO_IDENTIFICADO"
                        ]
                    },

                    "fonte": {
                        "type": "string",
                        "enum": [
                            "EMENTA",
                            "INTEIRO_TEOR"
                        ]
                    },

                    "evidencia_literal": {
                        "type": "string"
                    }
                },

                "required": [
                    "criterio",
                    "efeito_no_valor",
                    "fonte",
                    "evidencia_literal"
                ]
            }
        },

        "precedentes_sumulas_relevantes_llm": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },

        "sintese_juridica_llm": {
            "type": "string"
        },

        "confianca_llm": {
            "type": "number",
            "minimum": 0,
            "maximum": 1
        },

        "necessita_revisao_humana_llm": {
            "type": "boolean"
        },

        "motivo_revisao_humana_llm": {
            "type": "string"
        }
    },

    "required": [
        "tema_confirmado_llm",
        "evidencia_tema_llm",
        "negativacao_ocorreu_llm",
        "tipo_negativacao_llm",
        "orgao_restritivo_llm",
        "dano_moral_reconhecido_llm",
        "resultado_llm",
        "evidencia_resultado_llm",
        "valor_final_dano_moral_llm",
        "evidencia_valor_llm",
        "valor_anterior_dano_moral_llm",
        "dano_in_re_ipsa_llm",
        "evidencia_in_re_ipsa_llm",
        "sumula_385_stj_llm",
        "inscricao_preexistente_llm",
        "existencia_debito_llm",
        "legitimidade_debito_llm",
        "fraude_terceiro_llm",
        "responsabilidade_objetiva_llm",
        "falha_prestacao_servico_llm",
        "razoabilidade_proporcionalidade_llm",
        "carater_pedagogico_llm",
        "carater_compensatorio_llm",
        "enriquecimento_sem_causa_llm",
        "criterios_quantum_llm",
        "motivo_resultado_llm",
        "justificativa_quantum_llm",
        "tese_central_llm",
        "fundamento_dominante_llm",
        "fundamentos_secundarios_llm",
        "fundamentos_decisao_llm",
        "fundamentos_quantum_llm",
        "precedentes_sumulas_relevantes_llm",
        "sintese_juridica_llm",
        "confianca_llm",
        "necessita_revisao_humana_llm",
        "motivo_revisao_humana_llm"
    ]
}


# ============================================================
# SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT = """
Você é um assistente especializado em análise jurisprudencial brasileira.

Analise exclusivamente o material judicial fornecido.

OBJETO:

Decisões relacionadas à inscrição ou negativação em cadastros
restritivos de crédito e eventual dano moral.

REGRAS OBRIGATÓRIAS:

1. Não invente fatos, fundamentos, valores, súmulas, precedentes,
posições das partes ou conclusões.

2. Use exclusivamente o texto fornecido.

3. Ausência de menção não significa negativa.

4. Quando determinada informação não estiver presente, use:
NAO_MENCIONADA,
NAO_MENCIONADO,
NAO_DETERMINADO
ou null,
conforme o campo.

5. Diferencie rigorosamente:
- valor pedido;
- valor fixado em primeiro grau;
- valor anteriormente existente;
- valor final depois do julgamento;
- dano material;
- dano moral;
- honorários;
- multa;
- custas;
- outros valores.

6. valor_final_dano_moral_llm deve corresponder exclusivamente ao
valor FINAL da indenização por dano moral depois do julgamento analisado.

7. resultado_llm:

MAJORADO:
o julgamento aumentou o valor anteriormente fixado.

REDUZIDO:
o julgamento diminuiu o valor anteriormente fixado.

MANTIDO:
já havia indenização por dano moral e o julgamento preservou o valor.

RECONHECIDO:
o próprio julgamento reconheceu ou concedeu dano moral que antes
não estava favoravelmente fixado.

NAO_DETERMINADO:
o texto não permite estabelecer o resultado com segurança.

8. Não classifique como RECONHECIDO simplesmente porque o acórdão afirma
a existência de dano moral. Verifique o efeito do julgamento sobre
a decisão anterior.

9. Evidências literais devem ser trechos curtos realmente existentes
no material fornecido.

10. Quando houver EMENTA e INTEIRO_TEOR, identifique corretamente
a fonte de cada fundamento.

11. Não atribua ao INTEIRO_TEOR um trecho que aparece somente na EMENTA,
nem atribua à EMENTA um trecho que aparece somente no INTEIRO_TEOR.

12. Fundamentos secundários devem estar diretamente relacionados a:
- negativação;
- débito;
- responsabilidade;
- dano moral;
- quantum indenizatório.

13. Não inclua questões processuais ou contratuais irrelevantes ao objeto.

14. Precedentes, súmulas e artigos somente podem ser registrados quando
efetivamente mencionados no texto.

15. Se houver conflito relevante entre ementa e inteiro teor, marque:
necessita_revisao_humana_llm = true.

16. IMPORTANTE:
fundamentos_decisao_llm e fundamentos_quantum_llm devem conter
APENAS fundamentos efetivamente presentes.

17. Nunca crie um item de fundamento apenas para dizer que determinado
fundamento não foi mencionado.

18. Se algo não foi mencionado, NÃO o inclua nos arrays de fundamentos.

19. evidencia_literal deve ser uma reprodução curta e literal do texto.
Não use NAO_MENCIONADA, NAO_MENCIONADO ou NAO_DETERMINADO como evidência.

FUNDAMENTOS DA DECISÃO:

Em fundamentos_decisao_llm registre apenas fundamentos jurídicos
efetivamente utilizados.

Para cada fundamento informe:
- categoria;
- descrição objetiva;
- fonte;
- evidência literal.

Não inclua fundamento apenas porque ele seria juridicamente aplicável.

Se nenhum fundamento puder ser identificado com segurança, retorne [].

FUNDAMENTOS DO QUANTUM:

Em fundamentos_quantum_llm registre critérios usados para justificar
o valor da indenização somente quando estiverem realmente no texto.

Exemplos possíveis:
- razoabilidade;
- proporcionalidade;
- extensão do dano;
- gravidade da conduta;
- capacidade econômica das partes;
- caráter pedagógico;
- caráter compensatório;
- vedação ao enriquecimento sem causa;
- jurisprudência do tribunal;
- circunstâncias do caso concreto.

Para cada critério indique:
MAJORACAO,
REDUCAO,
MANUTENCAO
ou NAO_IDENTIFICADO.

A evidência literal deve existir no material fornecido.

SÍNTESE:

A sintese_juridica_llm deve explicar sucintamente:
- o que aconteceu;
- a conclusão sobre a negativação;
- a conclusão sobre dano moral;
- o resultado recursal;
- o valor final, quando identificável;
- os principais fundamentos.

Retorne exclusivamente JSON compatível com o schema solicitado.
"""


# ============================================================
# SESSÕES HTTP
# ============================================================

session_tribunais = requests.Session()

session_tribunais.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/153.0 Safari/537.36"
        ),

        "Accept-Language": (
            "pt-BR,pt;q=0.9,en;q=0.7"
        ),
    }
)


session_llm = requests.Session()


class ErroAutenticacaoTogether(Exception):
    pass


# ============================================================
# UTILIDADES
# ============================================================

def agora_iso():

    return datetime.now(
        timezone.utc
    ).isoformat()


def limpar_str(valor):

    if valor is None:
        return ""

    valor = str(
        valor
    ).strip()

    if valor.lower() in {
        "",
        "null",
        "none",
        "nan",
        "nat"
    }:
        return ""

    return valor


def limpar_texto(texto):

    if not texto:
        return ""

    texto = html.unescape(
        str(
            texto
        )
    )

    texto = texto.replace(
        "\x00",
        " "
    )

    texto = texto.replace(
        "\r",
        "\n"
    )

    texto = re.sub(
        r"[ \t\f\v]+",
        " ",
        texto
    )

    texto = re.sub(
        r"\n[ \t]+",
        "\n",
        texto
    )

    texto = re.sub(
        r"\n{3,}",
        "\n\n",
        texto
    )

    return texto.strip()


def html_para_texto(texto_html):

    if not texto_html:
        return ""

    try:

        soup = BeautifulSoup(
            texto_html,
            "html.parser"
        )

        for tag in soup(
            [
                "script",
                "style",
                "noscript",
                "svg",
                "nav",
                "footer"
            ]
        ):
            tag.decompose()

        texto = soup.get_text(
            "\n",
            strip=True
        )

        return limpar_texto(
            texto
        )

    except Exception:

        return limpar_texto(
            texto_html
        )


def normalizar_busca(texto):

    if not texto:
        return ""

    texto = unicodedata.normalize(
        "NFKD",
        str(
            texto
        )
    )

    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(
            caractere
        )
    )

    return texto.lower()


def normalizar_evidencia(texto):

    if not texto:
        return ""

    texto = html.unescape(
        str(
            texto
        )
    )

    texto = unicodedata.normalize(
        "NFKD",
        texto
    )

    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(
            caractere
        )
    )

    texto = texto.lower()

    texto = texto.replace(
        "“",
        '"'
    )

    texto = texto.replace(
        "”",
        '"'
    )

    texto = texto.replace(
        "‘",
        "'"
    )

    texto = texto.replace(
        "’",
        "'"
    )

    texto = re.sub(
        r"\s+",
        " ",
        texto
    )

    return texto.strip()


def safe_float(valor):

    if valor is None:
        return None

    if isinstance(
        valor,
        (int, float)
    ):
        return float(
            valor
        )

    texto = str(
        valor
    ).strip()

    if not texto:
        return None

    texto = texto.replace(
        "R$",
        ""
    ).strip()

    if (
        "," in texto
        and "." in texto
    ):

        texto = texto.replace(
            ".",
            ""
        ).replace(
            ",",
            "."
        )

    elif "," in texto:

        texto = texto.replace(
            ",",
            "."
        )

    try:

        return float(
            texto
        )

    except Exception:

        return None


def json_string(valor):

    return json.dumps(
        valor,
        ensure_ascii=False
    )


# ============================================================
# API KEY
# ============================================================

def obter_api_key():

    chave_ambiente = limpar_str(
        os.getenv(
            "TOGETHER_API_KEY"
        )
    )

    if chave_ambiente:

        print(
            "TOGETHER_API_KEY encontrada "
            "na variável de ambiente."
        )

        return chave_ambiente

    while True:

        print()
        print(
            "=" * 80
        )

        print(
            "CHAVE DA TOGETHER AI"
        )

        print(
            "=" * 80
        )

        print(
            "Cole sua API Key da Together AI abaixo."
        )

        print(
            "Use Ctrl+V ou clique com o botão direito."
        )

        print(
            "Por segurança, nenhum caractere aparecerá na tela."
        )

        print(
            "Depois de colar, pressione ENTER."
        )

        print()

        chave = getpass.getpass(
            "TOGETHER_API_KEY: "
        )

        chave = limpar_str(
            chave
        )

        if not chave:

            print()
            print(
                "ERRO: nenhuma chave foi informada."
            )

            continue

        if (
            "\n" in chave
            or "\r" in chave
        ):

            print(
                "ERRO: o conteúdo contém múltiplas linhas."
            )

            continue

        if (
            "import " in chave.lower()
            or "def " in chave.lower()
            or "class " in chave.lower()
            or "from " in chave.lower()
        ):

            print(
                "ERRO: o conteúdo parece código Python."
            )

            continue

        if len(
            chave
        ) > 500:

            print(
                "ERRO: conteúdo grande demais para uma API Key."
            )

            continue

        if len(
            chave
        ) < 20:

            print(
                "ERRO: chave curta demais."
            )

            continue

        if any(
            caractere.isspace()
            for caractere in chave
        ):

            print(
                "ERRO: chave contém espaço ou quebra de linha."
            )

            continue

        print()
        print(
            "Chave recebida."
        )

        print(
            "Ela não será exibida nem gravada "
            "nos arquivos do projeto."
        )

        return chave


# ============================================================
# CSV
# ============================================================

def ler_csv():

    if not INPUT_CSV.exists():

        raise FileNotFoundError(
            f"Arquivo CSV não encontrado:\n{INPUT_CSV}"
        )

    with INPUT_CSV.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as arquivo:

        registros = list(
            csv.DictReader(
                arquivo
            )
        )

    return registros


# ============================================================
# CHECKPOINT
# ============================================================

def carregar_checkpoint():

    resultados = {}

    if not CHECKPOINT_JSONL.exists():
        return resultados

    with CHECKPOINT_JSONL.open(
        "r",
        encoding="utf-8"
    ) as arquivo:

        for linha in arquivo:

            linha = linha.strip()

            if not linha:
                continue

            try:

                objeto = json.loads(
                    linha
                )

                id_llm = objeto.get(
                    "id_llm"
                )

                status = objeto.get(
                    "_status"
                )

                versao = objeto.get(
                    "validacao_evidencia_versao"
                )

                if (
                    id_llm
                    and status == "OK"
                    and versao
                    == VALIDACAO_EVIDENCIA_VERSAO
                ):

                    resultados[
                        id_llm
                    ] = objeto

            except Exception:

                continue

    return resultados


def append_jsonl(
    caminho,
    objeto
):

    with caminho.open(
        "a",
        encoding="utf-8"
    ) as arquivo:

        arquivo.write(
            json.dumps(
                objeto,
                ensure_ascii=False
            )
        )

        arquivo.write(
            "\n"
        )


# ============================================================
# TJPA
# ============================================================

def extrair_id_documento_tjpa(
    url
):

    url = limpar_str(
        url
    )

    if not url:
        return None

    match = re.search(
        r"/documento/(\d+)",
        url
    )

    if not match:
        return None

    return match.group(
        1
    )


def buscar_inteiro_teor_tjpa(
    url_oficial
):

    id_documento = (
        extrair_id_documento_tjpa(
            url_oficial
        )
    )

    if not id_documento:

        return {
            "obtido": False,
            "texto": "",
            "metodo": "TJPA_ID_NAO_IDENTIFICADO",
            "http_status": None,
            "campo": None,
            "erro": (
                "ID do documento não identificado "
                "na URL oficial."
            )
        }

    endpoint = (
        "https://jurisprudencia.tjpa.jus.br"
        "/bff/api/decisoes/"
        "buscar-por-numero-documento"
    )

    headers = {

        "Accept": (
            "application/json, text/plain, */*"
        ),

        "Content-Type": (
            "application/json"
        ),

        "Origin": (
            "https://jurisprudencia.tjpa.jus.br"
        ),

        "Referer": (
            url_oficial
        ),
    }

    ultimo_erro = None

    for tentativa in range(
        1,
        MAX_TENTATIVAS_COLETA + 1
    ):

        try:

            resposta = session_tribunais.post(
                endpoint,
                headers=headers,
                json={
                    "id": int(
                        id_documento
                    )
                },
                timeout=HTTP_TIMEOUT_TRIBUNAIS
            )

            if resposta.status_code != 200:

                ultimo_erro = (
                    f"HTTP {resposta.status_code}"
                )

                time.sleep(
                    tentativa
                )

                continue

            dados = resposta.json()

            data = (
                dados.get(
                    "data"
                )
                if isinstance(
                    dados,
                    dict
                )
                else None
            )

            content = (
                data.get(
                    "content"
                )
                if isinstance(
                    data,
                    dict
                )
                else None
            )

            if (
                not isinstance(
                    content,
                    list
                )
                or not content
            ):

                ultimo_erro = (
                    "API respondeu sem documento."
                )

                time.sleep(
                    tentativa
                )

                continue

            documento = content[
                0
            ]

            if not isinstance(
                documento,
                dict
            ):

                ultimo_erro = (
                    "Documento retornado "
                    "em formato inesperado."
                )

                continue

            textopuro = limpar_texto(
                documento.get(
                    "textopuro"
                )
            )

            if len(
                textopuro
            ) >= 1000:

                texto = textopuro
                campo = "textopuro"

            else:

                textooriginal = (
                    html_para_texto(
                        documento.get(
                            "textooriginal"
                        )
                    )
                )

                if len(
                    textooriginal
                ) >= 1000:

                    texto = textooriginal
                    campo = "textooriginal"

                else:

                    texto = ""
                    campo = None

            if not texto:

                return {
                    "obtido": False,
                    "texto": "",
                    "metodo": "TJPA_API_PUBLICA",
                    "http_status": resposta.status_code,
                    "campo": campo,
                    "erro": (
                        "API respondeu, mas não houve "
                        "inteiro teor utilizável."
                    )
                }

            return {
                "obtido": True,
                "texto": texto,
                "metodo": "TJPA_API_PUBLICA",
                "http_status": resposta.status_code,
                "campo": campo,
                "caracteres": len(
                    texto
                ),
                "erro": None
            }

        except Exception as erro:

            ultimo_erro = repr(
                erro
            )

            time.sleep(
                tentativa
            )

    return {
        "obtido": False,
        "texto": "",
        "metodo": "TJPA_API_PUBLICA",
        "http_status": None,
        "campo": None,
        "erro": ultimo_erro
    }


# ============================================================
# TJSE
# ============================================================

def buscar_inteiro_teor_tjse(
    url_oficial
):

    url_oficial = limpar_str(
        url_oficial
    )

    if not url_oficial:

        return {
            "obtido": False,
            "texto": "",
            "metodo": "TJSE_URL_AUSENTE",
            "http_status": None,
            "campo": None,
            "erro": "URL oficial ausente."
        }

    ultimo_erro = None

    for tentativa in range(
        1,
        MAX_TENTATIVAS_COLETA + 1
    ):

        try:

            resposta = session_tribunais.get(
                url_oficial,
                timeout=HTTP_TIMEOUT_TRIBUNAIS,
                allow_redirects=True
            )

            if resposta.status_code != 200:

                ultimo_erro = (
                    f"HTTP {resposta.status_code}"
                )

                time.sleep(
                    tentativa
                )

                continue

            content_type = (
                resposta.headers.get(
                    "Content-Type",
                    ""
                ).lower()
            )

            if "html" not in content_type:

                ultimo_erro = (
                    "Conteúdo não HTML: "
                    + content_type
                )

                continue

            # TJSE frequentemente declara ISO-8859-1.
            if resposta.encoding is None:

                resposta.encoding = (
                    resposta.apparent_encoding
                    or "iso-8859-1"
                )

            texto = html_para_texto(
                resposta.text
            )

            if len(
                texto
            ) < 1000:

                ultimo_erro = (
                    "Texto extraído muito curto."
                )

                continue

            return {
                "obtido": True,
                "texto": texto,
                "metodo": "TJSE_DIRETA_RELATORIO",
                "http_status": resposta.status_code,
                "campo": "html_relatorio",
                "caracteres": len(
                    texto
                ),
                "erro": None
            }

        except Exception as erro:

            ultimo_erro = repr(
                erro
            )

            time.sleep(
                tentativa
            )

    return {
        "obtido": False,
        "texto": "",
        "metodo": "TJSE_DIRETA_RELATORIO",
        "http_status": None,
        "campo": None,
        "erro": ultimo_erro
    }


# ============================================================
# COLETA POR TRIBUNAL
# ============================================================

def coletar_inteiro_teor(
    registro
):

    tribunal = limpar_str(
        registro.get(
            "tribunal"
        )
    ).upper()

    url = limpar_str(
        registro.get(
            "url_oficial"
        )
    )

    if tribunal == "TJPA":

        return buscar_inteiro_teor_tjpa(
            url
        )

    if tribunal == "TJSE":

        return buscar_inteiro_teor_tjse(
            url
        )

    if tribunal == "TJAM":

        return {
            "obtido": False,
            "texto": "",
            "metodo": "TJAM_EMENTA",
            "http_status": None,
            "campo": None,
            "erro": (
                "URL disponível é consulta genérica."
            )
        }

    if tribunal == "TJPE":

        return {
            "obtido": False,
            "texto": "",
            "metodo": "TJPE_EMENTA",
            "http_status": None,
            "campo": None,
            "erro": (
                "URL oficial ausente na fonte."
            )
        }

    return {
        "obtido": False,
        "texto": "",
        "metodo": "TRIBUNAL_NAO_TRATADO",
        "http_status": None,
        "campo": None,
        "erro": (
            "Tribunal não possui coletor específico."
        )
    }


# ============================================================
# RECORTE DO INTEIRO TEOR
# ============================================================

def encontrar_janelas(
    texto
):

    if not texto:
        return []

    normalizado = normalizar_busca(
        texto
    )

    intervalos = []

    for termo in TERMOS_RELEVANTES:

        termo_normalizado = normalizar_busca(
            termo
        )

        inicio_busca = 0

        while True:

            posicao = normalizado.find(
                termo_normalizado,
                inicio_busca
            )

            if posicao < 0:
                break

            inicio = max(
                0,
                posicao - JANELA_ANTES
            )

            fim = min(
                len(
                    texto
                ),
                posicao
                + len(
                    termo_normalizado
                )
                + JANELA_DEPOIS
            )

            intervalos.append(
                (
                    inicio,
                    fim
                )
            )

            inicio_busca = (
                posicao
                + len(
                    termo_normalizado
                )
            )

    if not intervalos:
        return []

    intervalos.sort()

    unidos = []

    for inicio, fim in intervalos:

        if not unidos:

            unidos.append(
                [
                    inicio,
                    fim
                ]
            )

            continue

        ultimo_fim = unidos[
            -1
        ][
            1
        ]

        if inicio <= ultimo_fim + 300:

            unidos[
                -1
            ][
                1
            ] = max(
                ultimo_fim,
                fim
            )

        else:

            unidos.append(
                [
                    inicio,
                    fim
                ]
            )

    janelas = []

    for inicio, fim in unidos[
        :MAX_JANELAS
    ]:

        trecho = limpar_texto(
            texto[
                inicio:fim
            ]
        )

        if trecho:

            janelas.append(
                trecho
            )

    return janelas


def remover_repeticoes(
    texto
):

    texto = limpar_texto(
        texto
    )

    paragrafos = re.split(
        r"\n+",
        texto
    )

    vistos = set()
    saida = []

    for paragrafo in paragrafos:

        paragrafo = limpar_texto(
            paragrafo
        )

        if not paragrafo:
            continue

        chave = normalizar_busca(
            paragrafo
        )[
            :700
        ]

        if chave in vistos:
            continue

        vistos.add(
            chave
        )

        saida.append(
            paragrafo
        )

    return "\n".join(
        saida
    )


def selecionar_inteiro_teor_relevante(
    texto
):

    texto = limpar_texto(
        texto
    )

    if not texto:
        return ""

    if len(
        texto
    ) <= MAX_INTEIRO_TEOR_RELEVANTE_CHARS:

        return texto

    janelas = encontrar_janelas(
        texto
    )

    if not janelas:

        return texto[
            :MAX_INTEIRO_TEOR_RELEVANTE_CHARS
        ]

    selecionado = ""

    for indice, trecho in enumerate(
        janelas,
        start=1
    ):

        bloco = (
            f"\n\n[TRECHO RELEVANTE {indice}]\n"
            f"{trecho}"
        )

        if (
            len(
                selecionado
            )
            + len(
                bloco
            )
            > MAX_INTEIRO_TEOR_RELEVANTE_CHARS
        ):

            restante = (
                MAX_INTEIRO_TEOR_RELEVANTE_CHARS
                - len(
                    selecionado
                )
            )

            if restante > 500:

                selecionado += bloco[
                    :restante
                ]

            break

        selecionado += bloco

    return remover_repeticoes(
        selecionado
    )


# ============================================================
# PROMPT
# ============================================================

def construir_prompt(
    registro,
    coleta
):

    tribunal = limpar_str(
        registro.get(
            "tribunal"
        )
    )

    processo = limpar_str(
        registro.get(
            "processo"
        )
    )

    numero_acordao = limpar_str(
        registro.get(
            "numero_acordao"
        )
    )

    classe = limpar_str(
        registro.get(
            "classe"
        )
    )

    relator = limpar_str(
        registro.get(
            "relator"
        )
    )

    orgao = limpar_str(
        registro.get(
            "orgao"
        )
    )

    data_referencia = limpar_str(
        registro.get(
            "data_referencia"
        )
    )

    ementa = limpar_texto(
        registro.get(
            "ementa_llm"
        )
    )

    if not ementa:

        ementa = limpar_texto(
            registro.get(
                "ementa"
            )
        )

    ementa = ementa[
        :MAX_EMENTA_CHARS
    ]

    inteiro = ""

    if coleta.get(
        "obtido"
    ):

        inteiro = (
            selecionar_inteiro_teor_relevante(
                coleta.get(
                    "texto",
                    ""
                )
            )
        )

    fonte_analise = (
        "EMENTA_E_INTEIRO_TEOR"
        if inteiro
        else "EMENTA"
    )

    partes = [
        "ANÁLISE JURISPRUDENCIAL",
        "",
        "METADADOS NEUTROS:",
        f"Tribunal: {tribunal}",
        f"Processo: {processo}",
        f"Número do acórdão: {numero_acordao}",
        f"Classe: {classe}",
        f"Relator: {relator}",
        f"Órgão julgador: {orgao}",
        f"Data de referência: {data_referencia}",
        "",
        (
            "Os metadados acima servem apenas para identificação. "
            "Não inferir fatos jurídicos a partir deles."
        ),
        "",
        "================ EMENTA ================",
        ementa,
        "================ FIM EMENTA ================",
    ]

    if inteiro:

        partes.extend(
            [
                "",
                (
                    "================ TRECHOS DO "
                    "INTEIRO TEOR ================"
                ),

                inteiro,

                (
                    "================ FIM DOS TRECHOS "
                    "DO INTEIRO TEOR ================"
                ),
            ]
        )

    partes.extend(
        [
            "",
            (
                "Analise exclusivamente o conteúdo acima "
                "e produza o JSON solicitado."
            )
        ]
    )

    prompt = "\n".join(
        partes
    )

    return (
        prompt,
        fonte_analise,
        len(
            ementa
        ),
        len(
            inteiro
        ),
        ementa,
        inteiro
    )


# ============================================================
# VALIDAÇÃO DETERMINÍSTICA DAS EVIDÊNCIAS
# ============================================================

def evidencia_eh_marcador(
    evidencia
):

    evidencia = limpar_str(
        evidencia
    )

    if not evidencia:
        return True

    evidencia_upper = evidencia.upper()

    if evidencia_upper in MARCADORES_SEM_EVIDENCIA:
        return True

    return False


def evidencia_existe(
    evidencia,
    texto
):

    if evidencia_eh_marcador(
        evidencia
    ):
        return False

    if not texto:
        return False

    evidencia_norm = normalizar_evidencia(
        evidencia
    )

    texto_norm = normalizar_evidencia(
        texto
    )

    if not evidencia_norm:
        return False

    return evidencia_norm in texto_norm


def validar_item_fundamento(
    item,
    ementa,
    inteiro,
    tipo_item
):

    if not isinstance(
        item,
        dict
    ):

        return (
            None,
            {
                "tipo": tipo_item,
                "acao": "REMOVIDO",
                "motivo": (
                    "Item não é objeto JSON."
                ),
                "item_original": item
            }
        )

    item = dict(
        item
    )

    evidencia = limpar_str(
        item.get(
            "evidencia_literal"
        )
    )

    fonte = limpar_str(
        item.get(
            "fonte"
        )
    ).upper()

    if evidencia_eh_marcador(
        evidencia
    ):

        return (
            None,
            {
                "tipo": tipo_item,
                "acao": "REMOVIDO",
                "motivo": (
                    "Evidência ausente ou marcador "
                    "de informação não mencionada."
                ),
                "item_original": item
            }
        )

    if fonte == "EMENTA":

        if evidencia_existe(
            evidencia,
            ementa
        ):

            return (
                item,
                None
            )

        if (
            inteiro
            and evidencia_existe(
                evidencia,
                inteiro
            )
        ):

            fonte_anterior = fonte

            item[
                "fonte"
            ] = "INTEIRO_TEOR"

            return (
                item,
                {
                    "tipo": tipo_item,
                    "acao": "FONTE_CORRIGIDA",
                    "fonte_anterior": fonte_anterior,
                    "fonte_corrigida": "INTEIRO_TEOR",
                    "evidencia_literal": evidencia
                }
            )

        return (
            None,
            {
                "tipo": tipo_item,
                "acao": "REMOVIDO",
                "motivo": (
                    "Evidência não localizada "
                    "literalmente na ementa nem "
                    "no inteiro teor enviado."
                ),
                "item_original": item
            }
        )

    if fonte == "INTEIRO_TEOR":

        if (
            inteiro
            and evidencia_existe(
                evidencia,
                inteiro
            )
        ):

            return (
                item,
                None
            )

        if evidencia_existe(
            evidencia,
            ementa
        ):

            fonte_anterior = fonte

            item[
                "fonte"
            ] = "EMENTA"

            return (
                item,
                {
                    "tipo": tipo_item,
                    "acao": "FONTE_CORRIGIDA",
                    "fonte_anterior": fonte_anterior,
                    "fonte_corrigida": "EMENTA",
                    "evidencia_literal": evidencia
                }
            )

        return (
            None,
            {
                "tipo": tipo_item,
                "acao": "REMOVIDO",
                "motivo": (
                    "Evidência não localizada "
                    "literalmente no inteiro teor "
                    "nem na ementa enviada."
                ),
                "item_original": item
            }
        )

    return (
        None,
        {
            "tipo": tipo_item,
            "acao": "REMOVIDO",
            "motivo": (
                "Fonte inválida ou ausente."
            ),
            "item_original": item
        }
    )


def auditar_fundamentos_llm(
    dados,
    ementa,
    inteiro
):

    dados = dict(
        dados
    )

    auditoria = []

    fundamentos_validos = []

    fundamentos_originais = dados.get(
        "fundamentos_decisao_llm"
    )

    if not isinstance(
        fundamentos_originais,
        list
    ):

        fundamentos_originais = []

    for item in fundamentos_originais:

        item_validado, evento = (
            validar_item_fundamento(
                item,
                ementa,
                inteiro,
                "FUNDAMENTO_DECISAO"
            )
        )

        if item_validado is not None:

            fundamentos_validos.append(
                item_validado
            )

        if evento is not None:

            auditoria.append(
                evento
            )

    quantum_validos = []

    quantum_originais = dados.get(
        "fundamentos_quantum_llm"
    )

    if not isinstance(
        quantum_originais,
        list
    ):

        quantum_originais = []

    for item in quantum_originais:

        item_validado, evento = (
            validar_item_fundamento(
                item,
                ementa,
                inteiro,
                "FUNDAMENTO_QUANTUM"
            )
        )

        if item_validado is not None:

            quantum_validos.append(
                item_validado
            )

        if evento is not None:

            auditoria.append(
                evento
            )

    dados[
        "fundamentos_decisao_llm"
    ] = fundamentos_validos

    dados[
        "fundamentos_quantum_llm"
    ] = quantum_validos

    qtd_removidos = sum(
        1
        for evento in auditoria
        if evento.get(
            "acao"
        )
        == "REMOVIDO"
    )

    qtd_corrigidos = sum(
        1
        for evento in auditoria
        if evento.get(
            "acao"
        )
        == "FONTE_CORRIGIDA"
    )

    return {
        "dados": dados,

        "auditoria": auditoria,

        "qtd_fundamentos_decisao_antes": (
            len(
                fundamentos_originais
            )
        ),

        "qtd_fundamentos_decisao_depois": (
            len(
                fundamentos_validos
            )
        ),

        "qtd_fundamentos_quantum_antes": (
            len(
                quantum_originais
            )
        ),

        "qtd_fundamentos_quantum_depois": (
            len(
                quantum_validos
            )
        ),

        "qtd_evidencias_removidas": (
            qtd_removidos
        ),

        "qtd_fontes_corrigidas": (
            qtd_corrigidos
        )
    }


# ============================================================
# TOGETHER / DEEPSEEK
# ============================================================

def chamar_deepseek(
    api_key,
    user_prompt
):

    headers = {

        "Authorization": (
            f"Bearer {api_key}"
        ),

        "Content-Type": (
            "application/json"
        ),
    }

    payload = {

        "model": MODEL,

        "temperature": 0.05,

        "max_tokens": 4200,

        "messages": [
            {
                "role": "system",
                "content": SYSTEM_PROMPT
            },

            {
                "role": "user",
                "content": user_prompt
            }
        ],

        "response_format": {

            "type": "json_schema",

            "json_schema": {

                "name": (
                    "jus_expertia_analise"
                ),

                "strict": True,

                "schema": SCHEMA
            }
        }
    }

    ultimo_erro = None

    for tentativa in range(
        1,
        MAX_TENTATIVAS_LLM + 1
    ):

        try:

            inicio = time.time()

            resposta = session_llm.post(
                TOGETHER_URL,
                headers=headers,
                json=payload,
                timeout=HTTP_TIMEOUT_LLM
            )

            tempo = (
                time.time()
                - inicio
            )

            if resposta.status_code == 401:

                raise ErroAutenticacaoTogether(
                    "A Together AI recusou a API Key "
                    "com HTTP 401."
                )

            if resposta.status_code in {
                400,
                403
            }:

                return {

                    "ok": False,

                    "dados": None,

                    "raw": None,

                    "tempo_segundos": round(
                        tempo,
                        3
                    ),

                    "prompt_tokens": 0,

                    "completion_tokens": 0,

                    "total_tokens": 0,

                    "erro": (
                        f"HTTP {resposta.status_code}: "
                        f"{resposta.text[:1500]}"
                    )
                }

            if resposta.status_code != 200:

                ultimo_erro = (
                    f"HTTP {resposta.status_code}: "
                    f"{resposta.text[:1000]}"
                )

                espera = min(
                    30,
                    tentativa * 5
                )

                print(
                    f"  LLM HTTP {resposta.status_code}. "
                    f"Nova tentativa em {espera}s..."
                )

                if tentativa < MAX_TENTATIVAS_LLM:

                    time.sleep(
                        espera
                    )

                continue

            bruto = resposta.json()

            choices = bruto.get(
                "choices",
                []
            )

            if not choices:

                raise RuntimeError(
                    "Resposta da API sem choices."
                )

            content = (
                choices[
                    0
                ]
                .get(
                    "message",
                    {}
                )
                .get(
                    "content"
                )
            )

            if not content:

                raise RuntimeError(
                    "Resposta da API sem content."
                )

            if isinstance(
                content,
                dict
            ):

                dados = content

            else:

                dados = json.loads(
                    content
                )

            usage = bruto.get(
                "usage",
                {}
            )

            return {

                "ok": True,

                "dados": dados,

                "raw": bruto,

                "tempo_segundos": round(
                    tempo,
                    3
                ),

                "prompt_tokens": (
                    usage.get(
                        "prompt_tokens",
                        0
                    )
                    or 0
                ),

                "completion_tokens": (
                    usage.get(
                        "completion_tokens",
                        0
                    )
                    or 0
                ),

                "total_tokens": (
                    usage.get(
                        "total_tokens",
                        0
                    )
                    or 0
                ),

                "erro": None
            }

        except ErroAutenticacaoTogether:

            raise

        except Exception as erro:

            ultimo_erro = repr(
                erro
            )

            espera = min(
                30,
                tentativa * 5
            )

            print(
                f"  Erro LLM tentativa "
                f"{tentativa}: {ultimo_erro}"
            )

            if tentativa < MAX_TENTATIVAS_LLM:

                time.sleep(
                    espera
                )

    return {

        "ok": False,

        "dados": None,

        "raw": None,

        "tempo_segundos": None,

        "prompt_tokens": 0,

        "completion_tokens": 0,

        "total_tokens": 0,

        "erro": ultimo_erro
    }


# ============================================================
# VALIDAÇÃO ESTRUTURAL
# ============================================================

def validar_resultado_llm(
    dados
):

    problemas = []

    if not isinstance(
        dados,
        dict
    ):

        return [
            "Resposta não é objeto JSON."
        ]

    resultado = dados.get(
        "resultado_llm"
    )

    if resultado not in {
        "MAJORADO",
        "REDUZIDO",
        "MANTIDO",
        "RECONHECIDO",
        "NAO_DETERMINADO"
    }:

        problemas.append(
            "resultado_llm inválido."
        )

    valor = dados.get(
        "valor_final_dano_moral_llm"
    )

    if (
        valor is not None
        and not isinstance(
            valor,
            (int, float)
        )
    ):

        problemas.append(
            "valor_final_dano_moral_llm inválido."
        )

    confianca = dados.get(
        "confianca_llm"
    )

    if not isinstance(
        confianca,
        (int, float)
    ):

        problemas.append(
            "confianca_llm inválida."
        )

    elif not (
        0 <= confianca <= 1
    ):

        problemas.append(
            "confianca_llm fora de 0..1."
        )

    if not isinstance(
        dados.get(
            "fundamentos_decisao_llm"
        ),
        list
    ):

        problemas.append(
            "fundamentos_decisao_llm inválido."
        )

    if not isinstance(
        dados.get(
            "fundamentos_quantum_llm"
        ),
        list
    ):

        problemas.append(
            "fundamentos_quantum_llm inválido."
        )

    return problemas


# ============================================================
# CRIAÇÃO DO RESULTADO
# ============================================================

def criar_item_resultado(
    registro,
    coleta,
    fonte_analise,
    chars_ementa,
    chars_inteiro,
    ementa_enviada,
    inteiro_enviado,
    resposta
):

    dados_originais = resposta[
        "dados"
    ]

    auditoria = auditar_fundamentos_llm(
        dados_originais,
        ementa_enviada,
        inteiro_enviado
    )

    dados = auditoria[
        "dados"
    ]

    resultado_regra = limpar_str(
        registro.get(
            "resultado_regra"
        )
    )

    valor_regra = safe_float(
        registro.get(
            "valor_regra"
        )
    )

    resultado_llm = limpar_str(
        dados.get(
            "resultado_llm"
        )
    )

    valor_llm = dados.get(
        "valor_final_dano_moral_llm"
    )

    if isinstance(
        valor_llm,
        (int, float)
    ):

        valor_llm = float(
            valor_llm
        )

    divergencia_resultado = bool(
        resultado_regra
        and resultado_llm
        and resultado_regra != resultado_llm
    )

    divergencia_valor = False

    if (
        valor_regra is not None
        and valor_llm is not None
    ):

        divergencia_valor = (
            abs(
                valor_regra
                - valor_llm
            )
            > 0.01
        )

    problemas = validar_resultado_llm(
        dados
    )

    necessita_revisao = bool(
        dados.get(
            "necessita_revisao_humana_llm"
        )
    )

    if problemas:

        necessita_revisao = True

    item = {

        "_status": "OK",

        "validacao_evidencia_versao": (
            VALIDACAO_EVIDENCIA_VERSAO
        ),

        "id_llm": limpar_str(
            registro.get(
                "id_llm"
            )
        ),

        "tribunal": limpar_str(
            registro.get(
                "tribunal"
            )
        ),

        "id_origem": limpar_str(
            registro.get(
                "id_origem"
            )
        ),

        "processo": limpar_str(
            registro.get(
                "processo"
            )
        ),

        "numero_acordao": limpar_str(
            registro.get(
                "numero_acordao"
            )
        ),

        "classe": limpar_str(
            registro.get(
                "classe"
            )
        ),

        "relator": limpar_str(
            registro.get(
                "relator"
            )
        ),

        "orgao": limpar_str(
            registro.get(
                "orgao"
            )
        ),

        "data_referencia": limpar_str(
            registro.get(
                "data_referencia"
            )
        ),

        "ano": limpar_str(
            registro.get(
                "ano"
            )
        ),

        "url_oficial": limpar_str(
            registro.get(
                "url_oficial"
            )
        ),

        "tipo_url": limpar_str(
            registro.get(
                "tipo_url"
            )
        ),

        "content_hash": limpar_str(
            registro.get(
                "content_hash"
            )
        ),

        "resultado_regra": (
            resultado_regra
        ),

        "valor_regra": (
            valor_regra
        ),

        "faixa_valor": limpar_str(
            registro.get(
                "faixa_valor"
            )
        ),

        "fonte_analise_llm": (
            fonte_analise
        ),

        "inteiro_teor_obtido": bool(
            coleta.get(
                "obtido"
            )
        ),

        "metodo_coleta_inteiro_teor": (
            coleta.get(
                "metodo"
            )
        ),

        "http_status_inteiro_teor": (
            coleta.get(
                "http_status"
            )
        ),

        "campo_inteiro_teor": (
            coleta.get(
                "campo"
            )
        ),

        "caracteres_inteiro_teor_original": len(
            coleta.get(
                "texto",
                ""
            )
        ),

        "caracteres_ementa_enviados": (
            chars_ementa
        ),

        "caracteres_inteiro_teor_enviados": (
            chars_inteiro
        ),

        "erro_coleta_inteiro_teor": (
            coleta.get(
                "erro"
            )
        ),
    }

    for chave, valor in dados.items():

        item[
            chave
        ] = valor

    item[
        "resultado_llm"
    ] = resultado_llm

    item[
        "valor_final_dano_moral_llm"
    ] = valor_llm

    item[
        "necessita_revisao_humana_llm"
    ] = necessita_revisao

    item[
        "problemas_validacao_local"
    ] = problemas

    item[
        "auditoria_evidencias_fundamentos"
    ] = auditoria[
        "auditoria"
    ]

    item[
        "qtd_fundamentos_decisao_antes_auditoria"
    ] = auditoria[
        "qtd_fundamentos_decisao_antes"
    ]

    item[
        "qtd_fundamentos_decisao_depois_auditoria"
    ] = auditoria[
        "qtd_fundamentos_decisao_depois"
    ]

    item[
        "qtd_fundamentos_quantum_antes_auditoria"
    ] = auditoria[
        "qtd_fundamentos_quantum_antes"
    ]

    item[
        "qtd_fundamentos_quantum_depois_auditoria"
    ] = auditoria[
        "qtd_fundamentos_quantum_depois"
    ]

    item[
        "qtd_evidencias_removidas"
    ] = auditoria[
        "qtd_evidencias_removidas"
    ]

    item[
        "qtd_fontes_corrigidas"
    ] = auditoria[
        "qtd_fontes_corrigidas"
    ]

    item[
        "divergencia_resultado_regra_llm"
    ] = divergencia_resultado

    item[
        "divergencia_valor_regra_llm"
    ] = divergencia_valor

    item[
        "tempo_llm_segundos"
    ] = resposta.get(
        "tempo_segundos"
    )

    item[
        "prompt_tokens"
    ] = resposta.get(
        "prompt_tokens",
        0
    )

    item[
        "completion_tokens"
    ] = resposta.get(
        "completion_tokens",
        0
    )

    item[
        "total_tokens"
    ] = resposta.get(
        "total_tokens",
        0
    )

    item[
        "modelo_llm"
    ] = MODEL

    item[
        "processado_em_utc"
    ] = agora_iso()

    return item


# ============================================================
# CSV
# ============================================================

def converter_para_csv(
    item
):

    linha = {}

    for chave, valor in item.items():

        if isinstance(
            valor,
            (dict, list)
        ):

            linha[
                chave
            ] = json_string(
                valor
            )

        else:

            linha[
                chave
            ] = valor

    return linha


def salvar_csv(
    caminho,
    itens
):

    if not itens:
        return

    linhas = [
        converter_para_csv(
            item
        )
        for item in itens
    ]

    campos = []

    for linha in linhas:

        for chave in linha.keys():

            if chave not in campos:

                campos.append(
                    chave
                )

    with caminho.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as arquivo:

        writer = csv.DictWriter(
            arquivo,
            fieldnames=campos,
            extrasaction="ignore"
        )

        writer.writeheader()

        for linha in linhas:

            writer.writerow(
                linha
            )


# ============================================================
# CONSOLIDAÇÃO
# ============================================================

def consolidar(
    resultados
):

    itens = [
        item
        for item in resultados.values()
        if item.get(
            "_status"
        )
        == "OK"
    ]

    itens.sort(
        key=lambda item: (
            item.get(
                "tribunal",
                ""
            ),

            item.get(
                "processo",
                ""
            )
        )
    )

    salvar_csv(
        RESULTADOS_CSV,
        itens
    )

    divergencias = [
        item
        for item in itens
        if (
            item.get(
                "divergencia_resultado_regra_llm"
            )
            or item.get(
                "divergencia_valor_regra_llm"
            )
            or item.get(
                "necessita_revisao_humana_llm"
            )
        )
    ]

    salvar_csv(
        DIVERGENCIAS_CSV,
        divergencias
    )


# ============================================================
# PROGRESSO
# ============================================================

def salvar_progresso(
    total,
    resultados
):

    itens_ok = [
        item
        for item in resultados.values()
        if item.get(
            "_status"
        )
        == "OK"
    ]

    processados = len(
        itens_ok
    )

    inteiro_teor = sum(
        1
        for item in itens_ok
        if item.get(
            "inteiro_teor_obtido"
        )
    )

    prompt_tokens = sum(
        int(
            item.get(
                "prompt_tokens",
                0
            )
            or 0
        )
        for item in itens_ok
    )

    completion_tokens = sum(
        int(
            item.get(
                "completion_tokens",
                0
            )
            or 0
        )
        for item in itens_ok
    )

    total_tokens = sum(
        int(
            item.get(
                "total_tokens",
                0
            )
            or 0
        )
        for item in itens_ok
    )

    evidencias_removidas = sum(
        int(
            item.get(
                "qtd_evidencias_removidas",
                0
            )
            or 0
        )
        for item in itens_ok
    )

    fontes_corrigidas = sum(
        int(
            item.get(
                "qtd_fontes_corrigidas",
                0
            )
            or 0
        )
        for item in itens_ok
    )

    objeto = {

        "total_esperado": total,

        "processados_ok": (
            processados
        ),

        "restantes": max(
            0,
            total - processados
        ),

        "inteiro_teor_obtido": (
            inteiro_teor
        ),

        "evidencias_fundamentos_removidas": (
            evidencias_removidas
        ),

        "fontes_evidencias_corrigidas": (
            fontes_corrigidas
        ),

        "prompt_tokens": (
            prompt_tokens
        ),

        "completion_tokens": (
            completion_tokens
        ),

        "total_tokens": (
            total_tokens
        ),

        "atualizado_em_utc": (
            agora_iso()
        )
    }

    PROGRESSO_JSON.write_text(
        json.dumps(
            objeto,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )


# ============================================================
# RESUMO
# ============================================================

def gerar_resumo(
    registros,
    resultados,
    inicio_execucao
):

    itens = [
        item
        for item in resultados.values()
        if item.get(
            "_status"
        )
        == "OK"
    ]

    por_tribunal = {}

    for item in itens:

        tribunal = item.get(
            "tribunal",
            ""
        )

        por_tribunal.setdefault(
            tribunal,
            {
                "total": 0,
                "inteiro_teor": 0,
                "revisao_humana": 0,
                "evidencias_removidas": 0,
                "fontes_corrigidas": 0
            }
        )

        por_tribunal[
            tribunal
        ][
            "total"
        ] += 1

        if item.get(
            "inteiro_teor_obtido"
        ):

            por_tribunal[
                tribunal
            ][
                "inteiro_teor"
            ] += 1

        if item.get(
            "necessita_revisao_humana_llm"
        ):

            por_tribunal[
                tribunal
            ][
                "revisao_humana"
            ] += 1

        por_tribunal[
            tribunal
        ][
            "evidencias_removidas"
        ] += int(
            item.get(
                "qtd_evidencias_removidas",
                0
            )
            or 0
        )

        por_tribunal[
            tribunal
        ][
            "fontes_corrigidas"
        ] += int(
            item.get(
                "qtd_fontes_corrigidas",
                0
            )
            or 0
        )

    resultados_llm = {}

    for item in itens:

        resultado = item.get(
            "resultado_llm",
            ""
        )

        resultados_llm[
            resultado
        ] = (
            resultados_llm.get(
                resultado,
                0
            )
            + 1
        )

    prompt_tokens = sum(
        int(
            item.get(
                "prompt_tokens",
                0
            )
            or 0
        )
        for item in itens
    )

    completion_tokens = sum(
        int(
            item.get(
                "completion_tokens",
                0
            )
            or 0
        )
        for item in itens
    )

    total_tokens = sum(
        int(
            item.get(
                "total_tokens",
                0
            )
            or 0
        )
        for item in itens
    )

    inteiro_teor = sum(
        1
        for item in itens
        if item.get(
            "inteiro_teor_obtido"
        )
    )

    revisao = sum(
        1
        for item in itens
        if item.get(
            "necessita_revisao_humana_llm"
        )
    )

    diverg_resultado = sum(
        1
        for item in itens
        if item.get(
            "divergencia_resultado_regra_llm"
        )
    )

    diverg_valor = sum(
        1
        for item in itens
        if item.get(
            "divergencia_valor_regra_llm"
        )
    )

    evidencias_removidas = sum(
        int(
            item.get(
                "qtd_evidencias_removidas",
                0
            )
            or 0
        )
        for item in itens
    )

    fontes_corrigidas = sum(
        int(
            item.get(
                "qtd_fontes_corrigidas",
                0
            )
            or 0
        )
        for item in itens
    )

    resumo = {

        "modelo": MODEL,

        "validacao_evidencia_versao": (
            VALIDACAO_EVIDENCIA_VERSAO
        ),

        "total_csv": len(
            registros
        ),

        "processados_ok": len(
            itens
        ),

        "inteiro_teor_obtido": (
            inteiro_teor
        ),

        "somente_ementa": (
            len(
                itens
            )
            - inteiro_teor
        ),

        "necessita_revisao_humana": (
            revisao
        ),

        "evidencias_fundamentos_removidas": (
            evidencias_removidas
        ),

        "fontes_evidencias_corrigidas": (
            fontes_corrigidas
        ),

        "divergencias_resultado_regra_llm": (
            diverg_resultado
        ),

        "divergencias_valor_regra_llm": (
            diverg_valor
        ),

        "resultados_llm": (
            resultados_llm
        ),

        "por_tribunal": (
            por_tribunal
        ),

        "prompt_tokens": (
            prompt_tokens
        ),

        "completion_tokens": (
            completion_tokens
        ),

        "total_tokens": (
            total_tokens
        ),

        "tempo_execucao_segundos": round(
            time.time()
            - inicio_execucao,
            2
        ),

        "gerado_em_utc": (
            agora_iso()
        )
    }

    RESUMO_JSON.write_text(
        json.dumps(
            resumo,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    return resumo


# ============================================================
# MAIN
# ============================================================

def main():

    inicio_execucao = time.time()

    print(
        "=" * 100
    )

    print(
        "JUS-EXPERTIA - DEEPSEEK 677"
    )

    print(
        "EMENTA + INTEIRO TEOR TJPA/TJSE"
    )

    print(
        "VALIDAÇÃO DETERMINÍSTICA DE EVIDÊNCIAS"
    )

    print(
        "=" * 100
    )

    print(
        "INPUT =",
        INPUT_CSV
    )

    print(
        "OUTPUT_DIR =",
        OUTPUT_DIR
    )

    print(
        "MODEL =",
        MODEL
    )

    print(
        "VALIDACAO_EVIDENCIA =",
        VALIDACAO_EVIDENCIA_VERSAO
    )

    print()

    registros = ler_csv()

    print(
        "TOTAL_CSV =",
        len(
            registros
        )
    )

    ids = [
        limpar_str(
            registro.get(
                "id_llm"
            )
        )
        for registro in registros
    ]

    ids_validos = [
        item
        for item in ids
        if item
    ]

    print(
        "IDS_UNICOS =",
        len(
            set(
                ids_validos
            )
        )
    )

    if len(
        registros
    ) != 677:

        print()
        print(
            "ATENÇÃO: eram esperados exatamente 677 registros."
        )

        resposta = input(
            "Continuar mesmo assim? [s/N]: "
        ).strip().lower()

        if resposta not in {
            "s",
            "sim"
        }:

            return

    if len(
        set(
            ids_validos
        )
    ) != len(
        registros
    ):

        raise RuntimeError(
            "Há id_llm ausente ou duplicado."
        )

    api_key = obter_api_key()

    checkpoint = carregar_checkpoint()

    resultados = dict(
        checkpoint
    )

    print()

    print(
        "CHECKPOINT_OK_EXISTENTE =",
        len(
            checkpoint
        )
    )

    print(
        "RESTANTES =",
        len(
            registros
        )
        - len(
            checkpoint
        )
    )

    if (
        len(
            checkpoint
        )
        == 0
        and CHECKPOINT_JSONL.exists()
    ):

        print()

        print(
            "OBSERVAÇÃO:"
        )

        print(
            "Há checkpoint de versão anterior, "
            "mas os registros serão reprocessados "
            "para aplicar a nova auditoria de evidências."
        )

    print()

    total = len(
        registros
    )

    novos_desde_consolidacao = 0

    for indice, registro in enumerate(
        registros,
        start=1
    ):

        id_llm = limpar_str(
            registro.get(
                "id_llm"
            )
        )

        tribunal = limpar_str(
            registro.get(
                "tribunal"
            )
        )

        processo = limpar_str(
            registro.get(
                "processo"
            )
        )

        if id_llm in resultados:

            print(
                f"[{indice:03d}/{total}] "
                f"{tribunal} | "
                f"{processo} | CHECKPOINT OK"
            )

            continue

        print()

        print(
            "-" * 100
        )

        print(
            f"[{indice:03d}/{total}] "
            f"{tribunal} | "
            f"{processo}"
        )

        print(
            "Coletando material..."
        )

        try:

            coleta = coletar_inteiro_teor(
                registro
            )

        except Exception as erro:

            coleta = {

                "obtido": False,

                "texto": "",

                "metodo": "ERRO_COLETA",

                "http_status": None,

                "campo": None,

                "erro": repr(
                    erro
                )
            }

        if coleta.get(
            "obtido"
        ):

            print(
                "  INTEIRO_TEOR = SIM"
            )

            print(
                "  MÉTODO =",
                coleta.get(
                    "metodo"
                )
            )

            print(
                "  CARACTERES =",
                len(
                    coleta.get(
                        "texto",
                        ""
                    )
                )
            )

        else:

            print(
                "  INTEIRO_TEOR = NÃO"
            )

            print(
                "  FALLBACK = EMENTA"
            )

            if coleta.get(
                "erro"
            ):

                print(
                    "  MOTIVO =",
                    coleta.get(
                        "erro"
                    )
                )

        (
            user_prompt,
            fonte_analise,
            chars_ementa,
            chars_inteiro,
            ementa_enviada,
            inteiro_enviado
        ) = construir_prompt(
            registro,
            coleta
        )

        print(
            "  FONTE_LLM =",
            fonte_analise
        )

        print(
            "  EMENTA_CHARS =",
            chars_ementa
        )

        print(
            "  INTEIRO_ENVIADO_CHARS =",
            chars_inteiro
        )

        print(
            "Chamando DeepSeek..."
        )

        try:

            resposta = chamar_deepseek(
                api_key,
                user_prompt
            )

        except ErroAutenticacaoTogether as erro:

            print()

            print(
                "=" * 100
            )

            print(
                "ERRO DE AUTENTICAÇÃO TOGETHER AI"
            )

            print(
                "=" * 100
            )

            print(
                str(
                    erro
                )
            )

            print()

            print(
                "O processamento foi interrompido."
            )

            return

        if not resposta.get(
            "ok"
        ):

            erro_obj = {

                "_status": "ERRO",

                "id_llm": (
                    id_llm
                ),

                "tribunal": (
                    tribunal
                ),

                "processo": (
                    processo
                ),

                "erro": resposta.get(
                    "erro"
                ),

                "processado_em_utc": (
                    agora_iso()
                )
            }

            append_jsonl(
                ERROS_JSONL,
                erro_obj
            )

            print(
                "  RESULTADO = ERRO"
            )

            print(
                "  ERRO =",
                resposta.get(
                    "erro"
                )
            )

            continue

        item = criar_item_resultado(
            registro,
            coleta,
            fonte_analise,
            chars_ementa,
            chars_inteiro,
            ementa_enviada,
            inteiro_enviado,
            resposta
        )

        resultados[
            id_llm
        ] = item

        append_jsonl(
            CHECKPOINT_JSONL,
            item
        )

        raw_obj = {

            "id_llm": (
                id_llm
            ),

            "tribunal": (
                tribunal
            ),

            "processo": (
                processo
            ),

            "modelo": (
                MODEL
            ),

            "raw": resposta.get(
                "raw"
            ),

            "processado_em_utc": (
                agora_iso()
            )
        }

        append_jsonl(
            RAW_JSONL,
            raw_obj
        )

        print(
            "  RESULTADO_LLM =",
            item.get(
                "resultado_llm"
            )
        )

        print(
            "  VALOR_FINAL_LLM =",
            item.get(
                "valor_final_dano_moral_llm"
            )
        )

        print(
            "  CONFIANÇA =",
            item.get(
                "confianca_llm"
            )
        )

        print(
            "  REVISÃO_HUMANA =",
            item.get(
                "necessita_revisao_humana_llm"
            )
        )

        print(
            "  FUNDAMENTOS_DECISAO =",
            len(
                item.get(
                    "fundamentos_decisao_llm"
                )
                or []
            )
        )

        print(
            "  FUNDAMENTOS_QUANTUM =",
            len(
                item.get(
                    "fundamentos_quantum_llm"
                )
                or []
            )
        )

        print(
            "  EVIDENCIAS_REMOVIDAS =",
            item.get(
                "qtd_evidencias_removidas"
            )
        )

        print(
            "  FONTES_CORRIGIDAS =",
            item.get(
                "qtd_fontes_corrigidas"
            )
        )

        print(
            "  TOKENS =",
            item.get(
                "total_tokens"
            )
        )

        print(
            "  TEMPO_LLM =",
            item.get(
                "tempo_llm_segundos"
            ),
            "s"
        )

        novos_desde_consolidacao += 1

        salvar_progresso(
            total,
            resultados
        )

        if (
            novos_desde_consolidacao
            >= SALVAR_CONSOLIDADO_A_CADA
        ):

            print(
                "  Salvando CSV consolidado..."
            )

            consolidar(
                resultados
            )

            novos_desde_consolidacao = 0

        time.sleep(
            PAUSA_ENTRE_LLM
        )

    print()

    print(
        "=" * 100
    )

    print(
        "CONSOLIDANDO RESULTADOS"
    )

    print(
        "=" * 100
    )

    consolidar(
        resultados
    )

    salvar_progresso(
        total,
        resultados
    )

    resumo = gerar_resumo(
        registros,
        resultados,
        inicio_execucao
    )

    print()

    print(
        "=" * 100
    )

    print(
        "PROCESSAMENTO FINALIZADO"
    )

    print(
        "=" * 100
    )

    print(
        "PROCESSADOS_OK =",
        resumo[
            "processados_ok"
        ]
    )

    print(
        "INTEIRO_TEOR_OBTIDO =",
        resumo[
            "inteiro_teor_obtido"
        ]
    )

    print(
        "SOMENTE_EMENTA =",
        resumo[
            "somente_ementa"
        ]
    )

    print(
        "REVISÃO_HUMANA =",
        resumo[
            "necessita_revisao_humana"
        ]
    )

    print(
        "EVIDENCIAS_REMOVIDAS =",
        resumo[
            "evidencias_fundamentos_removidas"
        ]
    )

    print(
        "FONTES_CORRIGIDAS =",
        resumo[
            "fontes_evidencias_corrigidas"
        ]
    )

    print(
        "DIVERGÊNCIAS_RESULTADO =",
        resumo[
            "divergencias_resultado_regra_llm"
        ]
    )

    print(
        "DIVERGÊNCIAS_VALOR =",
        resumo[
            "divergencias_valor_regra_llm"
        ]
    )

    print(
        "PROMPT_TOKENS =",
        resumo[
            "prompt_tokens"
        ]
    )

    print(
        "COMPLETION_TOKENS =",
        resumo[
            "completion_tokens"
        ]
    )

    print(
        "TOTAL_TOKENS =",
        resumo[
            "total_tokens"
        ]
    )

    print()

    print(
        "RESULTADOS =",
        RESULTADOS_CSV
    )

    print(
        "DIVERGÊNCIAS =",
        DIVERGENCIAS_CSV
    )

    print(
        "CHECKPOINT =",
        CHECKPOINT_JSONL
    )

    print(
        "RESUMO =",
        RESUMO_JSON
    )

    print()

    print(
        "=== POR TRIBUNAL ==="
    )

    for tribunal, dados in sorted(
        resumo[
            "por_tribunal"
        ].items()
    ):

        print(
            tribunal,
            "| total =",
            dados[
                "total"
            ],
            "| inteiro_teor =",
            dados[
                "inteiro_teor"
            ],
            "| revisao =",
            dados[
                "revisao_humana"
            ],
            "| evidencias_removidas =",
            dados[
                "evidencias_removidas"
            ],
            "| fontes_corrigidas =",
            dados[
                "fontes_corrigidas"
            ]
        )


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print()

        print(
            "Processamento interrompido pelo usuário."
        )

        print(
            "Os casos concluídos nesta versão permanecem "
            "salvos no checkpoint."
        )

        print(
            "Na próxima execução, somente os casos "
            "ainda não concluídos serão processados."
        )

    except Exception as erro:

        print()

        print(
            "ERRO FATAL =",
            repr(
                erro
            )
        )

        raise
