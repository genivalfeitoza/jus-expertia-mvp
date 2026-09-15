import json
import time

import requests

import processar_deepseek_677 as p


IDS_ALVO = {
    "58e78633e0489ad166c7322bf585ce780f1554943da90f6ddc3ad6bdb6b5b5e1",
    "0229308361351aa79de07909184125cdad193f7cd6b2893acf4689eec9fd84b2",
    "079588c56a84cf2fd9a68c681d6e97c33a201439d2ec6940a530b18aed93f8fc",
    "6ea5010efb787862e6ebf7837c96ece435a55df148388837d340d0d265bbbe25",
}


LIMITES_TOKENS = [
    8000,
    12000,
    16000
]


SYSTEM_PROMPT_RESGATE = (
    p.SYSTEM_PROMPT
    + """

REGRAS ADICIONAIS PARA ESTA EXECUÇÃO DE RESGATE:

20. Seja extremamente conciso.

21. Em fundamentos_decisao_llm, retorne no máximo 6 itens,
somente os fundamentos juridicamente mais relevantes.

22. Em fundamentos_quantum_llm, retorne no máximo 6 itens.

23. Cada evidencia_literal deve ser curta, preferencialmente
com até 180 caracteres, sem perder o sentido probatório.

24. fundamento, motivo_resultado_llm, justificativa_quantum_llm,
tese_central_llm e fundamento_dominante_llm devem ser escritos
de forma objetiva e compacta.

25. fundamentos_secundarios_llm deve conter somente os pontos
indispensáveis.

26. sintese_juridica_llm deve ser sucinta, preferencialmente
em até 120 palavras.

27. Não repita a mesma fundamentação em vários campos.

28. Priorize JSON completo e válido. Nunca interrompa uma string
ou objeto JSON.
"""
)


session_resgate = requests.Session()


def limpar(valor):
    return p.limpar_str(
        valor
    )


def obter_registros_alvo():
    registros = p.ler_csv()

    encontrados = []

    for registro in registros:

        id_llm = limpar(
            registro.get(
                "id_llm"
            )
        )

        if id_llm in IDS_ALVO:
            encontrados.append(
                registro
            )

    encontrados.sort(
        key=lambda registro: (
            limpar(
                registro.get(
                    "tribunal"
                )
            ),
            limpar(
                registro.get(
                    "processo"
                )
            )
        )
    )

    return registros, encontrados


def chamar_deepseek_resgate(
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

    ultimo_erro = None

    for tentativa, max_tokens_atual in enumerate(
        LIMITES_TOKENS,
        start=1
    ):
        print(
            f"  TENTATIVA_RESGATE = {tentativa}"
        )

        print(
            f"  MAX_TOKENS = {max_tokens_atual}"
        )

        payload = {
            "model": p.MODEL,

            "temperature": 0.0,

            "max_tokens": (
                max_tokens_atual
            ),

            "messages": [
                {
                    "role": "system",
                    "content": (
                        SYSTEM_PROMPT_RESGATE
                    )
                },

                {
                    "role": "user",
                    "content": (
                        user_prompt
                    )
                }
            ],

            "response_format": {
                "type": "json_schema",

                "json_schema": {
                    "name": (
                        "jus_expertia_analise"
                    ),

                    "strict": True,

                    "schema": (
                        p.SCHEMA
                    )
                }
            }
        }

        try:
            inicio = time.time()

            resposta = session_resgate.post(
                p.TOGETHER_URL,
                headers=headers,
                json=payload,
                timeout=240
            )

            tempo = (
                time.time()
                - inicio
            )

            if resposta.status_code == 401:
                raise p.ErroAutenticacaoTogether(
                    "A Together AI recusou "
                    "a API Key com HTTP 401."
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
                        f"HTTP "
                        f"{resposta.status_code}: "
                        f"{resposta.text[:2000]}"
                    )
                }

            if resposta.status_code != 200:
                ultimo_erro = (
                    f"HTTP "
                    f"{resposta.status_code}: "
                    f"{resposta.text[:1500]}"
                )

                print(
                    "  ERRO_HTTP =",
                    ultimo_erro
                )

                if tentativa < len(
                    LIMITES_TOKENS
                ):
                    time.sleep(
                        5
                    )

                continue

            bruto = resposta.json()

            choices = bruto.get(
                "choices",
                []
            )

            if not choices:
                ultimo_erro = (
                    "Resposta sem choices."
                )

                print(
                    "  ERRO =",
                    ultimo_erro
                )

                continue

            choice = choices[
                0
            ]

            mensagem = choice.get(
                "message",
                {}
            )

            content = mensagem.get(
                "content"
            )

            finish_reason = choice.get(
                "finish_reason"
            )

            usage = bruto.get(
                "usage",
                {}
            )

            print(
                "  FINISH_REASON =",
                finish_reason
            )

            print(
                "  COMPLETION_TOKENS_API =",
                usage.get(
                    "completion_tokens",
                    0
                )
            )

            if content is None:
                ultimo_erro = (
                    "Resposta sem content."
                )

                print(
                    "  ERRO =",
                    ultimo_erro
                )

                continue

            try:
                if isinstance(
                    content,
                    dict
                ):
                    dados = content

                else:
                    dados = json.loads(
                        content
                    )

            except json.JSONDecodeError as erro:
                ultimo_erro = (
                    f"{repr(erro)} | "
                    f"finish_reason="
                    f"{finish_reason} | "
                    f"max_tokens="
                    f"{max_tokens_atual} | "
                    f"content_chars="
                    f"{len(str(content))}"
                )

                print(
                    "  JSON_INVALIDO =",
                    repr(
                        erro
                    )
                )

                print(
                    "  CONTENT_CHARS =",
                    len(
                        str(
                            content
                        )
                    )
                )

                if tentativa < len(
                    LIMITES_TOKENS
                ):
                    print(
                        "  Nova tentativa com "
                        "limite maior..."
                    )

                    time.sleep(
                        3
                    )

                continue

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

        except p.ErroAutenticacaoTogether:
            raise

        except Exception as erro:
            ultimo_erro = repr(
                erro
            )

            print(
                "  ERRO_TENTATIVA =",
                ultimo_erro
            )

            if tentativa < len(
                LIMITES_TOKENS
            ):
                time.sleep(
                    5
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


def processar_um(
    registro,
    api_key,
    resultados
):
    id_llm = limpar(
        registro.get(
            "id_llm"
        )
    )

    tribunal = limpar(
        registro.get(
            "tribunal"
        )
    )

    processo = limpar(
        registro.get(
            "processo"
        )
    )

    print()
    print(
        "=" * 100
    )

    print(
        f"RESGATE | {tribunal} | "
        f"{processo}"
    )

    print(
        "=" * 100
    )

    if id_llm in resultados:
        print(
            "STATUS = JÁ CONCLUÍDO"
        )

        return True

    print(
        "Coletando material..."
    )

    try:
        coleta = (
            p.coletar_inteiro_teor(
                registro
            )
        )

    except Exception as erro:
        print(
            "ERRO_COLETA =",
            repr(
                erro
            )
        )

        return False

    print(
        "INTEIRO_TEOR =",
        coleta.get(
            "obtido"
        )
    )

    print(
        "METODO =",
        coleta.get(
            "metodo"
        )
    )

    print(
        "HTTP_STATUS =",
        coleta.get(
            "http_status"
        )
    )

    print(
        "CAMPO =",
        coleta.get(
            "campo"
        )
    )

    print(
        "CARACTERES_ORIGINAIS =",
        len(
            coleta.get(
                "texto",
                ""
            )
        )
    )

    (
        user_prompt,
        fonte_analise,
        chars_ementa,
        chars_inteiro,
        ementa_enviada,
        inteiro_enviado
    ) = p.construir_prompt(
        registro,
        coleta
    )

    print(
        "FONTE_ANALISE =",
        fonte_analise
    )

    print(
        "EMENTA_CHARS =",
        chars_ementa
    )

    print(
        "INTEIRO_ENVIADO_CHARS =",
        chars_inteiro
    )

    print(
        "Chamando DeepSeek em modo resgate..."
    )

    resposta = (
        chamar_deepseek_resgate(
            api_key,
            user_prompt
        )
    )

    if not resposta.get(
        "ok"
    ):
        erro_obj = {
            "_status": "ERRO",

            "id_llm": id_llm,

            "tribunal": tribunal,

            "processo": processo,

            "erro": resposta.get(
                "erro"
            ),

            "modo": (
                "RESGATE_4_FALTANTES"
            ),

            "processado_em_utc": (
                p.agora_iso()
            )
        }

        p.append_jsonl(
            p.ERROS_JSONL,
            erro_obj
        )

        print()
        print(
            "STATUS_FINAL = FALHA"
        )

        print(
            "ERRO =",
            resposta.get(
                "erro"
            )
        )

        return False

    item = (
        p.criar_item_resultado(
            registro,
            coleta,
            fonte_analise,
            chars_ementa,
            chars_inteiro,
            ementa_enviada,
            inteiro_enviado,
            resposta
        )
    )

    item[
        "modo_processamento"
    ] = (
        "RESGATE_4_FALTANTES"
    )

    resultados[
        id_llm
    ] = item

    p.append_jsonl(
        p.CHECKPOINT_JSONL,
        item
    )

    raw_obj = {
        "id_llm": id_llm,

        "tribunal": tribunal,

        "processo": processo,

        "modelo": p.MODEL,

        "modo": (
            "RESGATE_4_FALTANTES"
        ),

        "raw": resposta.get(
            "raw"
        ),

        "processado_em_utc": (
            p.agora_iso()
        )
    }

    p.append_jsonl(
        p.RAW_JSONL,
        raw_obj
    )

    print()
    print(
        "RESULTADO_LLM =",
        item.get(
            "resultado_llm"
        )
    )

    print(
        "VALOR_FINAL_LLM =",
        item.get(
            "valor_final_dano_moral_llm"
        )
    )

    print(
        "CONFIANCA =",
        item.get(
            "confianca_llm"
        )
    )

    print(
        "REVISAO_HUMANA =",
        item.get(
            "necessita_revisao_humana_llm"
        )
    )

    print(
        "FUNDAMENTOS_DECISAO =",
        len(
            item.get(
                "fundamentos_decisao_llm"
            )
            or []
        )
    )

    print(
        "FUNDAMENTOS_QUANTUM =",
        len(
            item.get(
                "fundamentos_quantum_llm"
            )
            or []
        )
    )

    print(
        "EVIDENCIAS_REMOVIDAS =",
        item.get(
            "qtd_evidencias_removidas"
        )
    )

    print(
        "FONTES_CORRIGIDAS =",
        item.get(
            "qtd_fontes_corrigidas"
        )
    )

    print(
        "TOKENS =",
        item.get(
            "total_tokens"
        )
    )

    print(
        "TEMPO_LLM =",
        item.get(
            "tempo_llm_segundos"
        ),
        "s"
    )

    print(
        "STATUS_FINAL = SUCESSO"
    )

    p.consolidar(
        resultados
    )

    p.salvar_progresso(
        677,
        resultados
    )

    return True


def main():
    print(
        "=" * 100
    )

    print(
        "JUS-EXPERTIA - RESGATE DOS 4 CASOS FALTANTES"
    )

    print(
        "=" * 100
    )

    todos_registros, alvos = (
        obter_registros_alvo()
    )

    resultados = (
        p.carregar_checkpoint()
    )

    print(
        "TOTAL_BASE =",
        len(
            todos_registros
        )
    )

    print(
        "CHECKPOINT_OK_EXISTENTE =",
        len(
            resultados
        )
    )

    print(
        "CASOS_ALVO_LOCALIZADOS =",
        len(
            alvos
        )
    )

    faltantes_reais = [
        registro
        for registro in alvos
        if limpar(
            registro.get(
                "id_llm"
            )
        )
        not in resultados
    ]

    print(
        "CASOS_A_REPROCESSAR =",
        len(
            faltantes_reais
        )
    )

    if len(
        alvos
    ) != 4:
        raise RuntimeError(
            "Não foram localizados exatamente "
            "os 4 IDs esperados no CSV."
        )

    if not faltantes_reais:
        print()
        print(
            "Nenhum dos 4 casos está faltando."
        )

        p.consolidar(
            resultados
        )

        return

    api_key = (
        p.obter_api_key()
    )

    sucessos = 0
    falhas = 0

    for registro in faltantes_reais:
        try:
            ok = processar_um(
                registro,
                api_key,
                resultados
            )

        except p.ErroAutenticacaoTogether as erro:
            print()
            print(
                "ERRO DE AUTENTICAÇÃO =",
                str(
                    erro
                )
            )

            return

        if ok:
            sucessos += 1

        else:
            falhas += 1

    print()
    print(
        "=" * 100
    )

    print(
        "CONSOLIDANDO RESULTADO FINAL"
    )

    print(
        "=" * 100
    )

    p.consolidar(
        resultados
    )

    p.salvar_progresso(
        677,
        resultados
    )

    resumo = p.gerar_resumo(
        todos_registros,
        resultados,
        time.time()
    )

    print()
    print(
        "=" * 100
    )

    print(
        "RESGATE FINALIZADO"
    )

    print(
        "=" * 100
    )

    print(
        "SUCESSOS_NESTA_EXECUCAO =",
        sucessos
    )

    print(
        "FALHAS_NESTA_EXECUCAO =",
        falhas
    )

    print(
        "PROCESSADOS_OK_TOTAL =",
        len(
            resultados
        )
    )

    print(
        "INTEIRO_TEOR_OBTIDO =",
        resumo.get(
            "inteiro_teor_obtido"
        )
    )

    print(
        "SOMENTE_EMENTA =",
        resumo.get(
            "somente_ementa"
        )
    )

    print(
        "REVISAO_HUMANA =",
        resumo.get(
            "necessita_revisao_humana"
        )
    )

    print(
        "EVIDENCIAS_REMOVIDAS =",
        resumo.get(
            "evidencias_fundamentos_removidas"
        )
    )

    print(
        "FONTES_CORRIGIDAS =",
        resumo.get(
            "fontes_evidencias_corrigidas"
        )
    )

    print()
    print(
        "=== DISTRIBUIÇÃO FINAL ==="
    )

    for tribunal, dados in sorted(
        resumo.get(
            "por_tribunal",
            {}
        ).items()
    ):
        print(
            tribunal,
            "| total =",
            dados.get(
                "total"
            ),
            "| inteiro_teor =",
            dados.get(
                "inteiro_teor"
            ),
            "| revisao =",
            dados.get(
                "revisao_humana"
            )
        )

    print()

    if len(
        resultados
    ) == 677:
        print(
            "GATE_677 = PASS"
        )

        print(
            "Todos os 677 julgados possuem "
            "resultado LLM válido no checkpoint."
        )

    else:
        print(
            "GATE_677 = FAIL"
        )

        print(
            "Ainda faltam",
            677 - len(
                resultados
            ),
            "casos."
        )


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print()
        print(
            "Execução interrompida."
        )
        print(
            "Os casos já concluídos continuam "
            "salvos no checkpoint."
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
