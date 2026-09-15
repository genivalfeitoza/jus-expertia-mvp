import csv
import json
from pathlib import Path
from collections import Counter


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

RESULTADOS_CSV = (
    OUTPUT_DIR
    / "resultados_deepseek_677.csv"
)

ERROS_JSONL = (
    OUTPUT_DIR
    / "erros_deepseek_677.jsonl"
)

CHECKPOINT_JSONL = (
    OUTPUT_DIR
    / "checkpoint_deepseek_677.jsonl"
)


def limpar(valor):
    if valor is None:
        return ""

    return str(
        valor
    ).strip()


def ler_csv(caminho):
    with caminho.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as arquivo:

        return list(
            csv.DictReader(
                arquivo
            )
        )


def carregar_erros():
    erros = []

    if not ERROS_JSONL.exists():
        return erros

    with ERROS_JSONL.open(
        "r",
        encoding="utf-8"
    ) as arquivo:

        for linha in arquivo:

            linha = linha.strip()

            if not linha:
                continue

            try:
                erros.append(
                    json.loads(
                        linha
                    )
                )

            except Exception:
                pass

    return erros


def carregar_checkpoint_ok():
    ids_ok = set()

    if not CHECKPOINT_JSONL.exists():
        return ids_ok

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

            except Exception:
                continue

            if (
                objeto.get("_status") == "OK"
                and
                objeto.get(
                    "validacao_evidencia_versao"
                )
                == "EVIDENCIA_LITERAL_V1"
            ):

                id_llm = limpar(
                    objeto.get(
                        "id_llm"
                    )
                )

                if id_llm:
                    ids_ok.add(
                        id_llm
                    )

    return ids_ok


def main():
    print(
        "=" * 100
    )

    print(
        "JUS-EXPERTIA - DIAGNÓSTICO DOS CASOS FALTANTES"
    )

    print(
        "=" * 100
    )

    entrada = ler_csv(
        INPUT_CSV
    )

    resultados = ler_csv(
        RESULTADOS_CSV
    )

    erros = carregar_erros()

    checkpoint_ok = (
        carregar_checkpoint_ok()
    )

    ids_entrada = {
        limpar(
            registro.get(
                "id_llm"
            )
        )
        for registro in entrada
        if limpar(
            registro.get(
                "id_llm"
            )
        )
    }

    ids_resultados = {
        limpar(
            registro.get(
                "id_llm"
            )
        )
        for registro in resultados
        if limpar(
            registro.get(
                "id_llm"
            )
        )
    }

    faltantes = (
        ids_entrada
        - ids_resultados
    )

    print()
    print(
        "TOTAL_ENTRADA =",
        len(
            entrada
        )
    )

    print(
        "IDS_ENTRADA_UNICOS =",
        len(
            ids_entrada
        )
    )

    print(
        "TOTAL_RESULTADOS =",
        len(
            resultados
        )
    )

    print(
        "IDS_RESULTADOS_UNICOS =",
        len(
            ids_resultados
        )
    )

    print(
        "CHECKPOINT_OK_V1 =",
        len(
            checkpoint_ok
        )
    )

    print(
        "TOTAL_FALTANTES =",
        len(
            faltantes
        )
    )

    print()

    contagem_entrada = Counter(
        limpar(
            registro.get(
                "tribunal"
            )
        )
        for registro in entrada
    )

    contagem_resultados = Counter(
        limpar(
            registro.get(
                "tribunal"
            )
        )
        for registro in resultados
    )

    print(
        "=== DISTRIBUIÇÃO ==="
    )

    for tribunal in sorted(
        set(
            contagem_entrada
        )
        | set(
            contagem_resultados
        )
    ):

        esperado = (
            contagem_entrada[
                tribunal
            ]
        )

        processado = (
            contagem_resultados[
                tribunal
            ]
        )

        print(
            tribunal,
            "| esperado =",
            esperado,
            "| processado =",
            processado,
            "| faltam =",
            esperado - processado
        )

    print()
    print(
        "=" * 100
    )

    print(
        "CASOS FALTANTES"
    )

    print(
        "=" * 100
    )

    registros_por_id = {
        limpar(
            registro.get(
                "id_llm"
            )
        ): registro
        for registro in entrada
    }

    erros_por_id = {}

    for erro in erros:

        id_llm = limpar(
            erro.get(
                "id_llm"
            )
        )

        if not id_llm:
            continue

        erros_por_id.setdefault(
            id_llm,
            []
        ).append(
            erro
        )

    faltantes_ordenados = sorted(
        faltantes,
        key=lambda id_llm: (
            limpar(
                registros_por_id[
                    id_llm
                ].get(
                    "tribunal"
                )
            ),
            limpar(
                registros_por_id[
                    id_llm
                ].get(
                    "processo"
                )
            )
        )
    )

    for numero, id_llm in enumerate(
        faltantes_ordenados,
        start=1
    ):

        registro = registros_por_id[
            id_llm
        ]

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

        url = limpar(
            registro.get(
                "url_oficial"
            )
        )

        print()
        print(
            "-" * 100
        )

        print(
            f"FALTANTE {numero}/{len(faltantes_ordenados)}"
        )

        print(
            "ID_LLM =",
            id_llm
        )

        print(
            "TRIBUNAL =",
            tribunal
        )

        print(
            "PROCESSO =",
            processo
        )

        print(
            "ID_ORIGEM =",
            limpar(
                registro.get(
                    "id_origem"
                )
            )
        )

        print(
            "ANO =",
            limpar(
                registro.get(
                    "ano"
                )
            )
        )

        print(
            "URL_OFICIAL =",
            url
        )

        print(
            "NO_CHECKPOINT_OK_V1 =",
            id_llm in checkpoint_ok
        )

        lista_erros = erros_por_id.get(
            id_llm,
            []
        )

        print(
            "ERROS_REGISTRADOS =",
            len(
                lista_erros
            )
        )

        if not lista_erros:

            print(
                "ERRO_DETALHADO = "
                "nenhum registro localizado em erros_deepseek_677.jsonl"
            )

        else:

            for indice_erro, erro in enumerate(
                lista_erros,
                start=1
            ):

                print()
                print(
                    f"ERRO_{indice_erro} ="
                )

                print(
                    limpar(
                        erro.get(
                            "erro"
                        )
                    )
                )

                print(
                    "DATA_ERRO =",
                    limpar(
                        erro.get(
                            "processado_em_utc"
                        )
                    )
                )

    print()
    print(
        "=" * 100
    )

    if len(
        faltantes
    ) == 4:

        print(
            "DIAGNOSTICO_ESTRUTURAL = OK"
        )

        print(
            "Foram encontrados exatamente os 4 casos "
            "que não chegaram ao CSV final."
        )

    elif not faltantes:

        print(
            "DIAGNOSTICO_ESTRUTURAL = "
            "NENHUM CASO FALTANTE"
        )

    else:

        print(
            "DIAGNOSTICO_ESTRUTURAL = ATENÇÃO"
        )

        print(
            "A quantidade de faltantes não é 4."
        )

    print(
        "=" * 100
    )


if __name__ == "__main__":
    main()
