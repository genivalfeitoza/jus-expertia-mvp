import sqlite3
from pathlib import Path
import time
import sys

BASE = Path(r"C:\Users\elson\Downloads\Projeto MVP")
ORIGEM = BASE / "jurisprudencia_target_oficial.sqlite3"

TRIBUNAIS = {
    "TJSE": 393078,
    "TJPA": 145465,
    "TJAM": 101746,
    "TJPE": 583844,
}

def format_bytes(n):
    n = float(n)
    for unidade in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.2f} {unidade}"
        n /= 1024
    return f"{n:.2f} PB"

def abrir_readonly(path):
    uri = path.resolve().as_uri() + "?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    return con

def copiar_indices(origem, destino):
    indices = origem.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type='index'
          AND tbl_name='judgments'
          AND sql IS NOT NULL
        ORDER BY name
        """
    ).fetchall()

    for idx in indices:
        try:
            destino.execute(idx["sql"])
        except sqlite3.OperationalError as e:
            print(f"AVISO_INDICE|{idx['name']}|{e}")

def main():
    print("============================================================")
    print("JUS-EXPERTIA - SEPARAÇÃO DOS BANCOS POR TRIBUNAL")
    print("============================================================")
    print(f"PASTA={BASE}")
    print(f"ORIGEM={ORIGEM}")
    print()

    if not ORIGEM.exists():
        print("ERRO: arquivo principal não encontrado.")
        print(f"CAMINHO_ESPERADO={ORIGEM}")
        input("Pressione ENTER para sair...")
        sys.exit(1)

    origem = abrir_readonly(ORIGEM)

    try:
        print("Verificando integridade do banco principal...")

        integrity = origem.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        print(f"INTEGRITY_ORIGINAL={integrity}")

        if integrity != "ok":
            raise RuntimeError(
                "Banco principal não passou no integrity_check."
            )

        schema = origem.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type='table'
              AND name='judgments'
            """
        ).fetchone()

        if not schema or not schema["sql"]:
            raise RuntimeError(
                "Tabela judgments não encontrada."
            )

        schema_judgments = schema["sql"]

        info = origem.execute(
            "PRAGMA table_info(judgments)"
        ).fetchall()

        colunas = [r["name"] for r in info]

        nomes_colunas = ", ".join(
            '"' + c.replace('"', '""') + '"'
            for c in colunas
        )

        placeholders = ", ".join(
            "?" for _ in colunas
        )

        print()
        print("--- CONTAGENS NO BANCO PRINCIPAL ---")

        contagens_reais = {}

        for tribunal in TRIBUNAIS:
            total = origem.execute(
                """
                SELECT COUNT(*)
                FROM judgments
                WHERE tribunal=?
                """,
                (tribunal,)
            ).fetchone()[0]

            contagens_reais[tribunal] = total

            print(
                f"{tribunal}="
                + f"{total:,}".replace(",", ".")
            )

        print()

        for tribunal, esperado in TRIBUNAIS.items():
            real = contagens_reais[tribunal]

            if real != esperado:
                print(
                    f"AVISO_CONTAGEM|{tribunal}"
                    f"|ESPERADO={esperado}"
                    f"|REAL={real}"
                )

        resultados = []

        for tribunal in TRIBUNAIS:
            destino_path = BASE / f"{tribunal}.sqlite3"

            print()
            print("============================================================")
            print(f"CRIANDO {tribunal}")
            print(f"DESTINO={destino_path}")
            print(
                "REGISTROS="
                + f"{contagens_reais[tribunal]:,}".replace(",", ".")
            )
            print("============================================================")

            if destino_path.exists():
                print(
                    f"Arquivo {destino_path.name} já existe."
                )

                resposta = input(
                    "Deseja apagar e recriar? [S/N]: "
                ).strip().upper()

                if resposta != "S":
                    print(f"PULADO={tribunal}")
                    continue

                destino_path.unlink()

            inicio = time.time()

            destino = sqlite3.connect(destino_path)

            try:
                destino.execute("PRAGMA journal_mode=DELETE")
                destino.execute("PRAGMA synchronous=NORMAL")
                destino.execute("PRAGMA temp_store=MEMORY")

                destino.execute(schema_judgments)
                destino.commit()

                cursor = origem.execute(
                    f"""
                    SELECT {nomes_colunas}
                    FROM judgments
                    WHERE tribunal=?
                    ORDER BY id
                    """,
                    (tribunal,)
                )

                insert_sql = (
                    f"INSERT INTO judgments "
                    f"({nomes_colunas}) "
                    f"VALUES ({placeholders})"
                )

                total = contagens_reais[tribunal]
                copiados = 0
                lote = 5000

                while True:
                    rows = cursor.fetchmany(lote)

                    if not rows:
                        break

                    valores = [
                        tuple(row[c] for c in colunas)
                        for row in rows
                    ]

                    destino.executemany(
                        insert_sql,
                        valores
                    )

                    destino.commit()

                    copiados += len(rows)

                    percentual = (
                        copiados / total * 100
                        if total
                        else 100
                    )

                    print(
                        f"{tribunal}: "
                        + f"{copiados:,}".replace(",", ".")
                        + "/"
                        + f"{total:,}".replace(",", ".")
                        + f" ({percentual:.1f}%)"
                    )

                print(f"Criando índices de {tribunal}...")

                copiar_indices(origem, destino)
                destino.commit()

                print(f"Executando ANALYZE em {tribunal}...")

                destino.execute("ANALYZE")
                destino.commit()

                integrity_destino = destino.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]

                total_destino = destino.execute(
                    """
                    SELECT COUNT(*)
                    FROM judgments
                    WHERE tribunal=?
                    """,
                    (tribunal,)
                ).fetchone()[0]

                outros = destino.execute(
                    """
                    SELECT COUNT(*)
                    FROM judgments
                    WHERE tribunal<>?
                       OR tribunal IS NULL
                    """,
                    (tribunal,)
                ).fetchone()[0]

                tribunais = [
                    x[0]
                    for x in destino.execute(
                        """
                        SELECT DISTINCT tribunal
                        FROM judgments
                        ORDER BY tribunal
                        """
                    ).fetchall()
                ]

                tamanho = destino_path.stat().st_size
                tempo = time.time() - inicio

                gate = (
                    integrity_destino == "ok"
                    and total_destino == total
                    and outros == 0
                    and tribunais == [tribunal]
                )

                print()
                print(f"RESULTADO_{tribunal}")
                print(f"INTEGRITY={integrity_destino}")
                print(f"REGISTROS={total_destino}")
                print(f"OUTROS_TRIBUNAIS={outros}")
                print(
                    "TRIBUNAIS_PRESENTES="
                    + ",".join(tribunais)
                )
                print(
                    f"TAMANHO={format_bytes(tamanho)}"
                )
                print(f"TEMPO={tempo:.1f}s")
                print(
                    f"GATE={'PASS' if gate else 'FAIL'}"
                )

                resultados.append(
                    (
                        tribunal,
                        total_destino,
                        tamanho,
                        gate
                    )
                )

            finally:
                destino.close()

        print()
        print("============================================================")
        print("RESUMO FINAL")
        print("============================================================")

        global_pass = True

        for tribunal, registros, tamanho, gate in resultados:
            print(
                f"{tribunal} | "
                + f"{registros:,}".replace(",", ".")
                + f" registros | "
                + format_bytes(tamanho)
                + " | "
                + ("PASS" if gate else "FAIL")
            )

            if not gate:
                global_pass = False

        print()
        print(
            f"RESULTADO_GLOBAL="
            f"{'PASS' if global_pass and len(resultados) == 4 else 'FAIL'}"
        )

        print(f"PASTA_DESTINO={BASE}")
        print("ARQUIVO_ORIGINAL_MODIFICADO=0")
        print("SEPARACAO_CONCLUIDA=1")

        print()
        print("Arquivos esperados:")
        print(BASE / "TJSE.sqlite3")
        print(BASE / "TJPA.sqlite3")
        print(BASE / "TJAM.sqlite3")
        print(BASE / "TJPE.sqlite3")

    finally:
        origem.close()

    input("\nPressione ENTER para fechar...")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print()
        print("ERRO_FATAL=" + repr(e))
        input("\nPressione ENTER para fechar...")
        sys.exit(1)
