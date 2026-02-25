import argparse

from db import assign_composto_to_batch


def main():
    parser = argparse.ArgumentParser(description="Vincula composto a um batch do FieldLogger")
    parser.add_argument("batch_id", type=int, help="Batch ID a ser atualizado")
    parser.add_argument("--codprod", type=int, default=None, help="Codigo do composto no catalogo")
    parser.add_argument("--descricao", type=str, default=None, help="Descricao do composto (caso nao informe codprod)")
    parser.add_argument("--lote", type=str, default=None, help="Lote a registrar no batch")
    parser.add_argument("--observacoes", type=str, default=None, help="Observacoes do batch")
    args = parser.parse_args()

    result = assign_composto_to_batch(
        batch_id=args.batch_id,
        codprod=args.codprod,
        descricao=args.descricao,
        lote=args.lote,
        observacoes=args.observacoes,
    )

    print("Resultado:")
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
