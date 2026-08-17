"""Compare the production libpostal parser with optional alternatives.

Run libpostal with::

    LIBPOSTAL_PREFIX="$(brew --prefix libpostal)"
    CPPFLAGS="-I${LIBPOSTAL_PREFIX}/include" \
    LDFLAGS="-L${LIBPOSTAL_PREFIX}/lib" \
    uv run --frozen python \
      corpscout/services/dagster_v3/scripts/address_parser_probe.py --parser libpostal

Run Deepparse in its supported Python version with::

    uv run --no-project --python 3.13 --with click --with deepparse \
      python corpscout/services/dagster_v3/scripts/address_parser_probe.py \
      --parser deepparse
"""

import json
from collections.abc import Callable, Sequence
from typing import Any

import click

DEFAULT_ADDRESSES = (
    "STADSGÅRDEN 6 (+1 KOMMUNIKATIONSBYRÅ AB), 11645 STOCKHOLM",
    (
        "A HOUSE KATARINAHUSET, STADSGÅRDEN 6 "
        "(+1 KOMMUNIKATIONSBYRÅ AB), 11645 STOCKHOLM"
    ),
    "STADSGÅRDEN 6, 11645 STOCKHOLM",
)


def parse_with_libpostal(addresses: Sequence[str]) -> list[dict[str, Any]]:
    from postal.parser import parse_address

    return [
        {
            "address": address,
            "components": [
                {"value": value, "label": label}
                for value, label in parse_address(address)
            ],
        }
        for address in addresses
    ]


def parse_with_deepparse(addresses: Sequence[str]) -> list[dict[str, Any]]:
    from deepparse.parser import AddressParser

    parser = AddressParser(model_type="bpemb", device="cpu", verbose=False)
    return [
        {
            "address": parsed.raw_address,
            "components": parsed.to_dict(),
            "tagged_tokens": [
                {"value": value, "label": label}
                for value, label in parsed.address_parsed_components
            ],
        }
        for parsed in parser(list(addresses))
    ]


PARSERS: dict[str, Callable[[Sequence[str]], list[dict[str, Any]]]] = {
    "libpostal": parse_with_libpostal,
    "deepparse": parse_with_deepparse,
}


@click.command()
@click.option(
    "parser_name",
    "--parser",
    type=click.Choice(tuple(PARSERS), case_sensitive=False),
    default="libpostal",
    show_default=True,
)
@click.argument("addresses", nargs=-1)
def main(parser_name: str, addresses: tuple[str, ...]) -> None:
    """Print parser output for ADDRESSES or the built-in SCB examples."""
    selected_addresses = addresses or DEFAULT_ADDRESSES
    results = PARSERS[parser_name](selected_addresses)
    click.echo(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
