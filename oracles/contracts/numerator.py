def get_numerator(asset_source: str, event=None, transaction=None) -> int:
    if event:
        return int(event.args.answer)
    elif transaction:
        return int(transaction["median_price"])
