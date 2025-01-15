def is_latest_log(current_log, latest_log):
    return (
        current_log.blockNumber > latest_log.blockNumber
        or (
            current_log.blockNumber == latest_log.blockNumber
            and current_log.transactionIndex > latest_log.transactionIndex
        )
        or (
            current_log.blockNumber == latest_log.blockNumber
            and current_log.transactionIndex == latest_log.transactionIndex
            and current_log.logIndex > latest_log.logIndex
        )
    )
