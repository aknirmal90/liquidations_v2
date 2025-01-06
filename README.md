### Periodic Tasks

1. Initialize
   - Frequency: Manual
   - Description: Must be run anytime a new network, protocol or event is added.

2. Sync Latest Blocks
   - Frequency: Every minute
   - Description: Determines whether events should be processed by the streaming events sync task or the backfill events sync task.

3. Sync All Events
   - Frequency: Every minute
   - Description: Triggers child streaming and backfill tasks.

4. Update MaxCappedRatios Task
   - Frequency: Every 15 minutes
   - Description: Updates max cap on Aave price sources where max-cap increases over time.

5. Verify Balances Task
   - Frequency: Daily
   - Description: Verifies balances and performs updates and deletes for necessary reserves.

6. Update Metadata Cache
   - Frequency: Daily
   - Description: Keeps caches for protocol, network and assets up to date.
