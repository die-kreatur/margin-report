# Binance tokens reports

Source code for a [telegram channel](https://t.me/+ziGHv7M8M-wyNDg6) with trading signals and exchange data.

### Workflow
Reports are based on Binance margin data. It's requested every 5 minutes to check if there any changes. If some token borrow increased the full report is collected and sent to telegram channel.

### Features
- Report contains 24 hours spot trading volume.
- It has aggregated data of both sell and buy trading volumes for 5 minutes, 15 minutes, 1 hour and 4 hours intervals.
- Report displays funding rate with next time payment.
- There are open interest changes for 5 minutes, 15 minutes, 1 hour and 4 hours intervals.
- Long short ratio for 5 minutes, 15 minutes, 1 hour and 4 hours intervals is also displayed.
