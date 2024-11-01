# neutron bulk sender

This script will send USDC to a list of Neutron addresses.

Usage:

1. Install dependencies:

```
npm install
```

2. Create a CSV file with the following columns and call it `refunds.csv`:
- address
- amount

amount is the amount of USDC to send to the address
15000000 = 15 USDC

3. Create a `.env` file with the following variable being your Neutron wallet mnemonic:
- MNEMONIC

4. Run the script:

```
node neutron-bulk-send.js
```

The script will output a summary of the refund process.

The CSV will be updated with the tx hashes of the refunds.
