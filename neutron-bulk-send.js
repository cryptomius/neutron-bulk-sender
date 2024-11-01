require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parse/sync');
const { DirectSecp256k1HdWallet } = require('@cosmjs/proto-signing');
const { SigningStargateClient, GasPrice } = require('@cosmjs/stargate');
const fetch = require('node-fetch');

const USDC_DENOM = 'ibc/B559A80D62249C8AA07A380E2A2BEA6E5CA9A6F079C912C3A9E9B494105E4F81';
const RPC_ENDPOINT = process.env.RPC_ENDPOINT || 'https://rpc-neutron.nodeist.net';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 25;
const GAS_PRICE = process.env.GAS_PRICE || '0.025';

const MAX_RETRIES = 3;
const BASE_GAS = 100000;
const GAS_PER_TX = 20000;

const DUST_THRESHOLD = BigInt(1000); // Allow for dust difference of 0.001 USDC

if (!process.env.MNEMONIC) {
    console.error('ERROR: MNEMONIC not found in environment variables');
    process.exit(1);
}

async function loadWallet(mnemonic) {
    return await DirectSecp256k1HdWallet.fromMnemonic(mnemonic, {
        prefix: 'neutron'
    });
}

function validateAmount(amount) {
    // Check if it's a valid number without decimals
    if (!/^\d+$/.test(amount)) {
        throw new Error(`Invalid amount format: ${amount}. Must be a whole number without decimals`);
    }
    return true;
}

function validateAddress(address) {
    if (!address.startsWith('neutron')) {
        throw new Error(`Invalid address format: ${address}. Must start with 'neutron'`);
    }
    return true;
}

function readRefundData(filepath) {
    try {
        const fileContent = fs.readFileSync(filepath);
        const records = csv.parse(fileContent, {
            columns: true,
            skip_empty_lines: true
        });
        
        if (records.length === 0) {
            throw new Error('No records found in CSV file');
        }
        
        return records
            .filter(record => !record.SUCCESS)
            .map((record, index) => {
                try {
                    validateAmount(record.amount);
                    validateAddress(record.address);
                    
                    return {
                        address: record.address,
                        amount: record.amount,
                        rowIndex: record.rowIndex || index
                    };
                } catch (error) {
                    throw new Error(`Error in row ${index + 1}: ${error.message}`);
                }
            });
    } catch (error) {
        console.error('Error reading CSV file:', error);
        throw error;
    }
}

function updateCsvSuccess(filepath, rowIndex, txHash) {
    try {
        // Read the entire file content
        const fileContent = fs.readFileSync(filepath, 'utf-8');
        const records = csv.parse(fileContent, {
            columns: true,
            skip_empty_lines: true
        });

        // Validate row exists
        if (!records[rowIndex]) {
            throw new Error(`Row ${rowIndex} not found in CSV`);
        }

        // Update the record
        records[rowIndex].SUCCESS = 'true';
        records[rowIndex].TX_HASH = txHash;

        // Create CSV string with header
        const header = Object.keys(records[0]);
        const csvContent = [
            header.join(','),
            ...records.map(record => 
                header.map(key => record[key] || '').join(',')
            )
        ].join('\n');

        // Write to temp file first
        const tempFile = `${filepath}.tmp`;
        fs.writeFileSync(tempFile, csvContent);
        
        // Atomic rename
        fs.renameSync(tempFile, filepath);
        
        console.log(`Updated CSV row ${rowIndex} with TX hash: ${txHash}`);
    } catch (error) {
        console.error(`Failed to update CSV for row ${rowIndex}:`, error);
        throw error;
    }
}

async function createBatchedMessages(refundData, sender, client) {
    const messages = [];
    const totalToSend = refundData.reduce((acc, curr) => acc + BigInt(curr.amount), BigInt(0));
    const walletBalance = await getUSDCBalance(client, sender);
    const difference = totalToSend - BigInt(walletBalance);

    // If difference is within dust threshold, adjust last transaction
    if (difference > 0 && difference <= DUST_THRESHOLD) {
        console.log(`Adjusting last transaction by ${difference.toString()} units to account for dust difference`);
        
        for (let i = 0; i < refundData.length; i++) {
            const { address, amount, rowIndex } = refundData[i];
            let adjustedAmount = BigInt(amount);

            // Adjust the last transaction
            if (i === refundData.length - 1) {
                adjustedAmount = adjustedAmount - difference;
                console.log(`Last transaction adjusted from ${amount} to ${adjustedAmount.toString()}`);
            }

            messages.push({
                typeUrl: '/cosmos.bank.v1beta1.MsgSend',
                value: {
                    fromAddress: sender,
                    toAddress: address,
                    amount: [{
                        denom: USDC_DENOM,
                        amount: adjustedAmount.toString()
                    }]
                },
                rowIndex
            });
        }
    } else {
        // No adjustment needed
        messages.push(...refundData.map(({ address, amount, rowIndex }) => ({
            typeUrl: '/cosmos.bank.v1beta1.MsgSend',
            value: {
                fromAddress: sender,
                toAddress: address,
                amount: [{
                    denom: USDC_DENOM,
                    amount: amount
                }]
            },
            rowIndex
        })));
    }

    return messages;
}

function formatUSDC(amount) {
    // Convert from 6 decimals to human readable with thousands separator
    return (BigInt(amount) / BigInt(1000000)).toLocaleString('en-US', {
        minimumFractionDigits: 6,
        maximumFractionDigits: 6
    });
}

async function getUSDCBalance(client, address) {
    const balance = await client.getBalance(address, USDC_DENOM);
    return balance.amount;
}

async function processBatch(client, messages, batch, filepath, totalBatches, sender) {
    console.log(`\nProcessing batch ${batch + 1} of ${totalBatches}`);
    const results = [];

    for (const [index, msg] of messages.entries()) {
        const currentBalance = await getUSDCBalance(client, sender);
        console.log(`\nTransaction ${index + 1} of ${messages.length} in batch ${batch + 1}`);
        console.log(`From: ${sender}`);
        console.log(`To: ${msg.value.toAddress}`);
        console.log(`Amount: ${formatUSDC(msg.value.amount[0].amount)} USDC`);
        console.log(`Wallet Balance: ${formatUSDC(currentBalance)} USDC`);
        
        try {
            const gasLimit = ((BASE_GAS + (GAS_PER_TX * messages.length))).toString();
            
            const response = await client.signAndBroadcast(
                sender,
                [msg],
                {
                    amount: [{ denom: 'untrn', amount: '5000' }],
                    gas: gasLimit
                }
            );
            
            if (response.code === 0) {
                console.log(`✅ Success - TX Hash: ${response.transactionHash}`);
                try {
                    // Update CSV with transaction result
                    await updateCsvSuccess(filepath, msg.rowIndex, response.transactionHash);
                    console.log(`✅ CSV updated successfully for row ${msg.rowIndex}`);
                } catch (csvError) {
                    console.error(`Failed to update CSV for row ${msg.rowIndex}:`, csvError);
                }
                results.push({ success: true, txHash: response.transactionHash });
            } else {
                console.log(` Transaction failed for address ${msg.value.toAddress}. Will retry in next execution.`);
                results.push({ 
                    success: false, 
                    error: response,
                    address: msg.value.toAddress 
                });
            }
        } catch (error) {
            console.log(`❌ Transaction failed for address ${msg.value.toAddress}. Will retry in next execution.`);
            console.error(`Error details:`, error.message);
            results.push({ 
                success: false, 
                error: error.message,
                address: msg.value.toAddress 
            });
        }
    }
    
    return results;
}

async function fetchChainRegistry() {
    try {
        const response = await fetch('https://raw.githubusercontent.com/cosmos/chain-registry/master/neutron/chain.json');
        const data = await response.json();
        return data.apis.rpc.map(rpc => {
            // Convert websocket URLs to https
            let address = rpc.address.replace('wss://', 'https://').replace('ws://', 'http://');
            // Remove any trailing paths
            address = address.split('/websocket')[0];
            return {
                address,
                provider: rpc.provider
            };
        });
    } catch (error) {
        console.error('Failed to fetch chain registry:', error);
        throw error;
    }
}

async function findHealthyRPC() {
    try {
        const rpcList = await fetchChainRegistry();
        console.log(`Testing ${rpcList.length} RPC endpoints...`);

        for (const rpc of rpcList) {
            try {
                console.log(`\nTesting ${rpc.address} (${rpc.provider})...`);
                const start = Date.now();
                const response = await fetch(`${rpc.address}/status`);
                const data = await response.json();
                const duration = Date.now() - start;

                if (response.ok && data.result?.sync_info) {
                    console.log(`✅ Healthy - Response time: ${duration}ms`);
                    console.log(`Latest block: ${data.result.sync_info.latest_block_height}`);
                    return rpc.address;
                } else {
                    console.log('❌ Unhealthy - Invalid response');
                }
            } catch (error) {
                console.log(`❌ Failed - ${error.message}`);
            }
        }
        throw new Error('No healthy RPC endpoints found');
    } catch (error) {
        console.error('Failed to find healthy RPC:', error);
        throw error;
    }
}

function serializeError(error) {
    return {
        message: error.message,
        code: error.code,
        stack: error.stack,
        timestamp: new Date().toISOString()
    };
}

function writeResults(batches) {
    try {
        const serializableResults = batches.map(batch => 
            batch.map(result => ({
                ...result,
                error: result.error ? serializeError(result.error) : undefined
            }))
        );

        fs.writeFileSync(
            'refund_results.json',
            JSON.stringify(serializableResults, null, 2)
        );
    } catch (error) {
        console.error('Error writing results:', error);
        throw error;
    }
}

async function checkRemainingBalance(client, sender, remainingToSend) {
    const balance = await client.getBalance(sender, USDC_DENOM);
    const difference = remainingToSend - BigInt(balance.amount);
    
    // Allow the transaction if the difference is within dust threshold
    if (difference > DUST_THRESHOLD) {
        throw new Error('Insufficient funds to continue processing');
    }
}

function validateCsvFile(filepath) {
    if (!fs.existsSync(filepath)) {
        throw new Error(`File not found: ${filepath}`);
    }
    
    const content = fs.readFileSync(filepath, 'utf-8');
    if (!content.trim()) {
        throw new Error('CSV file is empty');
    }
    
    const firstLine = content.split('\n')[0].toLowerCase();
    const requiredColumns = ['address', 'amount', 'success', 'tx_hash'];
    for (const column of requiredColumns) {
        if (!firstLine.includes(column)) {
            throw new Error(`Required column '${column}' not found in CSV`);
        }
    }
}

function writeSummary(stats) {
    const summary = {
        timestamp: new Date().toISOString(),
        totalProcessed: stats.successCount + stats.failureCount,
        successful: stats.successCount,
        failed: stats.failureCount,
        totalAmount: stats.totalAmount.toString(),
        duration: stats.duration,
        batchSize: BATCH_SIZE,
        rpcEndpoint: RPC_ENDPOINT
    };

    fs.writeFileSync(
        'refund_summary.json',
        JSON.stringify(summary, null, 2)
    );
}

async function main() {
    const startTime = Date.now();
    try {
        // Find healthy RPC first
        console.log('Finding healthy RPC endpoint...');
        const healthyRPC = await findHealthyRPC();
        console.log(`\nUsing RPC endpoint: ${healthyRPC}`);
        
        validateCsvFile('refunds.csv');
        const refundData = readRefundData('refunds.csv');
        console.log(`Loaded ${refundData.length} refund entries`);

        // Initialize wallet and client with the healthy RPC
        const wallet = await loadWallet(process.env.MNEMONIC);
        const [{ address: sender }] = await wallet.getAccounts();
        
        const client = await SigningStargateClient.connectWithSigner(
            healthyRPC,
            wallet,
            {
                gasPrice: GasPrice.fromString(`${GAS_PRICE}untrn`)
            }
        );

        // Get and display initial balance
        const initialBalance = await getUSDCBalance(client, sender);
        console.log(`\nInitial wallet balance: ${formatUSDC(initialBalance)} USDC`);
        
        // Calculate total to send
        const totalToSend = refundData.reduce((acc, curr) => acc + BigInt(curr.amount), BigInt(0));
        console.log(`Total to send: ${formatUSDC(totalToSend)} USDC`);
        
        const initialBalanceBigInt = BigInt(initialBalance);
        const totalToSendBigInt = totalToSend;
        console.log('Raw balance:', initialBalance);
        console.log('Raw total to send:', totalToSend.toString());
        console.log('BigInt balance:', initialBalanceBigInt.toString());
        console.log('BigInt total to send:', totalToSendBigInt.toString());

        // Calculate the difference
        const difference = totalToSendBigInt - initialBalanceBigInt;
        console.log('Difference:', difference.toString(), 'units');

        if (difference > DUST_THRESHOLD) {
            throw new Error(`Insufficient funds. Have: ${formatUSDC(initialBalance)} USDC, Need: ${formatUSDC(totalToSend)} USDC (Difference: ${formatUSDC(difference.toString())} USDC)`);
        }

        // Process in batches
        const messages = await createBatchedMessages(refundData, sender, client);
        const totalBatches = Math.ceil(messages.length / BATCH_SIZE);
        const batches = [];
        
        let remainingToSend = totalToSend;
        for (let i = 0; i < messages.length; i += BATCH_SIZE) {
            const batchMessages = messages.slice(i, i + BATCH_SIZE);
            await checkRemainingBalance(client, sender, remainingToSend);
            
            const result = await processBatch(
                client, 
                batchMessages, 
                Math.floor(i / BATCH_SIZE), 
                'refunds.csv',
                totalBatches,
                sender
            );
            
            // Update remaining amount
            const batchTotal = batchMessages.reduce((acc, msg) => 
                acc + BigInt(msg.value.amount[0].amount), BigInt(0));
            remainingToSend -= batchTotal;
            
            batches.push(result);
            
            // Add delay between batches to avoid rate limiting
            if (i + BATCH_SIZE < messages.length) {
                await new Promise(resolve => setTimeout(resolve, 6000));
            }
        }

        // Generate report
        let successCount = 0;
        let failureCount = 0;

        for (const batch of batches) {
            for (const result of batch) {
                if (result.success) {
                    successCount++;
                } else {
                    failureCount++;
                }
            }
        }

        // Get and display final balance
        const finalBalance = await getUSDCBalance(client, sender);
        
        console.log('\nRefund Distribution Complete');
        console.log(`Successful transactions: ${successCount}`);
        console.log(`Failed transactions: ${failureCount}`);
        console.log(`Total processed: ${successCount + failureCount}`);
        console.log(`\nWallet Balance Summary:`);
        console.log(`Initial balance: ${formatUSDC(initialBalance)} USDC`);
        console.log(`Final balance: ${formatUSDC(finalBalance)} USDC`);
        console.log(`Total sent: ${formatUSDC(BigInt(initialBalance) - BigInt(finalBalance))} USDC`);
        
        // Write detailed results to file
        writeResults(batches);

        // Update stats object
        const stats = {
            successCount,
            failureCount,
            totalAmount: totalToSend.toString(),
            initialBalance: initialBalance,
            finalBalance: finalBalance,
            duration: Date.now() - startTime
        };
        writeSummary(stats);

    } catch (error) {
        console.error('Fatal error:', error);
        process.exit(1);
    }
}

process.on('SIGINT', async () => {
    console.log('\nGracefully shutting down...');
    process.exit(0);
});

process.on('unhandledRejection', (error) => {
    console.error('Unhandled promise rejection:', error);
    process.exit(1);
});

main();
